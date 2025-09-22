#!/usr/bin/env python3
"""
Version simplifiée du writer HDFS utilisant l'API WebHDFS
"""
import os
import json
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, from_unixtime, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

def build_weather_stream_schema() -> StructType:
    """Schema pour le topic weather_stream avec informations de partitionnement HDFS"""
    return StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("admin1", StringType(), True),  # région/état
        StructField("region", StringType(), True),  # Pour partitionnement HDFS
        StructField("continent", StringType(), True),  # Pour partitionnement HDFS
        StructField("timestamp", LongType(), True),  # epoch seconds
        StructField("date", StringType(), True),  # Format YYYY-MM-DD
        StructField("hour", StringType(), True),  # Format HH
        StructField("weather", StructType([
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("winddirection", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("is_day", IntegerType(), True),
            StructField("time", StringType(), True),
        ]), True),
        StructField("location_info", StructType([
            StructField("timezone", StringType(), True),
            StructField("timezone_abbreviation", StringType(), True),
            StructField("elevation", DoubleType(), True),
        ]), True),
    ])

def write_to_hdfs_webhdfs(data, hdfs_namenode, hdfs_path):
    """Écrire des données dans HDFS via l'API WebHDFS"""
    try:
        # Créer le chemin de partitionnement
        city = data.get('city', 'Unknown')
        country = data.get('country', 'Unknown')
        date = time.strftime("%Y-%m-%d")
        hour = time.strftime("%H")
        
        partition_path = f"{hdfs_path}/city={city}/country={country}/date={date}/hour={hour}"
        
        # Créer le répertoire si nécessaire
        create_dir_url = f"http://namenode:9870/webhdfs/v1{partition_path}?op=MKDIRS"
        response = requests.put(create_dir_url)
        
        # Écrire le fichier
        filename = f"weather_{int(time.time() * 1000)}.json"
        file_path = f"{partition_path}/{filename}"
        
        write_url = f"http://namenode:9870/webhdfs/v1{file_path}?op=CREATE&overwrite=true"
        response = requests.put(write_url, data=json.dumps(data, indent=2))
        
        if response.status_code == 201:
            print(f"✅ Écrit dans HDFS: {file_path}")
            return True
        else:
            print(f"❌ Erreur écriture HDFS: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ Erreur WebHDFS: {e}")
        return False

def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    source_topic = os.getenv("SOURCE_TOPIC", "weather_stream")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    hdfs_path = os.getenv("HDFS_PATH", "/weather-data")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/hdfs_writer")

    # Créer la session Spark
    spark = (
        SparkSession.builder
        .appName("weather-hdfs-writer-simple")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Lire le stream depuis Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", source_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    
    # Parse JSON data
    stream_schema = build_weather_stream_schema()
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), stream_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*")
    
    # Fonction pour écrire dans HDFS
    def write_batch(batch_df, batch_id):
        """Fonction pour écrire chaque batch dans HDFS"""
        print(f"📝 Traitement du batch {batch_id}")
        
        # Convertir le DataFrame en liste de dictionnaires
        rows = batch_df.collect()
        
        for row in rows:
            data = row.asDict()
            # Convertir les objets Row en dictionnaires simples
            data = {k: v for k, v in data.items() if v is not None}
            
            # Écrire dans HDFS
            write_to_hdfs_webhdfs(data, hdfs_namenode, hdfs_path)
    
    # Écrire dans HDFS avec foreachBatch
    query = (
        parsed_df.writeStream
        .foreachBatch(write_batch)
        .outputMode("append")
        .start()
    )
    
    print("🚀 Service HDFS Writer démarré (version simplifiée)")
    query.awaitTermination()

if __name__ == "__main__":
    main()
