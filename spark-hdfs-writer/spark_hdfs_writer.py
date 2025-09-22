import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    from_unixtime,
    lit,
    current_timestamp,
    year,
    month,
    dayofmonth,
    hour
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType


def build_weather_stream_schema() -> StructType:
    """Schema pour le topic weather_stream avec informations de partitionnement"""
    return StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("admin1", StringType(), True),
        StructField("region", StringType(), True),  # Pour partitionnement HDFS
        StructField("continent", StringType(), True),  # Pour partitionnement HDFS
        StructField("timestamp", LongType(), True),
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


def main() -> None:
    # Configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    source_topic = os.getenv("SOURCE_TOPIC", "weather_stream")
    hdfs_namenode = os.getenv("HDFS_NAMENODE", "hdfs://namenode:9000")
    hdfs_path = os.getenv("HDFS_PATH", "/weather-data")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/hdfs_writer")
    
    # Créer la session Spark
    spark = (
        SparkSession.builder
        .appName("weather-hdfs-writer")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.fs.defaultFS", hdfs_namenode)
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
        .config("spark.hadoop.security.authentication", "simple")
        .config("spark.hadoop.dfs.permissions.enabled", "false")
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
    
    # Parser les données JSON
    schema = build_weather_stream_schema()
    parsed_df = raw_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.*"),
        col("kafka_timestamp")
    )
    
    # Ajouter des colonnes de partitionnement temporel
    df_with_partitions = parsed_df.withColumn(
        "year", year(to_timestamp(from_unixtime(col("timestamp"))))
    ).withColumn(
        "month", month(to_timestamp(from_unixtime(col("timestamp"))))
    ).withColumn(
        "day", dayofmonth(to_timestamp(from_unixtime(col("timestamp"))))
    ).withColumn(
        "hour_partition", hour(to_timestamp(from_unixtime(col("timestamp"))))
    ).withColumn(
        "processing_time", current_timestamp()
    )
    
    # Écrire dans HDFS avec partitionnement
    query = (
        df_with_partitions.writeStream
        .format("parquet")
        .option("path", f"{hdfs_namenode}{hdfs_path}")
        .option("checkpointLocation", checkpoint_dir)
        .partitionBy("continent", "region", "year", "month", "day", "hour_partition")
        .outputMode("append")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    print(f"🚀 Writing to HDFS: {hdfs_namenode}{hdfs_path}")
    print(f"📁 Partitioning by: continent, region, year, month, day, hour_partition")
    print(f"⏰ Processing every 30 seconds")
    
    query.awaitTermination()


if __name__ == "__main__":
    main()
