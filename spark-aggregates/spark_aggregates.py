import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    from_unixtime,
    lit,
    to_json,
    struct,
    window,
    count,
    avg,
    min as spark_min,
    max as spark_max,
    sum as spark_sum,
    expr,
    round as spark_round,
    concat,
    coalesce,
    broadcast,
    abs as spark_abs
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType


def build_weather_stream_schema() -> StructType:
    """Schema pour le topic weather_stream (données brutes)"""
    return StructType([
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("timestamp", LongType(), True),  # epoch seconds
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


def build_weather_transformed_schema() -> StructType:
    """Schema pour le topic weather_transformed"""
    return StructType([
        StructField("event_time", StringType(), True),  # timestamp as string
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("wind_alert_level", StringType(), True),
        StructField("heat_alert_level", StringType(), True),
    ])


def main() -> None:
    # Configuration
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    stream_topic = os.getenv("STREAM_TOPIC", "weather_stream")
    transformed_topic = os.getenv("TRANSFORMED_TOPIC", "weather_transformed")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/weather_aggregates")
    
    # Créer la session Spark
    spark = (
        SparkSession.builder
        .appName("weather-realtime-aggregates")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("WARN")
    
    # Lire le topic weather_stream (pour les coordonnées)
    stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", stream_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    
    # Lire le topic weather_transformed (pour les alertes)
    transformed_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", transformed_topic)
        .option("startingOffsets", "latest")
        .load()
    )
    
    # Parser les données de weather_stream
    stream_schema = build_weather_stream_schema()
    parsed_stream = stream_df.select(
        from_json(col("value").cast("string"), stream_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        col("data.latitude").alias("latitude"),
        col("data.longitude").alias("longitude"),
        when(
            col("data.weather.time").isNotNull(), 
            to_timestamp(col("data.weather.time"))
        ).when(
            col("data.timestamp").isNotNull(), 
            to_timestamp(from_unixtime(col("data.timestamp")))
        ).otherwise(
            col("kafka_timestamp")
        ).alias("event_time_stream")
    )
    
    # Parser les données de weather_transformed
    transformed_schema = build_weather_transformed_schema()
    parsed_transformed = transformed_df.select(
        from_json(col("value").cast("string"), transformed_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select(
        to_timestamp(col("data.event_time")).alias("event_time_transformed"),
        col("data.temperature").alias("temperature"),
        col("data.windspeed").alias("windspeed"),
        col("data.wind_alert_level").alias("wind_alert_level"),
        col("data.heat_alert_level").alias("heat_alert_level")
    )
    
    # Définir watermark pour gérer les données en retard (5 minutes de tolérance)
    stream_with_watermark = parsed_stream.withWatermark("event_time_stream", "5 minutes")
    transformed_with_watermark = parsed_transformed.withWatermark("event_time_transformed", "5 minutes")
    
    # Jointure sur event_time avec tolérance de 30 secondes
    joined_df = transformed_with_watermark.join(
        stream_with_watermark,
        expr("""
            abs(unix_timestamp(event_time_transformed) - unix_timestamp(event_time_stream)) <= 30
        """),
        "left"
    ).select(
        coalesce(col("event_time_transformed"), col("event_time_stream")).alias("event_time"),
        col("temperature"),
        col("windspeed"),
        col("wind_alert_level"),
        col("heat_alert_level"),
        col("latitude"),
        col("longitude")
    )
    
    # Créer une colonne pour la ville (basée sur les coordonnées)
    # Pour l'exemple, on utilise des zones géographiques simplifiées
    joined_with_location = joined_df.withColumn(
        "city",
        when((col("latitude").between(48.8, 48.9)) & (col("longitude").between(2.3, 2.4)), "Paris")
        .when((col("latitude").between(51.4, 51.6)) & (col("longitude").between(-0.2, 0.0)), "London")
        .when((col("latitude").between(40.6, 40.8)) & (col("longitude").between(-74.1, -73.9)), "New York")
        .otherwise("Other")
    )
    
    # Agrégats sur fenêtre de 1 minute
    window_1min_df = joined_with_location.groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("city")
    ).agg(
        # Alertes vent
        count(when(col("wind_alert_level") == "level_1", 1)).alias("wind_alerts_level1"),
        count(when(col("wind_alert_level") == "level_2", 1)).alias("wind_alerts_level2"),
        # Alertes chaleur
        count(when(col("heat_alert_level") == "level_1", 1)).alias("heat_alerts_level1"),
        count(when(col("heat_alert_level") == "level_2", 1)).alias("heat_alerts_level2"),
        # Stats température
        spark_round(spark_min("temperature"), 2).alias("min_temperature"),
        spark_round(spark_max("temperature"), 2).alias("max_temperature"),
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        # Total alertes
        count(when((col("wind_alert_level").isin("level_1", "level_2")) | 
                   (col("heat_alert_level").isin("level_1", "level_2")), 1)).alias("total_alerts"),
        # Nombre d'enregistrements
        count("*").alias("record_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("1_minute").alias("window_size"),
        col("city"),
        col("wind_alerts_level1"),
        col("wind_alerts_level2"),
        col("heat_alerts_level1"),
        col("heat_alerts_level2"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_temperature"),
        col("total_alerts"),
        col("record_count")
    )
    
    # Agrégats sur fenêtre de 5 minutes
    window_5min_df = joined_with_location.groupBy(
        window(col("event_time"), "5 minutes", "1 minute"),
        col("city")
    ).agg(
        # Alertes vent
        count(when(col("wind_alert_level") == "level_1", 1)).alias("wind_alerts_level1"),
        count(when(col("wind_alert_level") == "level_2", 1)).alias("wind_alerts_level2"),
        # Alertes chaleur
        count(when(col("heat_alert_level") == "level_1", 1)).alias("heat_alerts_level1"),
        count(when(col("heat_alert_level") == "level_2", 1)).alias("heat_alerts_level2"),
        # Stats température
        spark_round(spark_min("temperature"), 2).alias("min_temperature"),
        spark_round(spark_max("temperature"), 2).alias("max_temperature"),
        spark_round(avg("temperature"), 2).alias("avg_temperature"),
        # Total alertes
        count(when((col("wind_alert_level").isin("level_1", "level_2")) | 
                   (col("heat_alert_level").isin("level_1", "level_2")), 1)).alias("total_alerts"),
        # Nombre d'enregistrements
        count("*").alias("record_count")
    ).select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        lit("5_minutes").alias("window_size"),
        col("city"),
        col("wind_alerts_level1"),
        col("wind_alerts_level2"),
        col("heat_alerts_level1"),
        col("heat_alerts_level2"),
        col("min_temperature"),
        col("max_temperature"),
        col("avg_temperature"),
        col("total_alerts"),
        col("record_count")
    )
    
    # Union des deux fenêtres
    all_aggregates = window_1min_df.union(window_5min_df)
    
    # Convertir en JSON pour Kafka
    output_df = all_aggregates.select(
        to_json(struct([col(c) for c in all_aggregates.columns])).alias("value")
    )
    
    # Écrire dans la console pour debug
    console_query = (
        all_aggregates.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    # Écrire dans un nouveau topic Kafka
    kafka_query = (
        output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "weather_aggregates")
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )
    
    # Attendre la fin
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
