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
    round as spark_round,
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType


def build_weather_schema() -> StructType:
    return StructType(
        [
            # Optional enriched fields coming from producer v2
            StructField("city", StringType(), True),
            StructField("country", StringType(), True),

            # Original coordinate-based producer fields
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timestamp", LongType(), True),  # epoch seconds
            StructField(
                "weather",
                StructType(
                    [
                        StructField("temperature", DoubleType(), True),  # Celsius
                        StructField("windspeed", DoubleType(), True),  # km/h from Open-Meteo
                        StructField("winddirection", DoubleType(), True),
                        StructField("weathercode", IntegerType(), True),
                        StructField("is_day", IntegerType(), True),
                        StructField("time", StringType(), True),  # e.g. 2025-09-22T11:00
                    ]
                ),
                True,
            ),
            StructField(
                "location_info",
                StructType(
                    [
                        StructField("timezone", StringType(), True),
                        StructField("timezone_abbreviation", StringType(), True),
                        StructField("elevation", DoubleType(), True),
                    ]
                ),
                True,
            ),
        ]
    )


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    source_topic = os.getenv("SOURCE_TOPIC", "weather_stream")
    sink_topic = os.getenv("SINK_TOPIC", "weather_transformed")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/weather_transformed")

    spark = (
        SparkSession.builder.appName("weather-transform-alerts")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", source_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    json_df = raw_df.selectExpr("CAST(value AS STRING) AS value_str", "timestamp AS kafka_timestamp")

    schema = build_weather_schema()

    parsed_df = json_df.select(from_json(col("value_str"), schema).alias("data"), col("kafka_timestamp"))

    base_df = parsed_df.select(
        col("kafka_timestamp").alias("event_time_kafka"),
        col("data.timestamp").alias("event_epoch"),
        col("data.weather.time").alias("event_time_str"),
        col("data.weather.temperature").cast("double").alias("temperature_c"),
        col("data.weather.windspeed").cast("double").alias("windspeed_kmh"),
        # Preserve city/country when present in the source
        col("data.city").alias("city"),
        col("data.country").alias("country"),
    )

    # Determine event_time in priority order: weather.time -> producer epoch -> Kafka timestamp
    event_time_col = when(
        col("event_time_str").isNotNull(), to_timestamp(col("event_time_str"))
    ).when(
        col("event_epoch").isNotNull(), to_timestamp(from_unixtime(col("event_epoch")))
    ).otherwise(col("event_time_kafka"))

    # Convert wind speed from km/h to m/s
    windspeed_ms = (col("windspeed_kmh") / lit(3.6)).alias("windspeed")

    # Compute alert levels
    wind_alert_level = (
        when(windspeed_ms < lit(10.0), lit("level_0"))
        .when((windspeed_ms >= lit(10.0)) & (windspeed_ms <= lit(20.0)), lit("level_1"))
        .otherwise(lit("level_2"))
    ).alias("wind_alert_level")

    heat_alert_level = (
        when(col("temperature_c") < lit(25.0), lit("level_0"))
        .when((col("temperature_c") >= lit(25.0)) & (col("temperature_c") <= lit(35.0)), lit("level_1"))
        .otherwise(lit("level_2"))
    ).alias("heat_alert_level")

    transformed_df = base_df.select(
        event_time_col.alias("event_time").cast("timestamp"),
        col("temperature_c").alias("temperature"),
        spark_round(windspeed_ms, 2).alias("windspeed"),
        wind_alert_level,
        heat_alert_level,
        # Carry over city/country
        col("city"),
        col("country"),
    )

    out_df = transformed_df.select(to_json(struct(*[col(c) for c in transformed_df.columns])).alias("value"))

    query = (
        out_df.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", sink_topic)
        .option("checkpointLocation", checkpoint_dir)
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()

