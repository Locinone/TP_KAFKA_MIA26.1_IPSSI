import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


def build_spark_session(app_name: str) -> SparkSession:

	# Ensure the Kafka package is available when using spark-submit with --packages
	# The Dockerfile will pass the correct --packages; this is a safety net for local runs
	packages = os.environ.get(
		"SPARK_EXTRA_PACKAGES",
		"org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
	)

	return (
		SparkSession.builder.appName(app_name)
		.config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
		.config("spark.jars.packages", packages)
		.getOrCreate()
	)


def transform_weather_stream(spark: SparkSession) -> None:
	bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
	input_topic = os.environ.get("INPUT_TOPIC", "weather_stream")
	output_topic = os.environ.get("OUTPUT_TOPIC", "weather_transformed")
	checkpoint_location = os.environ.get(
		"CHECKPOINT_LOCATION", "/tmp/spark-checkpoints/weather_transformed"
	)

	# Read from Kafka
	kafka_df = (
		spark.readStream.format("kafka")
		.option("kafka.bootstrap.servers", bootstrap_servers)
		.option("subscribe", input_topic)
		.option("startingOffsets", "latest")
		.load()
	)

	# Parse JSON payload from value
	value_str_df = kafka_df.select(F.col("value").cast("string").alias("value"))

	# Define expected input schema
	input_schema = T.StructType(
		[
			T.StructField("id", T.StringType(), True),
			T.StructField("timestamp", T.LongType(), True),
			T.StructField("temperature", T.DoubleType(), True),
			T.StructField("windspeed", T.DoubleType(), True),
			T.StructField("station", T.StringType(), True),
		]
	)

	json_df = value_str_df.select(
		F.from_json(F.col("value"), input_schema).alias("data")
	).select("data.*")

	# Build event_time from Unix seconds timestamp
	with_event_time_df = json_df.withColumn(
		"event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp")))
	)

	# Ensure numeric and sane ranges (simple cleaning)
	clean_df = (
		with_event_time_df
		.withColumn("temperature", F.col("temperature").cast("double"))
		.withColumn("windspeed", F.col("windspeed").cast("double"))
	)

	# Alert levels
	wind_alert_level_df = (
		clean_df.withColumn(
			"wind_alert_level",
			F.when(F.col("windspeed") < 10, F.lit("level_0"))
			 .when((F.col("windspeed") >= 10) & (F.col("windspeed") <= 20), F.lit("level_1"))
			 .when(F.col("windspeed") > 20, F.lit("level_2"))
			 .otherwise(F.lit("level_0")),
		)
	)

	with_heat_alert_df = wind_alert_level_df.withColumn(
		"heat_alert_level",
		F.when(F.col("temperature") < 25, F.lit("level_0"))
		 .when((F.col("temperature") >= 25) & (F.col("temperature") <= 35), F.lit("level_1"))
		 .when(F.col("temperature") > 35, F.lit("level_2"))
		 .otherwise(F.lit("level_0")),
	)

	# Select and serialize to JSON for Kafka sink
	result_df = with_heat_alert_df.select(
		F.to_json(
			F.struct(
				F.col("id").alias("id"),
				F.col("station").alias("station"),
				F.col("event_time").cast("string").alias("event_time"),
				F.col("temperature").alias("temperature"),
				F.col("windspeed").alias("windspeed"),
				F.col("wind_alert_level").alias("wind_alert_level"),
				F.col("heat_alert_level").alias("heat_alert_level"),
			)
		).alias("value")
	)

	query = (
		result_df.writeStream.format("kafka")
		.option("kafka.bootstrap.servers", bootstrap_servers)
		.option("topic", output_topic)
		.option("checkpointLocation", checkpoint_location)
		.outputMode("append")
		.start()
	)

	query.awaitTermination()


if __name__ == "__main__":
	spark_session = build_spark_session("WeatherTransformJob")
	transform_weather_stream(spark_session)

