## Weather Streaming with Spark + Kafka

### Topics
- `weather_stream`: raw data from the producer (Open-Meteo)
- `weather_transformed`: enriched stream with alert levels and normalized fields

### Transforms
- `event_time`: derived from `weather.time` then `timestamp` (epoch) then Kafka timestamp
- `temperature`: passthrough in °C
- `windspeed`: converted from km/h to m/s
- `wind_alert_level`: level_0 (<10 m/s), level_1 (10–20 m/s), level_2 (>20 m/s)
- `heat_alert_level`: level_0 (<25°C), level_1 (25–35°C), level_2 (>35°C)

### Run (Docker Compose)
```bash
docker compose up -d --build
```

Services started:
- `zookeeper`, `kafka`
- `data-producer` → produces to `weather_stream`
- `spark-app` → reads `weather_stream`, writes `weather_transformed`
- `data-consumer` → tails `weather_stream`
- `data-consumer-transformed` → tails `weather_transformed`

### Inspect messages
```bash
docker logs -f data-consumer
docker logs -f data-consumer-transformed
```

### Environment variables (Spark)
- `KAFKA_BOOTSTRAP` (default: `kafka:9092`)
- `SOURCE_TOPIC` (default: `weather_stream`)
- `SINK_TOPIC` (default: `weather_transformed`)
- `CHECKPOINT_DIR` (default: `/tmp/checkpoints/weather_transformed`)

