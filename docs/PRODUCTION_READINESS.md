# Production Readiness Guide

## Current State (what you already have)
- Streaming topology: Kafka → Spark (transform + aggregates) → HDFS (partitioned writes)
- Sliding windows, watermarks, time-based join
- HDFS partitioning by city/country/date/hour (and aggregates job including region/continent)
- Basic visualization path: Grafana + JSON datasource backed by analytics API
- Docker Compose environment; healthcheck for Kafka

## Gaps to be Production‑like
- Reliability & scale
  - Single Kafka broker, RF=1, few/no partitions; no Schema Registry
  - Spark runs local[*], no HA, checkpoints on container FS
  - HDFS single NameNode (no HA), dfs.replication=1
- Data quality & modeling
  - No schema contracts (Avro/Protobuf), no evolution policy, no DLQ
  - Alerts written as many small JSON files (small-files anti-pattern)
- Security & operations
  - No TLS/SASL on Kafka; Grafana/API open; no secrets management
  - Limited monitoring/alerting (Kafka/Spark/HDFS) and logs centralization
- Serving layer for dashboards
  - HDFS only; no fast time‑series/analytics store (ClickHouse/Influx/Timescale)
  - No table format/catalog (Iceberg/Delta/Hudi + Hive/Glue)

## Recommended Target Architecture
- Infra metrics (brokers, ZK, HDFS, Spark)
  - JMX Exporter → Prometheus → Grafana (alerts + dashboards)
- Hot path (real‑time payload analytics)
  - Kafka → Flink/Spark (exactly‑once, 1–10s latency) → ClickHouse (or Timescale/Influx) → Grafana
- Cold path (historical)
  - Kafka → HDFS/S3 (Parquet with Iceberg/Delta/Hudi) → Trino/Presto/Spark SQL → Grafana/BI

## Minimal Production Blueprint
- Kafka
  - 3 brokers, topics with partitions ≥3, replication.factor=3
  - Schema Registry (Avro/Protobuf), SASL/TLS, proper retention/compaction
- Stream Processing
  - Flink (preferred) or Spark Structured Streaming with checkpoints on durable storage (S3/HDFS)
  - Exactly‑once sinks; retry/recovery strategy
- Storage/Serving
  - Fast store: ClickHouse for dashboards (MergeTree; partition by date, order by (city, ts))
  - Data lake: Parquet + Iceberg/Delta/Hudi; Hive/Glue catalog
- Observability
  - Prometheus scrape: Kafka/ZK/HDFS/Spark/JVM; Grafana dashboards
  - Centralized logs; alert rules for lag, errors, latency
- Security & Ops
  - TLS/SASL for Kafka; restricted Grafana/API; secrets in env manager/vault
  - IaC (K8s/Helm/Terraform), dashboards-as-code, CI/CD pipelines

## Quick Wins You Can Implement Now
1) Observability
- Add Prometheus + JMX Exporter for Kafka (and ZK/HDFS/Spark)
- Import Kafka dashboards in Grafana

2) Schemas & Quality
- Add Schema Registry; serialize with Avro/Protobuf
- Define contracts and evolution policy; add DLQ topic for bad events

3) Serving Layer
- Add ClickHouse container + Kafka Connect sink for `weather_transformed`
- Build Grafana panels directly from ClickHouse (temperature, windspeed, alert counts, top weathercode)

4) Fix Small Files
- Write alerts in micro‑batches to Parquet partitioned by country/city/date/hour
- Compact small files periodically (or use table format compaction)

## Hot Path (Spark → ClickHouse) Sketch
- In Spark `foreachBatch`:
  - Transform rows to: ts, city, country, temperature, windspeed, wind_alert_level, heat_alert_level, weathercode
  - Write via ClickHouse HTTP/Native driver
- ClickHouse table example (MergeTree):
  - Columns: ts DateTime64, city String, country String, temperature Float64, windspeed Float64, wind_alert_level LowCardinality(String), heat_alert_level LowCardinality(String), weathercode UInt16
  - PARTITION BY toDate(ts) ORDER BY (city, ts)

## Infra Metrics (Kafka) Sketch
- Kafka service env & agent:
  - KAFKA_JMX_PORT=5555, JMX_PORT=5555
  - KAFKA_OPTS="-javaagent:/jmx/jmx_prometheus_javaagent.jar=7071:/jmx/kafka.yml"
- Prometheus scrape config:
  - job_name: kafka-broker; targets: ["kafka:7071"]
- Grafana datasource: Prometheus; import Kafka dashboards

## Validation Checklist
- End‑to‑end latency (p95) ≤ 5–10s for hot‑path dashboards
- Kafka consumer lag near zero; no failed batches; DLQ monitored
- Prometheus alerts for broker health, lag, job failures
- Small‑files ratios low; periodic compactions succeed
- Secure endpoints (Kafka TLS/SASL, Grafana login), secrets managed

## Suggested Priority
1) Prometheus/JMX + Kafka dashboards
2) Schema Registry + Avro/Protobuf in producers/consumers
3) ClickHouse (or Timescale/Influx) + sink → Grafana panels
4) Secure Kafka + access control; CI/CD for infra
5) Iceberg/Delta/Hudi + catalog, compaction, data governance
