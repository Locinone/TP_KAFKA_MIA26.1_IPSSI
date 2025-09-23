import os
import json
import time
from typing import Optional
from kafka import KafkaConsumer
import requests

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("TOPIC", "weather_transformed")
WEBHDFS_BASE = os.getenv("WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1")
HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/hdfs-data")


def ensure_dir(country: str, city: str) -> None:
    path = f"{HDFS_BASE_PATH}/{country}/{city}"
    url = f"{WEBHDFS_BASE}{path}?op=MKDIRS"
    try:
        requests.put(url, timeout=5)
    except Exception:
        pass


def append_alert(country: str, city: str, alert: dict) -> bool:
    ensure_dir(country, city)
    path = f"{HDFS_BASE_PATH}/{country}/{city}/alerts.json"
    # WebHDFS doesn't support append by default without enabling, fallback: create new file per event with timestamp
    filename = f"alerts_{int(time.time()*1000)}.json"
    file_path = f"{HDFS_BASE_PATH}/{country}/{city}/{filename}"
    create_url = f"{WEBHDFS_BASE}{file_path}?op=CREATE&overwrite=true"
    try:
        resp = requests.put(create_url, data=json.dumps(alert) + "\n", timeout=5)
        return resp.status_code in (201, 200)
    except Exception:
        return False


def parse_transformed(message_str: str) -> Optional[dict]:
    try:
        data = json.loads(message_str)
        # Expected fields from spark_job: event_time, temperature, windspeed, wind_alert_level, heat_alert_level
        alert_levels = {
            "wind": data.get("wind_alert_level"),
            "heat": data.get("heat_alert_level"),
        }
        has_alert = any(level in ("level_1", "level_2") for level in alert_levels.values())
        if not has_alert:
            return None
        return {
            "event_time": data.get("event_time"),
            "temperature": data.get("temperature"),
            "windspeed": data.get("windspeed"),
            "wind_alert_level": data.get("wind_alert_level"),
            "heat_alert_level": data.get("heat_alert_level"),
        }
    except Exception:
        return None


def main() -> None:
    print(f"📡 Alerts consumer connecting to {KAFKA_BROKER}, topic={TOPIC}")
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: v.decode("utf-8"),
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    consumer.subscribe([TOPIC])
    print("✅ Alerts consumer started.")

    # For now, read country/city from environment as fallback
    default_city = os.getenv("DEFAULT_CITY", "UnknownCity")
    default_country = os.getenv("DEFAULT_COUNTRY", "UnknownCountry")

    for msg in consumer:
        parsed = parse_transformed(msg.value)
        if not parsed:
            continue
        # Choose country/city: from env for now (since weather_transformed doesn't carry them)
        country = default_country
        city = default_city
        alert_record = {
            "city": city,
            "country": country,
            **parsed,
        }
        ok = append_alert(country, city, alert_record)
        if ok:
            print(f"✅ Alert written for {country}/{city} @ {parsed.get('event_time')}")
        else:
            print(f"❌ Failed to write alert for {country}/{city}")


if __name__ == "__main__":
    main()
