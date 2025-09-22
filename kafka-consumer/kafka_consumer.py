import json
import time
import sys
from kafka import KafkaConsumer
import random

# Récupérer le topic depuis les arguments
print(f"Debug: sys.argv = {sys.argv}")
print(f"Debug: len(sys.argv) = {len(sys.argv)}")

if len(sys.argv) != 2:
    print("Usage: python kafka_consumer.py <topic_name>")
    sys.exit(1)

TOPIC = sys.argv[1]
print(f"Debug: TOPIC = {TOPIC}") 

# Configuration Kafka
KAFKA_BROKER = 'kafka:9092'
GROUP_ID = 'weather_consumer'

consumer = None
while consumer is None:
    try:
        print("⏳ Trying to connect to Kafka...")
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID
        )
        consumer.subscribe([TOPIC])
        print("✅ Kafka consumer connected.")
    except Exception as e:
        print(f"❌ Kafka not ready yet: {e}")
        time.sleep(2)
    
if __name__ == "__main__":
    while True:
        print("🔍 Listening to Kafka...")
        for message in consumer:
            print(f"{TOPIC} : [byte]{message.value} : [string]{message.value.decode('utf-8')}")
            time.sleep(1)