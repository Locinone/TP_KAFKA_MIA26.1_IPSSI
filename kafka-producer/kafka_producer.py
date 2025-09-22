import json
import time
from kafka import KafkaProducer
import random

# Configuration Kafka
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'weather_stream'

# Retry until Kafka is available
producer = None
while producer is None:
    try:
        print("⏳ Trying to connect to Kafka...")
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Kafka producer connected.")
    except Exception as e:
        print(f"❌ Kafka not ready yet: {e}")
        time.sleep(2)

def send_dummy(message="Hello Kafka"):
   return  {"msg": message}

def generate_data():
    return {
        "id": f"meteor_{random.randint(10000, 99999)}",
        "timestamp": int(time.time()),
        "position": {
            "x": round(random.uniform(-500, 500), 2),
            "y": round(random.uniform(-500, 500), 2),
            "z": round(random.uniform(-500, 500), 2)
        },
        "vitesse": round(random.uniform(10, 30), 1),
        "taille": round(random.uniform(5, 20), 1),
        "type": random.choice([
            "astéroïde", "comète", "météorite", "ovni", "satellite",
            "débris", "falconX", "dragon", "gundam", "etoile de la mort"
        ])
    }

if __name__ == "__main__":
    while True:
        try:
            # data = generate_data()
            data = send_dummy()
            producer.send(TOPIC, data)
            print(f"🚀 Data sent: {data}")
            time.sleep(1)
        except Exception as e:
            print(f"❌ Failed to send data: {e}")
            time.sleep(1)