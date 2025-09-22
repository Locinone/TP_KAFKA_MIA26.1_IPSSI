import json
import time
import sys
import requests
from kafka import KafkaProducer
import random

# Configuration Kafka
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'weather_stream'

# Vérification des arguments de ligne de commande
# "48.8566", "2.3522" -> Paris
LATITUDE = "48.8566"
LONGITUDE = "2.3522"
print(f"🌍 Weather data for coordinates: {LATITUDE}, {LONGITUDE}")

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

def fetch_weather_data(latitude, longitude):
    """
    Récupère les données météo actuelles depuis l'API Open-Meteo
    """
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
        print(f"🌤️  Fetching weather data from: {url}")
        
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Lève une exception pour les codes d'erreur HTTP
        
        data = response.json()
        
        # Extraire les données météo actuelles
        current_weather = data.get('current_weather', {})
        
        # Enrichir avec les coordonnées et timestamp
        weather_data = {
            "latitude": latitude,
            "longitude": longitude,
            "timestamp": int(time.time()),
            "weather": {
                "temperature": current_weather.get('temperature'),
                "windspeed": current_weather.get('windspeed'),
                "winddirection": current_weather.get('winddirection'),
                "weathercode": current_weather.get('weathercode'),
                "is_day": current_weather.get('is_day'),
                "time": current_weather.get('time')
            },
            "location_info": {
                "timezone": data.get('timezone'),
                "timezone_abbreviation": data.get('timezone_abbreviation'),
                "elevation": data.get('elevation')
            }
        }
        
        return weather_data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête API: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Erreur lors du parsing JSON: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur inattendue: {e}")
        return None

def send_dummy(message="Hello Kafka"):
   return  {"msg": message}

if __name__ == "__main__":
    print("🌤️  Starting weather data producer...")
    print(f"📍 Coordinates: {LATITUDE}, {LONGITUDE}")
    print(f"📡 Sending to topic: {TOPIC}")
    print("=" * 50)
    
    while True:
        try:
            # Récupérer les données météo depuis l'API Open-Meteo
            data = fetch_weather_data(LATITUDE, LONGITUDE)
            
            if data:
                # Envoyer les données au topic Kafka
                producer.send(TOPIC, data)
                print(f"🚀 Weather data sent: {json.dumps(data, indent=2)}")
            else:
                print("⚠️  No weather data received, skipping this cycle")
            
            # Attendre 60 secondes avant la prochaine requête
            print("⏳ Waiting 60 seconds before next update...")
            time.sleep(60)
            
        except KeyboardInterrupt:
            print("\n🛑 Producer stopped by user")
            break
        except Exception as e:
            print(f"❌ Failed to send data: {e}")
            print("⏳ Retrying in 10 seconds...")
            time.sleep(10)