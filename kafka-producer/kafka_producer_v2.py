import json
import time
import sys
import requests
import os
from kafka import KafkaProducer

# Configuration Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
TOPIC = os.getenv('TOPIC', 'weather_stream')

# Configuration Ville/Pays depuis variables d'environnement ou arguments
if len(sys.argv) >= 3:
    CITY = sys.argv[1]
    COUNTRY = sys.argv[2]
    print(f"📍 Arguments utilisés: {CITY}, {COUNTRY}")
else:
    CITY = os.getenv('CITY', 'Paris')
    COUNTRY = os.getenv('COUNTRY', 'France')
    print(f"🌍 Variables d'environnement utilisées: {CITY}, {COUNTRY}")

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


def get_region_from_country(country):
    """
    Détermine la région géographique pour le partitionnement HDFS
    """
    region_mapping = {
        # Europe
        "France": "Europe",
        "United Kingdom": "Europe", 
        "Germany": "Europe",
        "Spain": "Europe",
        "Italy": "Europe",
        "Netherlands": "Europe",
        "Belgium": "Europe",
        "Switzerland": "Europe",
        "Austria": "Europe",
        "Sweden": "Europe",
        "Norway": "Europe",
        "Denmark": "Europe",
        "Finland": "Europe",
        "Poland": "Europe",
        "Czech Republic": "Europe",
        "Hungary": "Europe",
        "Romania": "Europe",
        "Bulgaria": "Europe",
        "Greece": "Europe",
        "Portugal": "Europe",
        "Ireland": "Europe",
        
        # Amérique du Nord
        "United States": "North America",
        "Canada": "North America",
        "Mexico": "North America",
        
        # Amérique du Sud
        "Brazil": "South America",
        "Argentina": "South America",
        "Chile": "South America",
        "Colombia": "South America",
        "Peru": "South America",
        "Venezuela": "South America",
        
        # Asie
        "China": "Asia",
        "Japan": "Asia",
        "India": "Asia",
        "South Korea": "Asia",
        "Thailand": "Asia",
        "Singapore": "Asia",
        "Malaysia": "Asia",
        "Indonesia": "Asia",
        "Philippines": "Asia",
        "Vietnam": "Asia",
        "Taiwan": "Asia",
        "Hong Kong": "Asia",
        
        # Océanie
        "Australia": "Oceania",
        "New Zealand": "Oceania",
        
        # Afrique
        "South Africa": "Africa",
        "Egypt": "Africa",
        "Nigeria": "Africa",
        "Kenya": "Africa",
        "Morocco": "Africa",
        "Tunisia": "Africa",
        "Algeria": "Africa",
    }
    
    return region_mapping.get(country, "Other")


def get_continent_from_country(country):
    """
    Détermine le continent pour le partitionnement HDFS
    """
    continent_mapping = {
        # Europe
        "France": "Europe",
        "United Kingdom": "Europe",
        "Germany": "Europe",
        "Spain": "Europe",
        "Italy": "Europe",
        "Netherlands": "Europe",
        "Belgium": "Europe",
        "Switzerland": "Europe",
        "Austria": "Europe",
        "Sweden": "Europe",
        "Norway": "Europe",
        "Denmark": "Europe",
        "Finland": "Europe",
        "Poland": "Europe",
        "Czech Republic": "Europe",
        "Hungary": "Europe",
        "Romania": "Europe",
        "Bulgaria": "Europe",
        "Greece": "Europe",
        "Portugal": "Europe",
        "Ireland": "Europe",
        
        # Amérique du Nord
        "United States": "North America",
        "Canada": "North America",
        "Mexico": "North America",
        
        # Amérique du Sud
        "Brazil": "South America",
        "Argentina": "South America",
        "Chile": "South America",
        "Colombia": "South America",
        "Peru": "South America",
        "Venezuela": "South America",
        
        # Asie
        "China": "Asia",
        "Japan": "Asia",
        "India": "Asia",
        "South Korea": "Asia",
        "Thailand": "Asia",
        "Singapore": "Asia",
        "Malaysia": "Asia",
        "Indonesia": "Asia",
        "Philippines": "Asia",
        "Vietnam": "Asia",
        "Taiwan": "Asia",
        "Hong Kong": "Asia",
        
        # Océanie
        "Australia": "Oceania",
        "New Zealand": "Oceania",
        
        # Afrique
        "South Africa": "Africa",
        "Egypt": "Africa",
        "Nigeria": "Africa",
        "Kenya": "Africa",
        "Morocco": "Africa",
        "Tunisia": "Africa",
        "Algeria": "Africa",
    }
    
    return continent_mapping.get(country, "Unknown")


def get_coordinates_from_city(city, country):
    """
    Utilise l'API de géocodage d'Open-Meteo pour obtenir les coordonnées
    """
    try:
        # API de géocodage Open-Meteo
        geocoding_url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1&language=en&format=json"
        print(f"🗺️  Getting coordinates from: {geocoding_url}")
        
        response = requests.get(geocoding_url, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('results'):
            print(f"❌ Ville '{city}' non trouvée dans l'API de géocodage")
            return None
            
        result = data['results'][0]
        return {
            "latitude": result['latitude'],
            "longitude": result['longitude'],
            "name": result['name'],
            "country": result.get('country', country),
            "admin1": result.get('admin1', ''),
            "timezone": result.get('timezone', '')
        }
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erreur lors de la requête de géocodage: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Erreur lors du parsing JSON géocodage: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur inattendue géocodage: {e}")
        return None


def fetch_weather_data_by_city(city, country):
    """
    Récupère les données météo actuelles depuis l'API Open-Meteo par ville et pays
    """
    try:
        # Étape 1: Obtenir les coordonnées
        location_info = get_coordinates_from_city(city, country)
        if not location_info:
            return None
            
        latitude = location_info['latitude']
        longitude = location_info['longitude']
        
        # Étape 2: Obtenir les données météo avec les coordonnées
        weather_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true&timezone=auto"
        print(f"🌤️  Fetching weather data from: {weather_url}")
        
        response = requests.get(weather_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        # Extraire les données météo actuelles
        current_weather = data.get('current_weather', {})
        
        # Déterminer la région pour le partitionnement HDFS
        region = get_region_from_country(location_info['country'])
        
        # Enrichir avec ville/pays, région et timestamp (SANS coordonnées dans le message final)
        weather_data = {
            "city": location_info['name'],
            "country": location_info['country'],
            "admin1": location_info.get('admin1', ''),
            "region": region,  # Pour partitionnement HDFS
            "continent": get_continent_from_country(location_info['country']),  # Pour partitionnement HDFS
            "timestamp": int(time.time()),
            "date": time.strftime("%Y-%m-%d"),  # Pour partitionnement par date
            "hour": time.strftime("%H"),  # Pour partitionnement par heure
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
        print(f"❌ Erreur lors de la requête API météo: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"❌ Erreur lors du parsing JSON météo: {e}")
        return None
    except Exception as e:
        print(f"❌ Erreur inattendue météo: {e}")
        return None
    
if __name__ == "__main__":
    print("🌤️  Starting weather data producer v2 (with geocoding)...")
    print(f"📍 Location: {CITY}, {COUNTRY}")
    print(f"📡 Sending to topic: {TOPIC}")
    print("=" * 50)
    
    # Afficher les options d'utilisation
    if len(sys.argv) < 3:
        print("💡 Usage alternatives:")
        print("   python kafka_producer_v2.py <city> <country>")
        print("   ou utiliser les variables d'environnement CITY et COUNTRY")
        print("")
    
    while True:
        try:
            # Récupérer les données météo depuis l'API Open-Meteo
            data = fetch_weather_data_by_city(CITY, COUNTRY)
            
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