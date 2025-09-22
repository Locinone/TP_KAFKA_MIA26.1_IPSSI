# Exercice 6 : Extension du Producteur avec Géocodage

## Description

Cette implémentation étend le système pour accepter ville et pays comme arguments, en utilisant l'API de géocodage d'Open-Meteo pour convertir ces informations en coordonnées.

## Architecture

```
[Variables ENV ou Arguments] → [API Géocodage] → [API Météo] → [Kafka]
     CITY + COUNTRY              lat/lon          weather data    weather_stream
```

## Fonctionnalités Implémentées

### 1. Producer v2 avec Géocodage

**Fichier:** `kafka_producer_v2.py`

#### Arbre de Décision du Producer v2

```
[Démarrage]
    ↓
[Arguments fournis ?]
    ├─ OUI → Utiliser sys.argv[1] (city) et sys.argv[2] (country)
    └─ NON → Utiliser variables d'environnement CITY et COUNTRY
         ↓
    [Étape 1: Géocodage]
         ↓
    [API: https://geocoding-api.open-meteo.com/v1/search?name={city}]
         ├─ Succès → Récupérer latitude, longitude
         └─ Échec → Erreur et arrêt
              ↓
         [Étape 2: Données Météo]
              ↓
         [API: https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}]
              ├─ Succès → Créer message JSON
              └─ Échec → Retry
                   ↓
              [Étape 3: Publication Kafka]
                   ↓
              [Topic: weather_stream avec city/country]
```

#### Utilisation

```bash
# Avec arguments
python kafka_producer_v2.py "London" "United Kingdom"

# Avec variables d'environnement
export CITY="Berlin"
export COUNTRY="Germany"
python kafka_producer_v2.py
```

### 2. Structure des Messages

**Nouveau format weather_stream:**
```json
{
  "city": "London",
  "country": "United Kingdom", 
  "admin1": "England",
  "timestamp": 1695123456,
  "weather": {
    "temperature": 18.5,
    "windspeed": 12.3,
    "winddirection": 245,
    "weathercode": 2,
    "is_day": 1,
    "time": "2025-09-22T15:00"
  },
  "location_info": {
    "timezone": "Europe/London",
    "timezone_abbreviation": "BST",
    "elevation": 25.0
  }
}
```

**Différences avec la v1:**
- ✅ `city`, `country`, `admin1` au lieu de `latitude`, `longitude`
- ✅ Géocodage automatique
- ✅ Support multiple villes via variables d'environnement

### 3. Services Docker

**Nouveau service dans docker-compose.yml:**
```yaml
data-producer-v2:
  build:
    context: ./kafka-producer
    dockerfile: Dockerfile.v2
  environment:
    CITY: London
    COUNTRY: United Kingdom
    KAFKA_BROKER: kafka:9092
    TOPIC: weather_stream
```

### 4. Spark Aggregates Mis à Jour

Le service `spark-aggregates` a été adapté pour traiter les nouvelles données :

- **Schema modifié** : city/country au lieu de latitude/longitude
- **Jointures** : Utilise city/country pour les agrégats
- **Groupement** : Par fenêtre temporelle + city + country

#### Résultats d'Agrégation

```json
{
  "window_start": "2025-09-22T15:00:00.000Z",
  "window_end": "2025-09-22T15:01:00.000Z", 
  "window_size": "1_minute",
  "city": "London",
  "country": "United Kingdom",
  "wind_alerts_level1": 0,
  "wind_alerts_level2": 0,
  "heat_alerts_level1": 1,
  "heat_alerts_level2": 0,
  "min_temperature": 18.2,
  "max_temperature": 19.1,
  "avg_temperature": 18.65,
  "total_alerts": 1,
  "record_count": 3
}
```

## Lancement

### Option 1: Producer v1 (coordonnées)
```bash
docker-compose up data-producer
```

### Option 2: Producer v2 (ville/pays)
```bash
docker-compose up data-producer-v2
```

### Option 3: Les deux simultanément
```bash
docker-compose up data-producer data-producer-v2
```

## Avantages de l'Exercice 6

1. **🌍 Interface utilisateur améliorée** : Plus facile de spécifier "Paris, France" que "48.8566, 2.3522"
2. **🔄 Flexibilité** : Support de toute ville via l'API de géocodage
3. **📊 Agrégats plus lisibles** : Résultats par ville/pays au lieu de coordonnées
4. **⚙️ Configuration simplifiée** : Variables d'environnement claires
5. **🔗 API officielle** : Utilise l'API de géocodage d'Open-Meteo

## Test de l'API de Géocodage

```bash
# Test manuel de l'API
curl "https://geocoding-api.open-meteo.com/v1/search?name=paris&count=1&language=en&format=json"
```

Cette implémentation suit exactement les spécifications de l'exercice 6, en utilisant l'API de géocodage d'Open-Meteo comme demandé ! 🎯
