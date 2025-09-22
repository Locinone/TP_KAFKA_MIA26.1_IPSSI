# Producteur de Données Météo en Temps Réel

## Description

Ce producteur Kafka interroge l'API Open-Meteo pour récupérer les données météorologiques actuelles et les envoie dans le topic `weather_stream`.

## Fonctionnalités

- 🌤️ **Récupération de données météo** : Interroge l'API Open-Meteo en temps réel
- 📍 **Coordonnées personnalisables** : Accepte latitude et longitude en arguments
- 🔄 **Mise à jour automatique** : Récupère les données toutes les 60 secondes
- 🛡️ **Gestion d'erreurs** : Retry automatique en cas d'échec
- 📊 **Données enrichies** : Inclut température, vent, code météo, timezone, etc.

## Structure des Données

Le producteur envoie des messages JSON avec la structure suivante :

```json
{
  "latitude": 48.8566,
  "longitude": 2.3522,
  "timestamp": 1695123456,
  "weather": {
    "temperature": 15.3,
    "windspeed": 5.1,
    "winddirection": 180,
    "weathercode": 3,
    "is_day": 1,
    "time": "2025-09-22T11:00"
  },
  "location_info": {
    "timezone": "Europe/Paris",
    "timezone_abbreviation": "CEST",
    "elevation": 35.0
  }
}
```

## Utilisation

### 1. Avec Docker (Recommandé)

```bash
# Construire l'image
docker build -t kafka-weather-producer ./kafka-producer

# Lancer le producteur avec des coordonnées spécifiques
docker run -it --network kafka-network kafka-weather-producer python kafka_producer.py 48.8566 2.3522

# Exemples de coordonnées :
# Paris : 48.8566 2.3522
# New York : 40.7128 -74.0060
# Tokyo : 35.6762 139.6503
# Londres : 51.5074 -0.1278
```

### 2. Avec Docker Compose

Modifiez le `docker-compose.yml` pour inclure les arguments :

```yaml
services:
  kafka-producer:
    build: ./kafka-producer
    command: ["python", "kafka_producer.py", "48.8566", "2.3522"]
    depends_on:
      - kafka
    networks:
      - kafka-network
```

### 3. En local (avec Python)

```bash
# Installer les dépendances
pip install kafka-python requests

# Lancer le producteur
python kafka_producer.py 48.8566 2.3522
```

## Arguments Requis

Le script nécessite exactement 2 arguments :

1. **Latitude** : Coordonnée de latitude (ex: 48.8566)
2. **Longitude** : Coordonnée de longitude (ex: 2.3522)

## Gestion des Erreurs

- **Erreur de connexion API** : Retry automatique après 10 secondes
- **Données manquantes** : Cycle suivant après 60 secondes
- **Interruption utilisateur** : Arrêt propre avec Ctrl+C

## Codes Météo

L'API Open-Meteo utilise des codes météo standardisés :

- **0** : Ciel dégagé
- **1-3** : Partiellement nuageux
- **45, 48** : Brouillard
- **51-67** : Bruine/Pluie légère
- **71-77** : Chute de neige
- **80-82** : Averses
- **85-86** : Averses de neige
- **95** : Orage
- **96-99** : Orage avec grêle

## Monitoring

Le producteur affiche des logs détaillés :

- 🌍 Coordonnées utilisées
- 🌤️ URL de l'API appelée
- 🚀 Données envoyées à Kafka
- ⏳ Temps d'attente entre les cycles
- ❌ Erreurs rencontrées

## Test de l'API

Un script de test est fourni pour vérifier la connectivité :

```bash
python3 test_weather_api.py
```

Ce script teste l'API avec Paris et New York pour valider le fonctionnement.
