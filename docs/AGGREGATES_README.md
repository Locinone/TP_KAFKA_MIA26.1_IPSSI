# Exercice 5 : Agrégats en Temps Réel avec Spark Streaming

## Description

Ce service implémente des agrégats en temps réel sur des fenêtres glissantes (sliding windows) en utilisant Spark Structured Streaming. Il consomme à la fois les topics `weather_stream` et `weather_transformed` pour calculer des métriques.

## Architecture

```
weather_stream ──┐
                 ├──> spark-aggregates ──> weather_aggregates
weather_transformed ─┘
```

## Fonctionnalités Implémentées

### 1. Fenêtres Glissantes
- **Fenêtre 1 minute** : Glisse toutes les 30 secondes
- **Fenêtre 5 minutes** : Glisse toutes les 1 minute

### 2. Métriques Calculées

Pour chaque fenêtre et par ville :
- **Alertes Vent**
  - Nombre d'alertes level_1
  - Nombre d'alertes level_2
- **Alertes Chaleur**
  - Nombre d'alertes level_1
  - Nombre d'alertes level_2
- **Statistiques de Température**
  - Minimum
  - Maximum
  - Moyenne
- **Total des alertes** (toutes catégories confondues)
- **Nombre d'enregistrements** dans la fenêtre

### 3. Localisation

Le service détermine la ville basée sur les coordonnées GPS :
- Paris : latitude [48.8, 48.9], longitude [2.3, 2.4]
- London : latitude [51.4, 51.6], longitude [-0.2, 0.0]
- New York : latitude [40.6, 40.8], longitude [-74.1, -73.9]
- Other : toutes les autres coordonnées

## Solution Technique : Jointure des Streams

Le service utilise la **Solution B** proposée :
1. Lit simultanément `weather_stream` (pour les coordonnées) et `weather_transformed` (pour les alertes)
2. Effectue une jointure temporelle avec tolérance de 30 secondes
3. Utilise des watermarks de 5 minutes pour gérer les données en retard

## Lancement

### Avec Docker Compose

```bash
# Démarrer tous les services
docker-compose up -d

# Voir les logs des agrégats
docker-compose logs -f spark-aggregates

# Voir les résultats dans le consumer
docker-compose logs -f data-consumer-aggregates
```

### Données de Sortie

Format JSON dans le topic `weather_aggregates` :
```json
{
  "window_start": "2025-09-22T10:00:00.000Z",
  "window_end": "2025-09-22T10:01:00.000Z",
  "window_size": "1_minute",
  "city": "Paris",
  "wind_alerts_level1": 2,
  "wind_alerts_level2": 0,
  "heat_alerts_level1": 3,
  "heat_alerts_level2": 1,
  "min_temperature": 18.5,
  "max_temperature": 28.3,
  "avg_temperature": 23.4,
  "total_alerts": 6,
  "record_count": 10
}
```

## Configuration

Variables d'environnement disponibles :
- `KAFKA_BOOTSTRAP` : Adresse du broker Kafka (défaut: kafka:9092)
- `STREAM_TOPIC` : Topic source avec coordonnées (défaut: weather_stream)
- `TRANSFORMED_TOPIC` : Topic source avec alertes (défaut: weather_transformed)
- `CHECKPOINT_DIR` : Répertoire des checkpoints (défaut: /tmp/checkpoints/weather_aggregates)

## Monitoring

Les agrégats sont affichés dans deux formats :
1. **Console** : Pour debug, mise à jour toutes les 30 secondes
2. **Topic Kafka** : `weather_aggregates` pour intégration avec d'autres systèmes

## Optimisations

- Utilisation de Spark Adaptive Query Execution (AQE)
- Watermarks pour limiter l'utilisation mémoire
- Jointure avec tolérance temporelle pour gérer les décalages
- Processing time trigger de 30 secondes pour contrôler la charge
