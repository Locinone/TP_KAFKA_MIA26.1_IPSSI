# 🗂️ Intégration HDFS avec Partitionnement par Région

## Description

Cette implémentation intègre HDFS dans l'architecture Kafka pour permettre le stockage persistant des données météo avec partitionnement par région, continent et temps.

## Architecture HDFS

```
[Kafka Topics] → [Spark HDFS Writer] → [HDFS Partitioned Storage]
     ↓                    ↓                      ↓
weather_stream    spark-hdfs-writer    /weather-data/
                                           ├── continent=Europe/
                                           │   ├── region=Europe/
                                           │   │   ├── year=2025/
                                           │   │   │   ├── month=09/
                                           │   │   │   │   ├── day=22/
                                           │   │   │   │   │   ├── hour_partition=14/
                                           │   │   │   │   │   │   └── *.parquet
                                           │   │   │   │   │   └── hour_partition=15/
                                           │   │   │   │   └── day=23/
                                           │   │   │   └── month=10/
                                           │   │   └── year=2026/
                                           │   └── region=North America/
                                           └── continent=Asia/
```

## Modifications Apportées

### 1. **Messages Enrichis** (kafka_producer_v2.py)

**Nouveau format des messages :**
```json
{
  "city": "London",
  "country": "United Kingdom",
  "admin1": "England",
  "region": "Europe",           // ← NOUVEAU: Pour partitionnement
  "continent": "Europe",        // ← NOUVEAU: Pour partitionnement
  "timestamp": 1758567241,
  "date": "2025-09-22",        // ← NOUVEAU: Pour partitionnement temporel
  "hour": "19",                // ← NOUVEAU: Pour partitionnement temporel
  "weather": {
    "temperature": 13.2,
    "windspeed": 11.8,
    "winddirection": 27,
    "weathercode": 0,
    "is_day": 0,
    "time": "2025-09-22T19:45"
  },
  "location_info": {
    "timezone": "Europe/London",
    "timezone_abbreviation": "GMT+1",
    "elevation": 23.0
  }
}
```

### 2. **Mapping Région/Continent**

**Fonctions de mapping :**
- `get_region_from_country()` : Détermine la région géographique
- `get_continent_from_country()` : Détermine le continent

**Exemples de mapping :**
- France → Europe → Europe
- United Kingdom → Europe → Europe  
- United States → North America → North America
- Japan → Asia → Asia

### 3. **Service Spark HDFS Writer**

**Fichier :** `spark-hdfs-writer/spark_hdfs_writer.py`

**Fonctionnalités :**
- Lit depuis `weather_stream`
- Partitionne par : `continent`, `region`, `year`, `month`, `day`, `hour_partition`
- Écrit en format Parquet optimisé
- Trigger toutes les 30 secondes

### 4. **Schémas Mis à Jour**

**Tous les services Spark** ont été mis à jour pour traiter :
- `region` : Région géographique
- `continent` : Continent
- `date` : Date au format YYYY-MM-DD
- `hour` : Heure au format HH

## Services Docker

### Nouveaux Services
```yaml
namenode:           # HDFS NameNode (Port 9870, 9000)
datanode:           # HDFS DataNode
spark-hdfs-writer:  # Service Spark pour écrire dans HDFS
```

### Configuration HDFS
- **NameNode** : `hdfs://namenode:9000`
- **Web UI** : http://localhost:9870
- **Path** : `/weather-data`
- **Format** : Parquet (optimisé pour analytics)

## Structure de Partitionnement

### Niveaux de Partitionnement
1. **continent** : Europe, North America, Asia, Africa, Oceania
2. **region** : Europe, North America, South America, Asia, Africa, Oceania
3. **year** : 2025, 2026, etc.
4. **month** : 01, 02, ..., 12
5. **day** : 01, 02, ..., 31
6. **hour_partition** : 00, 01, ..., 23

### Exemple de Structure
```
/weather-data/
├── continent=Europe/
│   ├── region=Europe/
│   │   ├── year=2025/
│   │   │   ├── month=09/
│   │   │   │   ├── day=22/
│   │   │   │   │   ├── hour_partition=14/
│   │   │   │   │   │   ├── part-00000-xxx.parquet
│   │   │   │   │   │   └── part-00001-xxx.parquet
│   │   │   │   │   └── hour_partition=15/
│   │   │   │   └── day=23/
│   │   │   └── month=10/
│   │   └── year=2026/
│   └── region=North America/
└── continent=Asia/
```

## Avantages du Partitionnement

### 1. **Performance de Requête**
- Filtrage rapide par région/continent
- Scan minimal des données pertinentes
- Optimisation des requêtes analytiques

### 2. **Gestion des Données**
- Suppression facile des anciennes données
- Compression par partition
- Maintenance granulaire

### 3. **Scalabilité**
- Distribution des données
- Parallélisation des traitements
- Équilibrage de charge

## Test du Système

### Script de Test
```bash
./test_hdfs_partitioning.sh
```

### Vérifications
1. **HDFS Status** : `hdfs dfsadmin -report`
2. **Structure** : `hdfs dfs -ls -R /weather-data`
3. **Web UI** : http://localhost:9870
4. **Données** : Vérification des partitions créées

### Commandes Utiles
```bash
# Voir la structure HDFS
docker-compose exec namenode hdfs dfs -ls -R /weather-data

# Voir les données d'une partition
docker-compose exec namenode hdfs dfs -cat /weather-data/continent=Europe/region=Europe/year=2025/month=09/day=22/hour_partition=14/part-*.parquet

# Vérifier l'espace disque
docker-compose exec namenode hdfs dfs -du -h /weather-data
```

## Intégration avec l'Architecture Existante

### Flux de Données Complet
```
[Producers] → [Kafka] → [Spark-APP] → [Kafka] → [Spark-AGGREGATES] → [Kafka]
     ↓           ↓          ↓           ↓            ↓               ↓
[Producers] → [Kafka] → [Spark-HDFS-Writer] → [HDFS Partitioned]
```

### Services Impactés
- **kafka_producer_v2.py** : Messages enrichis
- **spark-aggregates.py** : Schémas mis à jour
- **spark-hdfs-writer.py** : Nouveau service
- **docker-compose.yml** : Services HDFS ajoutés

Cette intégration HDFS permet un stockage persistant optimisé pour les analyses géographiques et temporelles ! 🎯
