#!/bin/bash

echo "🗂️  Test du Partitionnement HDFS"
echo "================================"

echo "1. Démarrage des services HDFS..."
docker-compose up -d namenode datanode

echo "2. Attente du démarrage de HDFS (30s)..."
sleep 30

echo "3. Vérification de l'état de HDFS..."
docker-compose exec namenode hdfs dfsadmin -report

echo "4. Création du répertoire de données..."
docker-compose exec namenode hdfs dfs -mkdir -p /weather-data

echo "5. Démarrage des producers avec partitionnement..."
docker-compose up -d data-producer-london data-producer-paris data-producer-new-york

echo "6. Démarrage du writer HDFS..."
docker-compose up -d spark-hdfs-writer

echo "7. Attente de l'écriture de données (60s)..."
sleep 60

echo "8. Vérification de la structure de partitionnement..."
echo "Structure des partitions dans HDFS:"
docker-compose exec namenode hdfs dfs -ls -R /weather-data

echo ""
echo "9. Exemple de données par partition:"
echo "Europe/London:"
docker-compose exec namenode hdfs dfs -ls /weather-data/continent=Europe/region=Europe/city=London/ 2>/dev/null || echo "Pas encore de données"

echo "Europe/Paris:"
docker-compose exec namenode hdfs dfs -ls /weather-data/continent=Europe/region=Europe/city=Paris/ 2>/dev/null || echo "Pas encore de données"

echo "North America/New York:"
docker-compose exec namenode hdfs dfs -ls /weather-data/continent=North\ America/region=North\ America/city=New\ York/ 2>/dev/null || echo "Pas encore de données"

echo ""
echo "10. Arrêt des services..."
# docker-compose stop data-producer-london data-producer-paris data-producer-new-york spark-hdfs-writer

echo ""
echo "✅ Test terminé!"
echo "📊 Interface HDFS: http://localhost:9870"
