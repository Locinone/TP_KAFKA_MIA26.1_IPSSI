#!/bin/bash

echo "🚀 Test des Agrégats en Temps Réel"
echo "=================================="

echo "1. Arrêt des services existants..."
docker-compose down

echo "2. Construction des images..."
docker-compose build spark-aggregates

echo "3. Démarrage des services..."
docker-compose up -d

echo "4. Attente du démarrage de Kafka (30s)..."
sleep 30

echo "5. Vérification des topics..."
docker-compose exec kafka kafka-topics.sh --list --zookeeper zookeeper:2181

echo "6. Monitoring des agrégats..."
echo "   - Fenêtre 1: Logs du service spark-aggregates"
echo "   - Fenêtre 2: Consumer des agrégats"
echo ""
echo "Commandes utiles:"
echo "  docker-compose logs -f spark-aggregates    # Voir les logs Spark"
echo "  docker-compose logs -f data-consumer-aggregates  # Voir les résultats"
echo ""
echo "Pour arrêter: docker-compose down"
