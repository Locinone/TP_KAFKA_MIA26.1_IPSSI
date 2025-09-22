FROM python:3.10-slim

WORKDIR /app

# Copier d'abord requirements.txt pour optimiser le cache Docker
COPY requirements.txt .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . .

# Les arguments latitude et longitude doivent être passés lors du lancement du container
# Exemple: docker run -it kafka-producer 48.8566 2.3522
CMD ["python", "kafka_producer.py"]