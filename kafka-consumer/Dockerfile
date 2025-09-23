FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka_consumer.py ./
COPY alerts_consumer.py ./

CMD ["python", "alerts_consumer.py"]