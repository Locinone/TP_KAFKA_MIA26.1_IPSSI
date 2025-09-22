FROM python:3.10-slim

WORKDIR /app

RUN pip install --no-cache-dir kafka-python

COPY . .

CMD ["python", "kafka_producer.py"]