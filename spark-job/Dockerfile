FROM python:3.10-slim

WORKDIR /app

# Install Java for Spark and PySpark
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

RUN pip install --no-cache-dir pyspark==3.5.1

COPY stream_job.py /app/stream_job.py

ENV KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
    INPUT_TOPIC=weather_stream \
    OUTPUT_TOPIC=weather_transformed

CMD ["python", "/app/stream_job.py"]

