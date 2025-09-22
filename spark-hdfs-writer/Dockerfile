FROM bitnami/spark:3.5

USER root

RUN adduser --disabled-password --gecos '' --shell /bin/bash --uid 1001 sparkuser

# Installer requests pour l'API WebHDFS
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install requests

# Créé les dossiers cache et change les droits
RUN mkdir -p /opt/bitnami/.ivy2 /opt/bitnami/.m2 && \
    chown -R sparkuser:sparkuser /opt/bitnami/.ivy2 /opt/bitnami/.m2

# Copie du job Spark et change les droits
COPY spark_hdfs_writer.py /opt/spark-job/spark_hdfs_writer.py
COPY spark_hdfs_writer_simple.py /opt/spark-job/spark_hdfs_writer_simple.py
RUN chown sparkuser:sparkuser /opt/spark-job/spark_hdfs_writer.py /opt/spark-job/spark_hdfs_writer_simple.py

WORKDIR /opt/spark-job

USER sparkuser

ENV HOME=/home/sparkuser
ENV HADOOP_USER_NAME=sparkuser

ENTRYPOINT ["spark-submit", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
            "--conf", "spark.jars.ivy=/opt/bitnami/.ivy2", \
            "--master", "local[*]", \
            "/opt/spark-job/spark_hdfs_writer_simple.py"]
