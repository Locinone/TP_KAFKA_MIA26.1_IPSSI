FROM bitnami/spark:3.5.1

USER root

# Create directories for Ivy cache and set permissions
RUN mkdir -p /opt/bitnami/.ivy2 /opt/bitnami/.m2 && \
    chown -R 1001:1001 /opt/bitnami/.ivy2 /opt/bitnami/.m2

WORKDIR /opt/app

COPY spark_job.py /opt/app/spark_job.py
RUN chown 1001:1001 /opt/app/spark_job.py

ENV PYSPARK_PYTHON=python3

# Drop privileges to the default non-root user used by Bitnami Spark
USER 1001

# Set HOME environment variable
ENV HOME=/opt/bitnami

CMD ["/opt/bitnami/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1","--conf","spark.jars.ivy=/opt/bitnami/.ivy2","/opt/app/spark_job.py"]

