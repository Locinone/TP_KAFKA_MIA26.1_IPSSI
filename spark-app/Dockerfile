FROM bitnami/spark:3.5.1

USER root
WORKDIR /opt/app

COPY spark_job.py /opt/app/spark_job.py

ENV PYSPARK_PYTHON=python3

# Drop privileges to the default non-root user used by Bitnami Spark
USER 1001

CMD ["/opt/bitnami/spark/bin/spark-submit","--packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1","/opt/app/spark_job.py"]

