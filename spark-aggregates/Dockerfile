FROM bitnami/spark:3.5

USER root

# Create directories for Ivy cache and set permissions
RUN mkdir -p /opt/bitnami/.ivy2 /opt/bitnami/.m2 && \
    chown -R 1001:1001 /opt/bitnami/.ivy2 /opt/bitnami/.m2

# Copy the aggregates job
COPY spark_aggregates.py /opt/spark-job/spark_aggregates.py
RUN chown 1001:1001 /opt/spark-job/spark_aggregates.py

# Set working directory
WORKDIR /opt/spark-job

# Switch back to non-root user
USER 1001

# Set HOME environment variable
ENV HOME=/opt/bitnami

# Run the Spark aggregates job
ENTRYPOINT ["spark-submit", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
            "--conf", "spark.jars.ivy=/opt/bitnami/.ivy2", \
            "--master", "local[*]", \
            "/opt/spark-job/spark_aggregates.py"]
