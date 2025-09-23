








|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||FROM bitnami/spark:3.5

USER root

# Create dedicated sparkuser
RUN adduser --disabled-password --gecos '' --shell /bin/bash --uid 1001 sparkuser

# Create directories for Ivy cache and set permissions
RUN mkdir -p /opt/bitnami/.ivy2 /opt/bitnami/.m2 && \
    chown -R sparkuser:sparkuser /opt/bitnami/.ivy2 /opt/bitnami/.m2

# Copy the aggregates job
COPY spark_aggregates.py /opt/spark-job/spark_aggregates.py
RUN chown sparkuser:sparkuser /opt/spark-job/spark_aggregates.py

# Set working directory
WORKDIR /opt/spark-job

# Switch to sparkuser
USER sparkuser

# Set HOME environment variable
ENV HOME=/home/sparkuser
ENV HADOOP_USER_NAME=sparkuser

# Run the Spark aggregates job
ENTRYPOINT ["spark-submit", \
            "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
            "--conf", "spark.jars.ivy=/opt/bitnami/.ivy2", \
            "--master", "local[*]", \
            "/opt/spark-job/spark_aggregates.py"]
