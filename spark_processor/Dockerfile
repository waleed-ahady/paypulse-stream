FROM apache/spark:4.0.0-python3

# Run commands as root while installing dependencies.
USER root

# Prevent Python bytecode files and enable immediate log output.
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Use a writable home directory for Spark and Ivy dependency downloads.
ENV HOME=/tmp
ENV IVY_HOME=/tmp/.ivy2

# Set the application directory.
WORKDIR /app

# Install the PostgreSQL JDBC driver.
RUN mkdir -p /opt/spark/jars \
    && curl --fail --location \
       --output /opt/spark/jars/postgresql-42.7.5.jar \
       https://jdbc.postgresql.org/download/postgresql-42.7.5.jar

# Copy the Spark processing code.
COPY jobs ./jobs

# Create writable directories for checkpoints, rejected records, and Ivy.
RUN mkdir -p /app/data/checkpoints \
    /app/data/rejected \
    /tmp/.ivy2 \
    && chown -R spark:spark /app /tmp/.ivy2

# Run the streaming process as the non-root Spark user.
USER spark

RUN pip install --no-cache-dir psycopg2-binary

# Start Spark Structured Streaming with the Kafka connector.
CMD ["/opt/spark/bin/spark-submit", "--conf", "spark.jars.ivy=/tmp/.ivy2", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0", "--jars", "/opt/spark/jars/postgresql-42.7.5.jar", "/app/jobs/payment_stream.py"]