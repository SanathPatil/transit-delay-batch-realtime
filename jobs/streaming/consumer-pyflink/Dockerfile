# Use the official Flink image with Python support
FROM apache/flink:1.16-scala_2.12

# Install dependencies for Python 3.10 and necessary tools
USER root

RUN apt-get update && apt-get install -y \
    python3.10 \
    python3.10-dev \
    python3-pip \
    wget \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 1
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.10 1

# Create plugins directory for connectors
RUN mkdir -p /opt/flink/plugins

# Download Kafka connector for Flink
# Download the correct Kafka connector JAR from Maven Central
# Download Kafka and PostgreSQL connectors
# Download required SQL connectors for Kafka + JDBC + optional JSON
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.16.2/flink-sql-connector-kafka-1.16.2.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/1.16.2/flink-connector-jdbc-1.16.2.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/postgresql/postgresql/42.5.0/postgresql-42.5.0.jar && \
    wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.16.2/flink-json-1.16.2.jar


#wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.16.2/flink-connector-kafka-1.16.2.jar -P /opt/flink/lib/ && \
#    wget https://jdbc.postgresql.org/download/postgresql-42.5.0.jar -P /opt/flink/lib/

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Copy the Python script (consumer)
COPY consumer.py gtfs_realtime_pb2.py /app/

USER flink

# Default command: submit the PyFlink job to the JobManager
CMD ["flink", "run", "-m", "flink-jobmanager:6123", "-py", "/app/consumer.py"]

