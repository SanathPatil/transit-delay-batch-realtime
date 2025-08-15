FROM bitnami/spark:3.4.1

# Switch to root to install python3, pip, wget
USER root

RUN apt-get update && apt-get install -y python3 python3-pip wget && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r /app/requirements.txt

# Add PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.6.0.jar -P /opt/bitnami/spark/jars/

# Copy batch job script only (no gtfs_static copy)
COPY gtfs_batch_job.py /app/gtfs_batch_job.py
COPY generate_schema.py /app/generate_schema.py
COPY data_quality.py /app/data_quality.py
COPY schemas/ /app/schemas/
WORKDIR /app

# Run the PySpark batch job using spark-submit
CMD ["tail", "-f", "/dev/null"]
#CMD ["spark-submit", "gtfs_batch_job.py"]