FROM python:3.10-slim

WORKDIR /app

# Copy only necessary files
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Copy compiled protobuf only
COPY proto_out/gtfs_realtime_pb2.py .

CMD ["python", "producer.py"]