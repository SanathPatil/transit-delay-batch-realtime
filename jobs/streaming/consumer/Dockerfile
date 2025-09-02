FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .
COPY gtfs_realtime_pb2.py .

CMD ["python", "consumer.py"]
