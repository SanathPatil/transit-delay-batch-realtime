import os
import sys
import traceback
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import gtfs_realtime_pb2

load_dotenv()

KAFKA_TOPIC = "mbta-trip-updates"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "timescaledb")
POSTGRES_DB = os.getenv("POSTGRES_DB", "transit_delays")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS mbta_trip_delays (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT,
    delay INTEGER
);
"""

def parse_protobuf(data_bytes):
    trip_updates = []
    try:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(data_bytes)
        print(f"[INFO] Parsed FeedMessage with {len(feed.entity)} entities")

        for entity in feed.entity:
            if entity.HasField('trip_update'):
                trip_id = entity.trip_update.trip.trip_id
                route_id = entity.trip_update.trip.route_id
                delay = entity.trip_update.delay if entity.trip_update.HasField('delay') else 0
                trip_updates.append((trip_id, route_id, delay))
    except Exception as e:
        print(f"[ERROR] Error parsing protobuf: {e}", file=sys.stderr)
        traceback.print_exc()
    return trip_updates

def insert_trip_delay(cursor, trip_id, route_id, delay):
    cursor.execute("""
        INSERT INTO mbta_trip_delays (trip_id, route_id, delay)
        VALUES (%s, %s, %s)
        ON CONFLICT (trip_id) DO UPDATE SET
          route_id = EXCLUDED.route_id,
          delay = EXCLUDED.delay
    """, (trip_id, route_id, delay))

def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='mbta-delay-consumer-group'
    )
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cursor = conn.cursor()

    # Create table if not exists
    cursor.execute(CREATE_TABLE_SQL)
    conn.commit()
    print("[INFO] Ensured mbta_trip_delays table exists")

    print("[INFO] Kafka consumer started, waiting for messages...")

    for message in consumer:
        trip_updates = parse_protobuf(message.value)
        for trip_id, route_id, delay in trip_updates:
            try:
                insert_trip_delay(cursor, trip_id, route_id, delay)
                conn.commit()
                print(f"[INFO] Inserted delay for trip {trip_id} route {route_id} delay {delay}")
            except Exception as e:
                print(f"[ERROR] DB insert error: {e}", file=sys.stderr)
                traceback.print_exc()

if __name__ == "__main__":
    main()
