import os
from datetime import datetime, timedelta, timezone
import psycopg2
from kafka import KafkaConsumer
from dotenv import load_dotenv
import gtfs_realtime_pb2
from zoneinfo import ZoneInfo  # ✅ for timezone-aware conversions

load_dotenv()

# Config
KAFKA_TOPIC = "mbta-trip-updates"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POSTGRES_CONN_INFO = {
    "host": os.getenv("POSTGRES_HOST", "timescaledb"),
    "dbname": os.getenv("POSTGRES_DB", "transit_delays"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

# SQL to ensure table exists
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS trip_delays (
    trip_id TEXT,
    stop_id TEXT,
    stop_sequence INT,
    scheduled_arrival TIMESTAMPTZ,
    actual_arrival TIMESTAMPTZ,
    delay_seconds INTEGER,
    PRIMARY KEY (trip_id, stop_id, stop_sequence)
);
"""

# Timezone used by MBTA (America/New_York)
MBTA_TZ = ZoneInfo("America/New_York")

# Function to convert GTFS arrival_time + service day into UTC datetime
def parse_gtfs_scheduled_datetime(arrival_time: str, actual_ts: datetime) -> datetime:
    """
    Converts GTFS arrival_time (which may exceed 24:00:00) into a UTC datetime,
    using actual_ts to infer the correct service date in MBTA local time.
    """

    # Parse arrival_time like "26:30:00"
    hours, minutes, seconds = map(int, arrival_time.split(":"))
    delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

    # Local version of actual arrival timestamp
    local_actual = actual_ts.astimezone(MBTA_TZ)
    # Service date is likely 1 day before if arrival_time >= 24:00:00
    service_date = local_actual.date()
    if hours >= 24:
        service_date -= timedelta(days=1)

    local_sched_dt = datetime.combine(service_date, datetime.min.time(), tzinfo=MBTA_TZ) + delta
    return local_sched_dt.astimezone(timezone.utc)

def main():
    # Connect to Postgres
    conn = psycopg2.connect(**POSTGRES_CONN_INFO)
    cur = conn.cursor()
    cur.execute(CREATE_TABLE_SQL)
    conn.commit()

    # Start Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='latest',
        group_id='mbta-delay-consumer'
    )

    print("Consumer started, waiting for messages...")

    for msg in consumer:
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(msg.value)

        for entity in feed.entity:
            if not entity.HasField('trip_update'):
                continue

            trip_id = entity.trip_update.trip.trip_id

            for stu in entity.trip_update.stop_time_update:
                stop_id = stu.stop_id
                stop_seq = stu.stop_sequence

                # Get actual arrival time from GTFS-RT
                actual_ts = None
                if stu.HasField('arrival') and stu.arrival.HasField('time'):
                    actual_ts = datetime.fromtimestamp(stu.arrival.time, tz=timezone.utc)
                elif stu.HasField('departure') and stu.departure.HasField('time'):
                    actual_ts = datetime.fromtimestamp(stu.departure.time, tz=timezone.utc)
                if not actual_ts:
                    continue  # Skip if no usable time

                # Get scheduled arrival time string from static GTFS
                cur.execute("""
                    SELECT arrival_time FROM dim_stop_times
                    WHERE trip_id = %s AND stop_id = %s AND stop_sequence = %s
                """, (trip_id, stop_id, stop_seq))
                res = cur.fetchone()
                if not res:
                    continue
                sched_str = res[0]  # e.g. "26:35:00"

                try:
                    scheduled_ts = parse_gtfs_scheduled_datetime(sched_str, actual_ts)
                except Exception as e:
                    print(f"[WARN] Failed to parse scheduled time '{sched_str}' for trip {trip_id}: {e}")
                    continue

                delay = int((actual_ts - scheduled_ts).total_seconds())

                cur.execute("""
                    INSERT INTO trip_delays (
                        trip_id, stop_id, stop_sequence,
                        scheduled_arrival, actual_arrival, delay_seconds
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id, stop_id, stop_sequence)
                    DO UPDATE SET
                        scheduled_arrival = EXCLUDED.scheduled_arrival,
                        actual_arrival = EXCLUDED.actual_arrival,
                        delay_seconds = EXCLUDED.delay_seconds
                """, (trip_id, stop_id, stop_seq, scheduled_ts, actual_ts, delay))

                conn.commit()
                print(f"[INFO] Trip {trip_id} Stop {stop_id} → Delay: {delay} seconds")

if __name__ == "__main__":
    main()
