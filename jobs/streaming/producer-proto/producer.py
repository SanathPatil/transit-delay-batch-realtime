# import os
# import requests
# from kafka import KafkaProducer
# from dotenv import load_dotenv
# from gtfs_realtime_pb2 import FeedMessage  # from proto_out
#
# load_dotenv()
#
# MBTA_URL = 'https://cdn.mbta.com/realtime/TripUpdates.pb' #"https://api.mbta.com/realtime/TripUpdates.pb"
# KAFKA_TOPIC = "mbta-trip-updates"
# KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
#
# producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
#
# def fetch_trip_updates():
#     print("[INFO] Fetching MBTA real-time updates...")
#     res = requests.get(MBTA_URL)
#     feed = FeedMessage()
#     feed.ParseFromString(res.content)
#
#     # for entity in feed.entity:
#     #     producer.send(KAFKA_TOPIC, entity.SerializeToString())
#     #     print(f"[INFO] Sent entity {entity.id} to Kafka")
#     producer.send(KAFKA_TOPIC, feed.SerializeToString())
#     print(f"[INFO] Sent full feed with {len(feed.entity)} entities to Kafka")
#     producer.flush()
#
# if __name__ == "__main__":
#     fetch_trip_updates()
import os
import time
import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from gtfs_realtime_pb2 import FeedMessage  # from proto_out

load_dotenv()

MBTA_URL = 'https://cdn.mbta.com/realtime/TripUpdates.pb'
KAFKA_TOPIC = "mbta-trip-updates"
KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POLL_INTERVAL_SECONDS = 30  # how often to fetch updates

# Try to connect to Kafka with retries
producer = None
for i in range(10):
    try:
        producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
        print("[INFO] Connected to Kafka successfully.")
        break
    except NoBrokersAvailable:
        print(f"[WARN] Kafka not available, retrying in 5 seconds... ({i+1}/10)")
        time.sleep(5)
else:
    print("[ERROR] Kafka is not available after 10 retries. Exiting.")
    exit(1)

def fetch_and_send_trip_updates():
    try:
        print("[INFO] Fetching MBTA real-time updates...")
        res = requests.get(MBTA_URL)
        res.raise_for_status()

        feed = FeedMessage()
        feed.ParseFromString(res.content)

        producer.send(KAFKA_TOPIC, feed.SerializeToString())
        producer.flush()
        print(f"[INFO] Sent full feed with {len(feed.entity)} entities to Kafka")

    except Exception as e:
        print(f"[ERROR] Failed to fetch or send trip updates: {e}")

if __name__ == "__main__":
    while True:
        fetch_and_send_trip_updates()
        time.sleep(POLL_INTERVAL_SECONDS)