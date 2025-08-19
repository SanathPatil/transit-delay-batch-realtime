import time
import os
import requests
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2


class GtfsRtKafkaProducer:
    def __init__(self, topic, kafka_broker):
        self.topic = topic
        self.kafka_broker = kafka_broker
        self.producer = Producer({'bootstrap.servers': self.kafka_broker})

    def fetch_feed(self, url):
        try:
            response = requests.get(url)
            response.raise_for_status()

            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            return feed.entity

        except Exception as e:
            print(f"[ERROR] Failed to fetch or parse feed: {e}")
            return []

    def delivery_report(self, err, msg):
        if err is not None:
            print(f"[ERROR] Message delivery failed: {err}")
        else:
            print(f"[INFO] Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish_entities(self, entities):
        for entity in entities:
            try:
                self.producer.produce(self.topic, value=entity.SerializeToString(), callback=self.delivery_report)
            except Exception as e:
                print(f"[ERROR] Failed to produce message: {e}")
        self.producer.flush()
        print(f"[INFO] Published {len(entities)} entities to '{self.topic}'")

    def run(self, url, interval=30):
        print(f"[STARTED] Producing to topic: {self.topic} at broker: {self.kafka_broker}")
        while True:
            entities = self.fetch_feed(url)
            self.publish_entities(entities)
            time.sleep(interval)


if __name__ == "__main__":
    # Read Kafka broker and interval from environment variables, fallback to defaults
    KAFKA_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    TRIP_UPDATES_URL = "https://cdn.mbta.com/realtime/TripUpdates.pb"
    INTERVAL = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "30"))

    # Start producer
    trip_producer = GtfsRtKafkaProducer(topic="trip_updates", kafka_broker=KAFKA_BROKER)
    trip_producer.run(url=TRIP_UPDATES_URL, interval=INTERVAL)