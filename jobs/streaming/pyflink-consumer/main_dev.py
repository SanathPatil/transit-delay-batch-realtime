# import os
# from datetime import datetime
# import logging
# import base64
# import gtfs_realtime_pb2
# import psycopg2
#
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.common.watermark_strategy import WatermarkStrategy
# from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
# from pyflink.common.typeinfo import Types
# from pyflink.datastream.functions import FlatMapFunction
# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.common import Configuration
#
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger("gtfs")
#
# class TripUpdateMapper(FlatMapFunction):
#     def open(self, runtime_context):
#         # Initialize DB connection using environment variables
#         self.conn = psycopg2.connect(
#             dbname=os.getenv("POSTGRES_DB", "transit_delays"),
#             user=os.getenv("POSTGRES_USER", "transit_user"),
#             password=os.getenv("POSTGRES_PASSWORD"),
#             host=os.getenv("POSTGRES_HOST", "timescaledb"),
#             port=5432
#         )
#         self.cursor = self.conn.cursor()
#
#     def close(self):
#         if self.cursor:
#             self.cursor.close()
#         if self.conn:
#             self.conn.close()
#
#     def flat_map(self, message, collector):
#         try:
#             # Decode Base64-encoded GTFS message from Kafka
#             feed = gtfs_realtime_pb2.FeedMessage()
#             feed.ParseFromString(base64.b64decode(message))
#         except Exception as e:
#             logger.error(f"Failed to parse GTFS message: {e}")
#             return
#
#         for entity in feed.entity:
#             if entity.HasField("trip_update"):
#                 trip = entity.trip_update.trip
#
#                 trip_id = trip.trip_id if trip and trip.trip_id else "unknown"
#                 start_date = trip.start_date if trip and trip.start_date else None
#                 route_id = trip.route_id if trip and trip.route_id else "unknown"
#
#                 timestamp = int(entity.trip_update.timestamp) if entity.trip_update.timestamp else int(datetime.utcnow().timestamp())
#
#                 try:
#                     start_date_dt = datetime.strptime(start_date, "%Y%m%d").date() if start_date else None
#                 except Exception:
#                     start_date_dt = None
#
#                 timestamp_dt = datetime.utcfromtimestamp(timestamp)
#
#                 try:
#                     self.cursor.execute(
#                         """
#                         INSERT INTO trip_updates (trip_id, start_date, route_id, timestamp)
#                         VALUES (%s, %s, %s, %s)
#                         ON CONFLICT DO NOTHING
#                         """,
#                         (trip_id, start_date_dt, route_id, timestamp_dt)
#                     )
#                     self.conn.commit()
#                     logger.info(f"Inserted: {trip_id}, {route_id}, {timestamp_dt}")
#                 except Exception as e:
#                     logger.error(f"Failed to insert record: {e}")
#
#                 collector.collect((trip_id, start_date_dt, route_id, timestamp_dt))
#
#
# def main():
#     logger.info("Flink Job Starting...")
#
#     config = Configuration()
#     config.set_string("pipeline.jars", "file:///opt/flink/plugins/custom/flink-sql-connector-kafka-1.17.1.jar")
#
#     env = StreamExecutionEnvironment.get_execution_environment(config)
#     env.set_parallelism(1)
#
#     kafka_source = KafkaSource.builder() \
#         .set_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")) \
#         .set_topics(os.getenv("TOPIC", "trip_updates")) \
#         .set_group_id("gtfs-consumer-group") \
#         .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
#         .set_value_only_deserializer(SimpleStringSchema()) \
#         .build()
#
#     ds = env.from_source(
#         source=kafka_source,
#         watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
#         source_name="KafkaSource"
#     )
#
#     flat_ds = ds.flat_map(
#         TripUpdateMapper(),
#         output_type=Types.TUPLE([
#             Types.STRING(),         # trip_id
#             Types.SQL_DATE(),       # start_date
#             Types.STRING(),         # route_id
#             Types.SQL_TIMESTAMP()   # timestamp
#         ])
#     )
#
#     env.execute("GTFS Trip Updates Consumer")
#
#
# if __name__ == "__main__":
#     main()


import os
from datetime import datetime
import logging
import base64
import gtfs_realtime_pb2
import psycopg2

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.typeinfo import Types
from pyflink.datastream.functions import FlatMapFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Configuration

logging.basicConfig(level=logging.DEBUG)  # Set to DEBUG for maximum detail
logger = logging.getLogger("gtfs")

class TripUpdateMapper(FlatMapFunction):
    def open(self, runtime_context):
        logger.debug("TripUpdateMapper.open() called")
        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("POSTGRES_DB", "transit_delays"),
                user=os.getenv("POSTGRES_USER", "transit_user"),
                password=os.getenv("POSTGRES_PASSWORD"),
                host=os.getenv("POSTGRES_HOST", "timescaledb"),
                port=5432
            )
            self.cursor = self.conn.cursor()
            logger.info("Database connection established successfully")
        except Exception as e:
            logger.error(f"Failed to connect to DB in open(): {e}")
            raise

    def close(self):
        logger.debug("TripUpdateMapper.close() called")
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("Database connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing DB connection: {e}")

    def flat_map(self, message, collector):
        logger.debug(f"flat_map received message of length {len(message)}")
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(base64.b64decode(message))
            logger.debug(f"Parsed GTFS feed with {len(feed.entity)} entities")
        except Exception as e:
            logger.error(f"Failed to parse GTFS message: {e}")
            return

        for i, entity in enumerate(feed.entity):
            logger.debug(f"Processing entity {i} of type {entity.WhichOneof('entity')}")
            if entity.HasField("trip_update"):
                trip = entity.trip_update.trip

                trip_id = trip.trip_id if trip and trip.trip_id else "unknown"
                start_date = trip.start_date if trip and trip.start_date else None
                route_id = trip.route_id if trip and trip.route_id else "unknown"
                timestamp = int(entity.trip_update.timestamp) if entity.trip_update.timestamp else int(datetime.utcnow().timestamp())

                logger.debug(f"Extracted trip_id={trip_id}, start_date={start_date}, route_id={route_id}, timestamp={timestamp}")

                try:
                    start_date_dt = datetime.strptime(start_date, "%Y%m%d").date() if start_date else None
                    logger.debug(f"Parsed start_date to datetime: {start_date_dt}")
                except Exception as e:
                    logger.warning(f"Could not parse start_date '{start_date}': {e}")
                    start_date_dt = None

                timestamp_dt = datetime.utcfromtimestamp(timestamp)
                logger.debug(f"Converted timestamp to datetime: {timestamp_dt}")

                try:
                    self.cursor.execute(
                        """
                        INSERT INTO trip_updates (trip_id, start_date, route_id, timestamp)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT DO NOTHING
                        """,
                        (trip_id, start_date_dt, route_id, timestamp_dt)
                    )
                    self.conn.commit()
                    logger.info(f"Inserted DB record: trip_id={trip_id}, route_id={route_id}, timestamp={timestamp_dt}")
                except Exception as e:
                    logger.error(f"Failed to insert record: {e}")

                collector.collect((trip_id, start_date_dt, route_id, timestamp_dt))
            else:
                logger.debug(f"Entity {i} has no trip_update field, skipping")

def main():
    logger.info("Starting Flink Job...")

    config = Configuration()
    logger.info("config loaded...")
    config.set_string("pipeline.jars", "file:///opt/flink/plugins/custom/flink-sql-connector-kafka-1.17.1.jar")
    logger.info("config set string...")
    logger.debug("Creating StreamExecutionEnvironment")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    logger.info("env...")
    env.set_parallelism(1)

    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "broker:29092")
    kafka_topic = os.getenv("TOPIC", "trip_updates")
    logger.info(f"Kafka config - Bootstrap servers: {kafka_bootstrap}, Topic: {kafka_topic}")

    logger.info("Creating Kafka Source...")
    try:
        kafka_source = KafkaSource.builder() \
            .set_bootstrap_servers(kafka_bootstrap) \
            .set_topics(kafka_topic) \
            .set_group_id("gtfs-consumer-group") \
            .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
            .set_value_only_deserializer(SimpleStringSchema()) \
            .build()
        logger.info("Kafka Source created successfully.")
    except Exception as e:
        logger.error(f"Error creating Kafka source: {e}")

    logger.info("Creating Flink source stream from Kafka...")
    ds = env.from_source(
        source=kafka_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name="KafkaSource"
    )

    flat_ds = ds.flat_map(
        TripUpdateMapper(),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.SQL_DATE(),
            Types.STRING(),
            Types.SQL_TIMESTAMP()
        ])
    )
    flat_ds.print()


    logger.info("Executing Flink job")
    logger.info("Attempting to execute Flink job")
    env.execute("GTFS Trip Updates Consumer")

if __name__ == "__main__":
    main()