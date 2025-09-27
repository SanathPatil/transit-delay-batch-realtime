from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment, DataTypes
from pyflink.table.udf import ScalarFunction, udf
from pyflink.table.expressions import col
from jobs.streaming.consumer.gtfs_realtime_pb2 import FeedMessage
import sys
import traceback


class ParseProtobuf(ScalarFunction):
    def eval(self, data_bytes: bytes):
        print("ParseProtobuf.eval called")  # log each eval call
        try:
            feed = FeedMessage()
            feed.ParseFromString(data_bytes)
            for entity in feed.entity:
                if entity.HasField('trip_update'):
                    trip_id = entity.trip_update.trip.trip_id
                    route_id = entity.trip_update.trip.route_id
                    delay = entity.trip_update.delay if entity.trip_update.HasField('delay') else 0
                    print(f"Parsed trip_id={trip_id}, route_id={route_id}, delay={delay}")
                    return trip_id, route_id, delay
            return None, None, 0
        except Exception as e:
            print(f"Error parsing protobuf: {e}", file=sys.stderr)
            traceback.print_exc()
            return None, None, 0


def main():
    try:
        print("Starting Flink job")
        env = StreamExecutionEnvironment.get_execution_environment()
        env.enable_checkpointing(10 * 1000)
        print("Checkpointing enabled")

        settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
        t_env = StreamTableEnvironment.create(env, environment_settings=settings)
        print("Table environment created")

        parse_protobuf = udf(ParseProtobuf(), result_type=DataTypes.ROW([
            DataTypes.FIELD("trip_id", DataTypes.STRING()),
            DataTypes.FIELD("route_id", DataTypes.STRING()),
            DataTypes.FIELD("delay", DataTypes.INT())
        ]))
        t_env.create_temporary_function("parse_protobuf", parse_protobuf)
        print("UDF registered")

        source_ddl = """
        CREATE TABLE kafka_source (
            data BYTES
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'mbta-trip-updates',
            'properties.bootstrap.servers' = 'kafka:9092',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'raw'
        )
        """
        t_env.execute_sql(source_ddl)
        print("Kafka source table created")

        sink_ddl = """
        CREATE TABLE mbta_trip_delays (
            trip_id STRING,
            route_id STRING,
            delay INT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://timescaledb:5432/transit_delays',
            'table-name' = 'mbta_trip_delays',
            'username' = 'transit_user',
            'password' = 'Tr!5ns1tD4l@ys2025',
            'driver' = 'org.postgresql.Driver'
        )
        """
        t_env.execute_sql(sink_ddl)
        print("TimescaleDB sink table created")

        table = t_env.from_path("kafka_source")
        print("Kafka source table loaded")

        parsed = table.select(col("data"), col("data").call("parse_protobuf").alias("parsed")) \
            .select(
                col("parsed").trip_id,
                col("parsed").route_id,
                col("parsed").delay
            )
        print("Applied UDF and selected parsed columns")

        parsed.execute_insert("mbta_trip_delays")
        print("Streaming job submitted")

        t_env.execute("mbta_trip_delay_stream_job")  # <-- THIS starts the actual stream

    except Exception as e:
        print(f"Job failed: {e}", file=sys.stderr)
        traceback.print_exc()


if __name__ == "__main__":
    main()
