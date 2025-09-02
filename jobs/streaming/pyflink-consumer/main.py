from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Configuration

def job():
    print("✅ Submitting to remote Flink cluster...")

    config = Configuration()
    config.set_string("rest.address", "flink-jobmanager")
    config.set_integer("rest.port", 8081)

    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

    env.set_parallelism(1)

    env.from_collection([("NYC", 10), ("LA", 20)]).print()

    env.execute("Cluster PyFlink Job")

if __name__ == "__main__":
    print("🚀 Starting PyFlink Cluster Job...")
    job()
