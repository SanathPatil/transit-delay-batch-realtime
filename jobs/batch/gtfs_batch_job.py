import os
import zipfile
import requests
import logging
from pyspark.sql import SparkSession

# ----------------------
# Logging Setup
# ----------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
logger = logging.getLogger(__name__)

# ----------------------
# Environment Variables
# ----------------------
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
GTFS_URL = os.getenv("GTFS_URL", "http://web.mta.info/developers/data/nyct/subway/google_transit.zip")

GTFS_DIR = "gtfs_static"
ZIP_PATH = os.path.join(GTFS_DIR, "gtfs_feed.zip")


# ----------------------
# Download + Extract GTFS
# ----------------------
def download_and_extract_gtfs():
    os.makedirs(GTFS_DIR, exist_ok=True)

    logger.info(f"Downloading GTFS feed from {GTFS_URL}")
    response = requests.get(GTFS_URL)
    if response.status_code != 200:
        raise Exception(f"Failed to download GTFS feed: HTTP {response.status_code}")

    with open(ZIP_PATH, "wb") as f:
        f.write(response.content)

    logger.info("Extracting GTFS ZIP")
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(GTFS_DIR)

    os.remove(ZIP_PATH)
    logger.info("GTFS feed extracted successfully")


# ----------------------
# Spark Batch Job
# ----------------------
def run_spark_batch_job():
    spark = SparkSession.builder \
        .appName("GTFSBatchLoader") \
        .getOrCreate()

    trips_path = os.path.join(GTFS_DIR, "trips.txt")
    if not os.path.exists(trips_path):
        raise FileNotFoundError(f"{trips_path} not found after extraction")

    logger.info(f"Reading GTFS file: {trips_path}")
    trips_df = spark.read.csv(trips_path, header=True, inferSchema=True)

    logger.info("Writing data to TimescaleDB table: trips")
    trips_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
        .option("dbtable", "trips") \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

    logger.info("Batch job completed successfully")


# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    try:
        download_and_extract_gtfs()
        run_spark_batch_job()
    except Exception as e:
        logger.exception("Error during batch job execution")