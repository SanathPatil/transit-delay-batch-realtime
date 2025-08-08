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
# Write DataFrame to DB
# ----------------------
def write_to_db(df, table_name):
    logger.info(f"Writing data to TimescaleDB table: {table_name}")
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{POSTGRES_HOST}:5432/{POSTGRES_DB}") \
        .option("dbtable", table_name) \
        .option("user", POSTGRES_USER) \
        .option("password", POSTGRES_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

# ----------------------
# Load GTFS file as DataFrame
# ----------------------
def load_gtfs_file(spark, filename):
    path = os.path.join(GTFS_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found after extraction")
    logger.info(f"Reading GTFS file: {filename}")
    return spark.read.csv(path, header=True, inferSchema=True)

# ----------------------
# Spark Batch Job
# ----------------------
def run_spark_batch_job():
    spark = SparkSession.builder \
        .appName("GTFSBatchLoader") \
        .getOrCreate()

    # Load and write each dimension table
    routes_df = load_gtfs_file(spark, "routes.txt")
    write_to_db(routes_df, "dim_routes")

    trips_df = load_gtfs_file(spark, "trips.txt")
    write_to_db(trips_df, "dim_trips")

    stops_df = load_gtfs_file(spark, "stops.txt")
    write_to_db(stops_df, "dim_stops")

    calendar_df = load_gtfs_file(spark, "calendar.txt")
    write_to_db(calendar_df, "dim_calendar")

    # calendar_dates is optional and can override calendar.txt service exceptions
    # calendar_dates_path = os.path.join(GTFS_DIR, "calendar_dates.txt")
    # if os.path.exists(calendar_dates_path):
    #     calendar_dates_df = load_gtfs_file(spark, "calendar_dates.txt")
    #     write_to_db(calendar_dates_df, "dim_calendar_dates")

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