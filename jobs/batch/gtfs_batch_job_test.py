import os
import zipfile
import requests
import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

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
# Required fields per schema for validation
# ----------------------
required_fields_map = {
    "routes": {"route_id"},
    "trips": {"route_id", "service_id", "trip_id", "direction_id", "shape_id"},
    "stops": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    "calendar": {"service_id", "start_date", "end_date"}
}

# ----------------------
# Load schema from JSON
# ----------------------
def load_schema(name):
    schema_path = os.path.join(os.path.dirname(__file__), "schemas", f"{name}_schema.json")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    with open(schema_path, "r") as f:
        schema_dict = json.load(f)
    return StructType.fromJson(schema_dict)

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
# Load GTFS file with schema
# ----------------------
def load_gtfs_file(spark, filename, schema):
    path = os.path.join(GTFS_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"{filename} not found after extraction")
    logger.info(f"Reading GTFS file with schema: {filename}")
    return spark.read.csv(path, header=True, schema=schema)

# ----------------------
# Validate DataFrame columns & types
# ----------------------
def validate_dataframe_schema(df, expected_schema, required_fields):
    df_fields = set(df.schema.fieldNames())
    expected_fields = set(field.name for field in expected_schema.fields)

    missing = required_fields - df_fields
    if missing:
        raise ValueError(f"Missing required columns in CSV: {missing}")

    extra = df_fields - expected_fields
    if extra:
        logger.warning(f"Extra columns found in CSV: {extra}")

    # Validate data types for present columns
    for expected_field in expected_schema.fields:
        col_name = expected_field.name
        expected_type = expected_field.dataType
        if col_name in df_fields:
            actual_type = df.schema[col_name].dataType
            if actual_type != expected_type:
                raise TypeError(f"Type mismatch for column '{col_name}': expected {expected_type}, found {actual_type}")

# ----------------------
# Spark Batch Job
# ----------------------
def run_spark_batch_job():
    spark = SparkSession.builder \
        .appName("GTFSBatchLoader") \
        .getOrCreate()

    for table_name in ["routes", "trips", "stops", "calendar"]:
        schema = load_schema(table_name)
        required_fields = required_fields_map.get(table_name, set())

        filename = f"{table_name}.txt"
        df = load_gtfs_file(spark, filename, schema)

        logger.info(f"Validating schema for {filename}")
        validate_dataframe_schema(df, schema, required_fields)

        db_table = f"dim_{table_name}"
        write_to_db(df, db_table)

    logger.info("Batch job completed successfully")

# ----------------------
# Main
# ----------------------
if __name__ == "__main__":
    try:
        download_and_extract_gtfs()
        run_spark_batch_job()
    except Exception as e:
        logger.exception(f" FAILED due to {e}")
        exit(1)