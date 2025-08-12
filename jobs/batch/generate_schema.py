import json
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ----------------------
# Define GTFS Schemas
# ----------------------

routes_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("route_short_name", StringType(), True),
    StructField("route_long_name", StringType(), True),
    StructField("route_desc", StringType(), True),
    StructField("route_type", IntegerType(), True),
    StructField("route_url", StringType(), True),
    StructField("route_color", StringType(), True),
    StructField("route_text_color", StringType(), True)
])

trips_schema = StructType([
    StructField("route_id", StringType(), True),
    StructField("service_id", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("trip_headsign", StringType(), True),
    StructField("trip_short_name", StringType(), True),
    StructField("direction_id", IntegerType(), True),
    StructField("block_id", StringType(), True),
    StructField("shape_id", StringType(), True),
    StructField("wheelchair_accessible", IntegerType(), True),
    StructField("bikes_allowed", IntegerType(), True)
])

stops_schema = StructType([
    StructField("stop_id", StringType(), True),
    StructField("stop_code", StringType(), True),
    StructField("stop_name", StringType(), True),
    StructField("stop_desc", StringType(), True),
    StructField("stop_lat", DoubleType(), True),
    StructField("stop_lon", DoubleType(), True),
    StructField("zone_id", StringType(), True),
    StructField("stop_url", StringType(), True),
    StructField("location_type", IntegerType(), True),
    StructField("parent_station", StringType(), True),
    StructField("stop_timezone", StringType(), True),
    StructField("wheelchair_boarding", IntegerType(), True)
])

calendar_schema = StructType([
    StructField("service_id", StringType(), True),
    StructField("monday", IntegerType(), True),
    StructField("tuesday", IntegerType(), True),
    StructField("wednesday", IntegerType(), True),
    StructField("thursday", IntegerType(), True),
    StructField("friday", IntegerType(), True),
    StructField("saturday", IntegerType(), True),
    StructField("sunday", IntegerType(), True),
    StructField("start_date", StringType(), True),
    StructField("end_date", StringType(), True)
])

# ----------------------
# Write schemas to files
# ----------------------

schemas = {
    "routes": routes_schema,
    "trips": trips_schema,
    "stops": stops_schema,
    "calendar": calendar_schema
}

output_dir = "./schemas"
os.makedirs(output_dir, exist_ok=True)

for name, schema in schemas.items():
    with open(f"{output_dir}/{name}_schema.json", "w") as f:
        f.write(schema.json())