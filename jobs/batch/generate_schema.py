import json
import os
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# ----------------------
# Define GTFS Schemas
# ----------------------

routes_schema = StructType([
    StructField("route_id", StringType(), False),
    StructField("agency_id", StringType(), True),
    StructField("route_short_name", StringType(), True),
    StructField("route_long_name", StringType(), True),
    StructField("route_desc", StringType(), True),
    StructField("route_type", IntegerType(), False),
    StructField("route_url", StringType(), True),
    StructField("route_color", StringType(), True),
    StructField("route_text_color", StringType(), True)
])

trips_schema = StructType([
    StructField("route_id", StringType(), False),
    StructField("service_id", StringType(), False),
    StructField("trip_id", StringType(), False),
    StructField("trip_headsign", StringType(), True),
    StructField("trip_short_name", StringType(), True),
    StructField("direction_id", IntegerType(), False),
    StructField("block_id", StringType(), True),
    StructField("shape_id", StringType(), True),
    StructField("wheelchair_accessible", IntegerType(), True),
    StructField("bikes_allowed", IntegerType(), True)
])

stops_schema = StructType([
    StructField("stop_id", StringType(), False),
    StructField("stop_code", StringType(), True),
    StructField("stop_name", StringType(), False),
    StructField("stop_desc", StringType(), True),
    StructField("stop_lat", DoubleType(), False),
    StructField("stop_lon", DoubleType(), False),
    StructField("zone_id", StringType(), True),
    StructField("stop_url", StringType(), True),
    StructField("location_type", IntegerType(), True),
    StructField("parent_station", StringType(), True),
    StructField("stop_timezone", StringType(), True),
    StructField("wheelchair_boarding", IntegerType(), True)
])

calendar_schema = StructType([
    StructField("service_id", StringType(), False),
    StructField("monday", IntegerType(), True),
    StructField("tuesday", IntegerType(), True),
    StructField("wednesday", IntegerType(), True),
    StructField("thursday", IntegerType(), True),
    StructField("friday", IntegerType(), True),
    StructField("saturday", IntegerType(), True),
    StructField("sunday", IntegerType(), True),
    StructField("start_date", StringType(), False),
    StructField("end_date", StringType(), False)
])

stop_times_schema = StructType([
    StructField("trip_id", StringType(), False),
    StructField("arrival_time", StringType(), True),  # HH:MM:SS format
    StructField("departure_time", StringType(), True), # HH:MM:SS format
    StructField("stop_id", StringType(), False),
    StructField("stop_sequence", IntegerType(), False),
    StructField("stop_headsign", StringType(), True),
    StructField("pickup_type", IntegerType(), True),
    StructField("drop_off_type", IntegerType(), True),
    StructField("shape_dist_traveled", StringType(), True),
    StructField("timepoint", IntegerType(), True)
])

# ----------------------
# Write schemas to files
# ----------------------

schemas = {
    "routes": routes_schema,
    "trips": trips_schema,
    "stops": stops_schema,
    "calendar": calendar_schema,
    "stop_times": stop_times_schema

}

output_dir = "./schemas"
os.makedirs(output_dir, exist_ok=True)

for name, schema in schemas.items():
    with open(f"{output_dir}/{name}_schema.json", "w") as f:
        f.write(schema.json())