# Transit Delay Analytics: Batch & Real-Time Data Pipeline

## Project Overview

This project demonstrates an **end-to-end data engineering pipeline** for analyzing transit delays using **both batch and streaming data processing**.

**Key Features:**

- Real-time ingestion of transit data using **Kafka**.
- Historical data storage and time-series analysis with **TimescaleDB**.
- Visualization via **Streamlit dashboard**.
- Fully containerized with **Docker Compose** for reproducible environments.

---

## Docker Compose Overview

This project is fully containerized using **Docker Compose**, allowing seamless orchestration of batch and streaming services.

### Key Services

- **TimescaleDB** – Stores both GTFS static (batch) and real-time delay (streaming) data.
- **Apache Airflow** – Orchestrates the batch ETL job that loads GTFS static data.
- **Apache Kafka + Zookeeper** – Handles real-time ingestion of GTFS-RT trip updates.
- **Kafka Producer** – Fetches and publishes MBTA GTFS-RT data to Kafka topic.
- **Kafka Consumer** – Consumes real-time trip updates, joins with schedule data, and calculates delays.
- **Streamlit Dashboard** – Visualizes per-trip delay information.
- *(Optional)*: Includes commented-out **Flink** containers for future/experimental use.

### Network Configuration

Docker Compose is configured to use an **external Docker network**:

```yaml
networks:
  default:
    name: kafka-flink-network
    external: true
```
Note:
This project uses a .env file to configure credentials and service endpoints.
**Important:** This file is intentionally not committed to Git (included in .gitignore) for security reasons. You must create your own .env file in the project root before running the system.
```
POSTGRES_USER= {}
POSTGRES_PASSWORD= {}
POSTGRES_DB= {}
POSTGRES_HOST= timescaledb

# Kafka connection
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# GTFS feed URL (used by batch job)
GTFS_URL=https://cdn.mbta.com/MBTA_GTFS.zip
```

## Architecture

```text
               +--------------------+
               |  Static GTFS Data  |
               +--------------------+
                        |
                        v
            +------------------------+
            |   TimescaleDB (Batch)  |
            +------------------------+
                        ^
                        |
+---------+      +------------+      +-------------+
| Transit | ---> |  Kafka     | ---> |  Kafka      | ---> TimescaleDB
| Feeds   |      |  Broker    |      | Consumer    |
+---------+      +------------+      +-------------+
                        |
                        v
               +----------------+
               |  Streamlit UI  |
               +----------------+


```
---

## Batch Processing Implementation

This project is a **PySpark-based ETL pipeline** that processes [GTFS (General Transit Feed Specification)](https://gtfs.org/) static transit data, validates it, 
and writes it into a **TimescaleDB** database.

The pipeline is:

* Containerized using **Docker**
* Orchestrated via **Apache Airflow**
* Executed in a **Spark** runtime environment

---

## Key Features

- Downloads GTFS static data feed (ZIP).
- Validates schema and data types.
- Performs data quality checks:
  - Null checks.
  - Primary key uniqueness.
  - Referential integrity.
- Loads data into **TimescaleDB**.
- Orchestrated via an **Airflow DAG**.
- Fully containerized using **Docker**.

---
## Data Model Architecture

This project uses a **Snowflake schema** to model transit delay data, combining static GTFS data (batch) with real-time streaming events for comprehensive delay analytics.

---

### 1. Batch (Static) Data Schema

The batch data schema models the core static GTFS information loaded periodically (e.g., trips, stops, routes, calendar) into dimension tables, with delays stored as facts.

```plaintext
                +-------------------------+
                |   GTFS Static Feed URL  |
                | (e.g., MBTA, MTA, etc.) |
                +-----------+-------------+
                            |
                            v
             +-----------------------------+
             |      (gtfs_batch_job.py)    |
             +-----------------------------+
                            |
                            v
         +--------------------------------------+
         | Read GTFS .txt files using PySpark   |
         |  and predefined (generated) schemas  |
         +------------------+-------------------+
                            |
                            v
   +---------------------------------------------------+
   | Perform Data Validation & Quality Checks          |
   | - Required Columns                                |
   | - Null Checks                                     |
   | - Primary Key Uniqueness                          |
   | - Referential Integrity (foreign keys)            |
   +------------------+--------------------------------+
                            |
                            v
                +--------------------------+
                | Write to TimescaleDB     |
                |  - dim_routes            |
                |  - dim_trips             |
                |  - dim_stops             |
                |  - dim_calendar          |
                |  - dim_stop_times        |
                +--------------------------+
```
### Data Modelling:
```

                             +------------------+
                             |   dim_routes     |
                             |------------------|
                             | route_id (PK)    |
                             | agency_id        |
                             | route_type       |
                             +--------^---------+
                                      |
                                      |
                             +--------+---------+
                             |    dim_trips     |
                             |------------------|
                             | trip_id (PK)     |
                             | route_id (FK)    |
                             | service_id (FK)  |
                             | direction_id     |
                             | shape_id         |
                             +---^--------^-----+
                                 |        |
                   +-------------+        +-------------+
                   |                              |
         +---------+--------+           +---------+---------+
         |  dim_calendar    |           |   dim_stop_times  |
         |------------------|           |-------------------|
         | service_id (PK)  |           | trip_id (FK)      |
         | start_date       |           | stop_id (FK)      |
         | end_date         |           | stop_sequence     |
         +------------------+           +---------^---------+
                                                 |
                                       +---------+--------+
                                       |   dim_stops      |
                                       |------------------|
                                       | stop_id (PK)     |
                                       | stop_name        |
                                       | stop_lat, lon    |
                                       +------------------+


```


---

## Streaming Pipeline

This streaming component enriches real-time trip updates (from MBTA) with GTFS static data to compute delays and persist them into a fact table: `trip_delays`.


## Architecture Overview
```
+----------------+        +----------------+        +--------------------+
|   GTFS Static  |        |  Kafka Topic   |        |  TimescaleDB Tables |
|  Data Load     |------->| (trip-updates) |        |  dim_stop_times     |  <-- Batch job loads this table
|  (Batch job)   |        | (Producer)     |        |  trip_delays        |  <-- Streaming consumer upserts here
+----------------+        +----------------+        +--------------------+
                                |
                                v
                        +------------------+
                        | Kafka Consumer    | 
                        | Streaming Logic   | (Joins with dim_stop_times)
                        | Calculate delays  |
                        | Upsert trip_delays|
                        +------------------+
                                |
                                v
                        +------------+
                        | Streamlit  |  (Reads delays)
                        +------------+


```
The consumer joins GTFS-RT data with GTFS static data (loaded by the batch job):

- `dim_stop_times`: to get scheduled arrival times per trip/stop/sequence.
- `dim_trips`: to enrich with route and headsign (used in dashboard).
- `dim_routes`: (used in dashboard, not consumer directly).

---

## Components

### 1: Kafka Producer 
(`producer.py`)

- Polls MBTA TripUpdates feed every 30s
- Parses GTFS-RT protobuf
- Publishes to Kafka topic `mbta-trip-updates`


### 2: Kafka Consumer 
(`consumer.py`)

- Reads `mbta-trip-updates` topic.
- For each stop update:
  - Extracts real-time timestamp.
  - Looks up scheduled time in `dim_stop_times`.
  - Computes delay in seconds.
  - Saves (or updates) row in `trip_delays`.

Note: Handles extended GTFS times (e.g., 25:30:00) and converts properly using timezone-aware logic (America/New_York → UTC).

---

## Streamlit Dashboard

The Streamlit app provides an interactive UI to explore trip delays with route information:
- Connects to TimescaleDB using psycopg2
- Lists recent trips with route and headsign info
- Shows stop-by-stop delay details for selected trip
- Visualizes delays using line charts and data tables
- Handles DB connection errors gracefully

### How It Works
- Fetches trip options joined with route info for selection
- On trip selection, retrieves metadata and delay data
- Displays delay trends and detailed stop-level data

![Streamlit App Screenshot](/images/streamlit_graph.png)
![Streamlit App Screenshot](/images/streamlit_table.png)

### Additional Notes

- Initially, I explored implementing the streaming pipeline using Apache Flink (PyFlink and Scala Flink) for Kafka producer and consumer components.  
- Due to serialization issues related to MBTA's protobuf data and Docker container environment, I faced challenges deploying the Flink jobs reliably.  
- Despite this, the approach demonstrates my ability to work with advanced streaming tools and troubleshoot complex data pipeline issues.  
- Ultimately, I developed a robust Kafka consumer in Python that successfully handles streaming data processing and integrates with TimescaleDB.  
- The Flink-related code and experiments are preserved in the `/streaming/flink` folder for reference.