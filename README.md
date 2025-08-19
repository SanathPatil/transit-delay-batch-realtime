# Transit Delay Analytics: Batch & Real-Time Data Pipeline

## Project Overview

This project demonstrates an **end-to-end data engineering pipeline** for analyzing transit delays using **both batch and streaming data processing**. It showcases modern data engineering practices and tools for professional evaluation.

**Key Features:**

- Real-time ingestion of transit data using **Kafka**.
- Streaming processing and analytics with **Flink**.
- Historical data storage and time-series analysis with **TimescaleDB**.
- Visualization via **pgAdmin** (and optional Streamlit dashboard).
- Fully containerized with **Docker Compose** for reproducible environments.

---

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
| Transit | ---> |  Kafka     | ---> |  Flink      | ---> TimescaleDB
| Feeds   |      |  Broker    |      | (Streaming) |
+---------+      +------------+      +-------------+
                        |
                        v
               +----------------+
               |  Streamlit UI  |
               +----------------+


```
- **Kafka**: Handles streaming transit data ingestion.
- **Flink**: Joins streaming data with static schedule data in TimescaleDB to calculate delays.
- **TimescaleDB**: Stores historical and real-time enriched transit data.
- **pgAdmin**: GUI to explore TimescaleDB.

---

## Batch Processing Implementation

- Batch processing is implemented using **Apache Spark (PySpark)**.
- The batch job downloads and extracts the latest static GTFS feed from the MTA, processes `trips.txt`, and writes the cleaned data into TimescaleDB.
- The Spark job is containerized for easy deployment and reproducibility.

---
## Data Model Architecture

This project uses a **Snowflake schema** to model transit delay data, combining static GTFS data (batch) with real-time streaming events for comprehensive delay analytics.

---

### 1. Batch (Static) Data Schema

The batch data schema models the core static GTFS information loaded periodically (e.g., trips, stops, routes, calendar) into dimension tables, with delays stored as facts.

```plaintext
+-----------------+      +-----------------+       +-----------------+       +-----------------+        +----------------------+
|   DimRoutes     |      |   DimTrips      |       |   DimStops      |       |  DimCalendar     |       |   DimStopTimes       |
|-----------------|      |-----------------|       |-----------------|       |------------------|       |----------------------|
| route_id (PK)   |<-----| trip_id (PK)    |       | stop_id (PK)    |       | service_id (PK)  |       | trip_id (FK)         |
| agency_id       |      | route_id (FK)   |       | stop_name       |       | start_date       |       | stop_id (FK)         |
| route_long_name |      | service_id      |       | stop_lat        |       | end_date         |       | arrival_time         |
| route_type      |      | trip_headsign   |       | stop_lon        |       | days_of_week     |       | departure_time       |
+-----------------+      | direction_id    |       +-----------------+       +------------------+       | stop_sequence        |
                         | block_id        |                                                            +----------------------+
                         | shape_id        |
                         +-----------------+


```


---

### 2. Streaming Schema:
```
                                  +-----------------+
                                  |    DimRoutes    |
                                  +-----------------+
                                          ^
                                          |
                                  +-----------------+
                                  |    DimTrips     |
                                  +-----------------+
                                          ^
                                          |
                                  +-----------------+         +-----------------+
                                  |   DimCalendar   |<--------| StreamingEvents |
                                  +-----------------+         |-----------------|
                                                             | event_id (PK)   |
                                                             | trip_id (FK)    |
                                                             | stop_id (FK)    |
                                                             | event_time      |
                                                             | actual_arrival  |
                                                             | delay_seconds   |
                                                             | delay_status    |
                                                             +-----------------+
                                          ^                           |
                                          |                           v
                  +-----------------+    +-----------------+     +--------------+
                  |    DimStops     |    |   FactDelays    |<----| EnrichedStream|
                  +-----------------+    +-----------------+     +--------------+
```

## Data Pipeline: Batch vs Streaming

### Batch Pipeline

The batch pipeline loads **only dimension tables** that represent the static GTFS schedule data:

- `DimRoutes`
- `DimTrips`
- `DimStops`
- `DimCalendar`
- `DimStopTimes`

These tables provide the foundational static reference data required for enriching real-time events.

---

### Streaming Pipeline

The streaming pipeline ingests live transit events (`StreamingEvents`) and enriches them by joining with the static dimension tables:

- `DimTrips`
- `DimCalendar`
- `DimRoutes`
- `DimStops`

This enrichment allows the pipeline to:

- **Validate service days** using `DimCalendar`
- **Add contextual information** about routes and stops
- **Calculate delay metrics** (e.g., difference between scheduled and actual arrival times)

Optionally, the enriched streaming events are stored temporarily in an intermediate table called `EnrichedStream`.

---

### Fact Table: FactDelays

The `FactDelays` table stores the **computed delay events** derived from streaming data and serves as the main fact table for delay analysis.

This architecture cleanly separates static schedule data (batch) from real-time delay information (streaming), enabling comprehensive and scalable transit delay analytics.



# TO DO:
🚀 Real-Time Streaming with Kafka + Flink — Step-by-Step

1. Set Up Kafka Topics

Create Kafka topics for each MBTA Protobuf feed you want to consume, e.g.:

vehicle_positions

trip_updates

alerts (optional)

docker exec -it broker kafka-topics --create --topic vehicle_positions --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
docker exec -it broker kafka-topics --create --topic trip_updates --bootstrap-server broker:29092 --partitions 1 --replication-factor 1
Verify:
docker exec -it broker kafka-topics --list --bootstrap-server broker:29092
__consumer_offsets
_schemas
trip_updates
vehicle_positions
# Repeat for other topics as needed

2. Build Kafka Producer for MBTA API

Write a producer service (Python, Scala, or Java) that:

Polls MBTA’s Protobuf HTTP feeds (e.g., vehicle_positions.pb) every 10-30 seconds.

Parses raw Protobuf messages using MBTA’s .proto definitions.

Publishes serialized Protobuf messages to the respective Kafka topics.

Containerize this producer and add it to your Docker Compose for easy orchestration.

3. Set Up Flink Environment

Make sure your Flink cluster is running (JobManager + TaskManager).

Add required dependencies in your Scala Flink project for:

Kafka connector (flink-connector-kafka)

Protobuf libraries (protobuf-java)

TimescaleDB JDBC driver (for sinks)

4. Implement Flink Consumer in Scala

In your Flink Scala job:

Use the Kafka consumer connector to consume messages from Kafka topics.

Implement a custom DeserializationSchema to parse Protobuf messages into Scala/Java case classes or generated Protobuf classes.

class ProtobufDeserializationSchema extends DeserializationSchema[YourProtoClass] {
  override def deserialize(message: Array[Byte]): YourProtoClass = {
    YourProtoClass.parseFrom(message)
  }
  override def isEndOfStream(nextElement: YourProtoClass): Boolean = false
  override def getProducedType: TypeInformation[YourProtoClass] = createTypeInformation[YourProtoClass]
}

5. Enrich Streaming Data

Join the streaming events with your static batch data (loaded from TimescaleDB) using Flink’s broadcast state pattern.

Broadcast static datasets (e.g., DimTrips, DimRoutes) to all Flink workers.

Perform real-time enrichment to calculate delay metrics and validate service days.

6. Write Results to Sink

After enrichment and calculation, write your results to:

TimescaleDB for persistent storage.

Or optionally, to Kafka (for downstream consumers or dashboards).

Use Flink JDBC sink connector or custom sink implementations.

7. Deploy and Monitor

Build your Flink Scala job JAR.

Submit it to your Flink cluster:

./bin/flink run -c your.main.Class /path/to/your-flink-job.jar


Monitor the job through Flink Web UI (localhost:8083).

8. (Optional) Visualize

Connect your visualization tool (Streamlit, Grafana, etc.) to TimescaleDB.

Build live dashboards with maps, delay stats, and vehicle tracking.

Summary Checklist:

 Create Kafka topics

 Build & deploy Kafka producer polling MBTA Protobuf feeds

 Set up Flink Scala project with Kafka & Protobuf dependencies

 Implement Flink consumer with Protobuf deserialization

 Load & broadcast static batch data to Flink

 Enrich streaming data with batch data

 Write enriched results to TimescaleDB

 Deploy & monitor Flink streaming job

 Build visualization dashboards (optional)