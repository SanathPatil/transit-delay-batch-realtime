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
  +-----------------+      +-----------------+       +-----------------+       +-----------------+
|    DimRoutes    |      |    DimTrips     |       |    DimStops     |       |   DimCalendar   |
|-----------------|      |-----------------|       |-----------------|       |-----------------|
| route_id (PK)   |<-----| trip_id (PK)    |       | stop_id (PK)    |       | service_id (PK) |
| route_short_name|      | route_id (FK)   |       | stop_name       |       | monday          |
| route_long_name |      | direction_id    |       | stop_lat        |       | tuesday         |
| route_type      |      | trip_headsign   |       | stop_lon        |       | ...             |
+-----------------+      +-----------------+       +-----------------+       | start_date      |
                                                                              | end_date        |
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

