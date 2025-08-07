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
