# Weather ETL Incremental Pipeline

## Overview
This project implements an incremental ETL pipeline using Apache Airflow, Snowflake, and the Open-Meteo API. The pipeline ingests daily historical weather data for a specified location, performs idempotent incremental updates, and loads the processed data into a Snowflake table for analytical use.

The workflow is designed to:
- Fetch one day of weather data per DAG run using Airflow’s `logical_date`
- Avoid duplicate records through delete-and-insert incremental logic
- Stage and load data into Snowflake using temporary stages and `COPY INTO`
- Maintain modular, production-style ETL code

---

## Architecture

### Pipeline Steps

1. **Extract**
   - Determines the execution date using `get_logical_date()`
   - Fetches weather data for a 24-hour window from the Open-Meteo API
   - Saves the extracted data as a CSV file in `/tmp`

2. **Load**
   - Connects to Snowflake using Airflow’s Snowflake connection
   - Ensures the target table exists
   - Deletes existing rows for the execution date (incremental update)
   - Loads the CSV into Snowflake via a temporary stage and `COPY INTO`

### Technologies Used
- Apache Airflow
- Snowflake
- Python
- Open-Meteo Weather API
- Docker

## Incremental Logic
The DAG uses Airflow’s `logical_date` to determine the date to process.

For each DAG run:
- `start_date = logical_date`
- `end_date = logical_date + 1 day`

Before loading, the pipeline executes:

    DELETE FROM <table>
    WHERE date >= <logical_date>
    AND date < <logical_date + 1 day>;





---
