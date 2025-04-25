# Airflow DagRun Export DAG

A DAG that extracts Airflow `DagRun` metadata and writes it to Redshift so historical run logs are easily queryable and analyzable in the data lake.

---

## Table of Contents

1. [Background](#background)
2. [Project Overview](#project-overview)
3. [Architecture & Workflow](#architecture--workflow)
4. [Configuration](#configuration)
5. [Key Components](#key-components)
6. [Usage](#usage)
7. [Extensibility & Future Enhancements](#extensibility--future-enhancements)

---

## Background

Airflow's native metastore can be difficult to query for historical `DagRun` logs.  
This DAG exports the last **5 days** of `DagRun` metadata into Redshift tables in the Data Lake, enabling:

- Easy access to execution history  
- Custom analytics and SLA monitoring  
- Long-term storage without Airflow schema constraints  

---

## Project Overview

The **Airflow DagRun Export** DAG runs daily to:

1. Fetch recent `DagRun` records from Airflow's metastore (past 5 days).  
2. Convert the records into a Pandas DataFrame.  
3. Truncate existing rows in Redshift for the latest execution date.  
4. Insert new metadata into a target table (e.g., `elementary.airflow_dagrun_metadata`).  

---

## Architecture & Workflow

```mermaid
flowchart LR
    A[Schedule: 21:00 UTC Every Day] --> B[Task: export_and_write_dagrun_metadata]
    B --> C[_export_dagrun_data()]
    C --> D[_write_to_redshift(df, table_name)]
    D --> E[elementary.airflow_dagrun_metadata updated]
```

1. **Trigger**: Cron at `0 21 * * *` (3 PM CST).  
2. **Extraction**: `_export_dagrun_data` uses SQLAlchemy session to fetch `DagRun` rows.  
3. **Transformation**: `_convert_to_dataframe` and column filtering.  
4. **Loading**: `_write_to_redshift` upserts by execution date into Redshift.

---

## Configuration

Ensure the following **Airflow Variables** are set:

| Variable               | Description                       |
|------------------------|-----------------------------------|
| `REDSHIFT_HOST`        | Redshift cluster endpoint         |
| `REDSHIFT_DBNAME`      | Database name                     |
| `REDSHIFT_USER`        | Username                          |
| `REDSHIFT_PASSWORD`    | Password                          |

Constants:

- **`MAX_AGE_IN_DAYS`**: Number of days of history to fetch (default: 5).  
- **`default_args['retries']`**: 1 retry with a 90-minute delay.  

---

## Key Components

- **`_export_dagrun_data`**: Orchestrates fetching and conversion to DataFrame.  
- **`_fetch_dagrun_records`**: Queries Airflow metastore for recent runs.  
- **`_convert_to_dataframe`**: Normalizes and filters the metadata.  
- **`_write_to_redshift`**: Upserts DataFrame rows into Redshift table, handling table creation and deletion of stale rows.  
- **Airflow DAG**: Decorated with `@dag`, single task `export_and_write_dagrun_metadata`.  

---

## Usage

1. **Deploy** this DAG file into your Airflow `dags/` directory.  
2. **Set** the required Airflow Variables for Redshift.  
3. **Verify** schedule: `0 21 * * *`, starting `2024-09-01`, no catchup.  
4. **Monitor** in Airflow UI for executions and Slack alerts on failure/success.  
5. **Query** the `elementary.airflow_dagrun_metadata` table in Redshift for logs.

---

## Extensibility & Future Enhancements

- Increase `MAX_AGE_IN_DAYS` for longer history.  
- Partition Redshift table by date for performance.  
- Add additional metadata columns (e.g., task duration).  
- Integrate with BI tools for SLA dashboards.  
- Automate schema evolution on new Airflow versions.
