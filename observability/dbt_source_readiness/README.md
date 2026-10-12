# CLD, Iceberg, and Airflow source signals

A quiet upstream cycle can succeed without changing rows or creating a new
snapshot, so snapshot age is not a reliable processing watermark. Three
independent tests expose the database, table, and job signals needed to evaluate
the source.

| Test | Question | Pass condition |
|---|---|---|
| `cld_catalog_link_status` | Can Snowflake discover and link objects from the external catalog? | The database link is `RUNNING` and has no entity failures |
| `iceberg_auto_refresh_status` | Can Snowflake continue refreshing this table? | The table's auto-refresh pipe is `RUNNING`, has no error, and its pending snapshot count is within the configured limit |
| `airflow_task_freshness` | Did the mapped table-level task complete recently? | The latest task state is `success` and that success is within the configured age |

No test reads or interprets another. A healthy catalog link does not prove that
one table's auto-refresh pipe is running, and Airflow success does not prove that
Snowflake applied the latest snapshot. Attach all three to a quiet source and
retain their results independently.

## Prerequisites

1. The dbt execution role has `MONITOR` on the catalog-linked database and
   `OWNERSHIP` on each table checked with `SYSTEM$AUTO_REFRESH_STATUS`.
2. Airflow task-instance history is exported to a relation dbt can read.
3. The export contains `dag_id`, `task_id`, `state`, `start_date`, and
   `try_number`.
4. Every quiet table maps to a task whose `success` state means that table was
   evaluated for the expected cycle. A broad DAG success is not sufficient.

## Install

1. Copy [`cld_catalog_link_status.sql`](tests/generic/cld_catalog_link_status.sql),
   [`iceberg_auto_refresh_status.sql`](tests/generic/iceberg_auto_refresh_status.sql),
   and [`airflow_task_freshness.sql`](tests/generic/airflow_task_freshness.sql)
   into the dbt project's `tests/generic/` directory.
2. Adapt [`example_sources.yml`](models/example_sources.yml). Pass the Airflow
   history relation, table-specific task identifier, allowed DAG identifiers,
   success-age threshold, and allowed Iceberg snapshot backlog explicitly.
3. Run and persist failures:

```bash
dbt parse
dbt test --select test_name:cld_catalog_link_status --store-failures
dbt test --select test_name:iceberg_auto_refresh_status --store-failures
dbt test --select test_name:airflow_task_freshness --store-failures
```

Each test returns no rows when its own condition passes. Failures include
`CLD_LINK_ENTITY_FAILURES`, `ICEBERG_AUTO_REFRESH_STOPPED`,
`ICEBERG_AUTO_REFRESH_PENDING`, `AIRFLOW_TASK_FAILED`, and
`STALE_AIRFLOW_SUCCESS`. No failure row mixes signal families.

## Operational use

Run all three tests before downstream source-dependent models. Retry a transient
`ICEBERG_AUTO_REFRESH_PENDING` result within the delivery window. For a stopped
table-level pipe, follow
[`iceberg_auto_refresh_recovery.md`](iceberg_auto_refresh_recovery.md). Persist
results and combine them only in the state and alerting layer.

This is a bridge for sources that do not publish a processing watermark. The
stronger contract is one completion row per table and business date, including
zero records written and the Airflow data interval or run identifier. That
removes the time-window inference and distinguishes “processed with no change”
from “the wrong task happened to succeed recently.”
