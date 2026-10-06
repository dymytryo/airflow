# CLD and Airflow source readiness

A source-readiness check determines whether a catalog-linked Iceberg table is
safe for downstream work when snapshot time is not a reliable processing
watermark. A quiet upstream cycle can succeed without changing rows or creating
a new snapshot, so snapshot age alone produces a false stale result.

The check keeps the catalog-linked database (CLD) and Airflow signals separate,
then applies one explicit readiness rule:

| CLD table refresh | Snapshot | Mapped Airflow task | Result |
|---|---|---|---|
| Not `RUNNING`, or snapshots queued | Any | Any | Not ready |
| `RUNNING` | Recent | Any | Ready from data change |
| `RUNNING` | Old or absent | Recent `success` | Ready with no change |
| `RUNNING` | Old or absent | Missing, stale, or not successful | Not ready |

Airflow success is accepted only for the exact table-level validation task. It
proves that the table was processed, not that rows changed. It never overrides a
stopped, stalled, or backlogged CLD refresh.

## Prerequisites

1. dbt runs against Snowflake and can execute `SHOW ICEBERG TABLES` for each
   monitored source.
2. Airflow task-instance history is exported to a relation dbt can read.
3. The export contains `dag_id`, `task_id`, `state`, `start_date`, and
   `try_number`.
4. Every quiet table maps to a task whose `success` state means that table was
   evaluated for the expected cycle. A broad DAG success is not sufficient.

## Install

1. Copy
   [`cld_airflow_source_readiness.sql`](tests/generic/cld_airflow_source_readiness.sql)
   into the dbt project's `tests/generic/` directory.
2. Adapt [`example_sources.yml`](models/example_sources.yml). Pass the Airflow
   history relation, table-specific task identifier, allowed DAG identifiers,
   and the two age thresholds explicitly.
3. Run and persist failures:

```bash
dbt parse
dbt test --select test_name:cld_airflow_source_readiness --store-failures
```

The test returns no rows when the source is ready. A failure row carries both
signals and one reason, including `CLD_REFRESH_PENDING`,
`AIRFLOW_TASK_FAILED`, `NO_AIRFLOW_RUN_IN_LOOKBACK`, and
`STALE_AIRFLOW_SUCCESS`.

## Operational use

Run this check before downstream source-dependent models. Retry a transient
`CLD_REFRESH_PENDING` result within the delivery window; alert only when the
state crosses the configured deadline. Persist the failure rows and alert on
state transitions so one incident produces one notification.

This is a bridge for sources that do not publish a processing watermark. The
stronger contract is one completion row per table and business date, including
zero records written and the Airflow data interval or run identifier. That
removes the time-window inference and distinguishes “processed with no change”
from “the wrong task happened to succeed recently.”
