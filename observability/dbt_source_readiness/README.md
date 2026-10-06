# CLD and Airflow source signals

A quiet upstream cycle can succeed without changing rows or creating a new
snapshot, so snapshot age is not a reliable processing watermark. Two independent
tests expose the signals needed to evaluate the source without coupling Airflow
to the catalog-linked database (CLD).

| Test | Question | Pass condition |
|---|---|---|
| `cld_table_refresh_health` | Can Snowflake continue following this table? | The table exists, auto-refresh is `RUNNING`, and the pending snapshot count is within the configured limit |
| `airflow_task_freshness` | Did the mapped table-level task complete recently? | The latest task state is `success` and that success is within the configured age |

Neither test reads or interprets the other. Attach both to a quiet source, retain
their results independently, and derive states such as `READY_NO_CHANGE` in the
observability model or orchestrator. The CLD test returns snapshot timestamps as
evidence but deliberately does not grade their age.

## Prerequisites

1. dbt runs against Snowflake and can execute `SHOW ICEBERG TABLES` for each
   monitored source.
2. Airflow task-instance history is exported to a relation dbt can read.
3. The export contains `dag_id`, `task_id`, `state`, `start_date`, and
   `try_number`.
4. Every quiet table maps to a task whose `success` state means that table was
   evaluated for the expected cycle. A broad DAG success is not sufficient.

## Install

1. Copy [`cld_table_refresh_health.sql`](tests/generic/cld_table_refresh_health.sql)
   and [`airflow_task_freshness.sql`](tests/generic/airflow_task_freshness.sql)
   into the dbt project's `tests/generic/` directory.
2. Adapt [`example_sources.yml`](models/example_sources.yml). Pass the Airflow
   history relation, table-specific task identifier, allowed DAG identifiers,
   success-age threshold, and allowed CLD backlog explicitly.
3. Run and persist failures:

```bash
dbt parse
dbt test --select test_name:cld_table_refresh_health --store-failures
dbt test --select test_name:airflow_task_freshness --store-failures
```

Each test returns no rows when its own condition passes. CLD failures include
`CLD_REFRESH_PENDING` and `CLD_REFRESH_STOPPED`. Airflow failures include
`AIRFLOW_TASK_FAILED`, `NO_AIRFLOW_RUN_IN_LOOKBACK`, and
`STALE_AIRFLOW_SUCCESS`. No failure row mixes the two signal families.

## Operational use

Run both tests before downstream source-dependent models. Retry a transient
`CLD_REFRESH_PENDING` result within the delivery window. Persist the results and
combine them only in the state and alerting layer, where transition-based logic
can produce one notification per incident.

This is a bridge for sources that do not publish a processing watermark. The
stronger contract is one completion row per table and business date, including
zero records written and the Airflow data interval or run identifier. That
removes the time-window inference and distinguishes “processed with no change”
from “the wrong task happened to succeed recently.”
