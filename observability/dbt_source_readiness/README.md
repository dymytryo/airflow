# CLD, Iceberg, and Airflow source signals

A quiet upstream cycle can succeed without changing rows or creating a new
snapshot, so snapshot age is not a reliable processing watermark. Three
independent tests expose the database, table, and job signals needed to evaluate
the source.

| Test | Plane and scope | Signal read | Pass condition | Failure isolated |
|---|---|---|---|---|
| `cld_catalog_link_status` | Data plane, one catalog-linked database (CLD) | `SYSTEM$CATALOG_LINK_STATUS` for the source database | The discovery pipe is `RUNNING` and `failureDetails` is empty | The CLD cannot connect to the external catalog, discover an entity, or complete a supported link operation |
| `iceberg_auto_refresh_status` | Data plane, one externally managed Iceberg table | `SYSTEM$AUTO_REFRESH_STATUS` for the fully qualified table | The table refresh pipe is `RUNNING`, has no execution error, and has no more than the allowed pending snapshots | The table-specific refresh is unavailable, stalled, stopped, invalid, or backlogged even though the CLD can still be healthy |
| `airflow_task_freshness` | Job plane, one table-specific task across its allowed DAGs | Exported Airflow task-instance history, deduplicated to the highest `try_number` for each run | The latest task state is `success`, and its most recent successful `start_date` is within `max_success_age_hours` | No task ran in the lookback window, the latest task failed, no successful task exists, or the latest success is stale |

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

## Check one source manually

The following statements inspect the same source configured in
[`example_sources.yml`](models/example_sources.yml):

- CLD: `PAYMENTS_PROD`
- Iceberg table: `PAYMENTS_PROD.RAW.PAYMENT_EVENTS`
- Airflow history: `OBSERVABILITY_PROD.AIRFLOW.TASK_INSTANCE`
- Mapped task: `validate_payments_payment_events`
- Allowed DAGs: `replay_payments_hourly` and `replay_payments_daily`
- Thresholds: no pending snapshots, a success within 26 hours, and a 72-hour
  Airflow lookback

These diagnostic statements always return one row with `PASS` or `FAIL`. The dbt
tests use the same decisions but return no rows on a pass so dbt can grade them
as generic tests.

### 1. Check the CLD discovery pipe

This is the database-wide prerequisite for the example table. It does not prove
that the table's own auto-refresh pipe is healthy.

```sql
with catalog_link_status as (
    select
        parse_json(
            system$catalog_link_status('PAYMENTS_PROD')
        ) as link_status
),
catalog_link_signal as (
    select
        link_status:executionState::varchar as link_state,
        link_status:failedExecutionStateReason::varchar
            as failure_reason_detail,
        link_status:failedExecutionStateErrorCode::varchar
            as failure_error_code,
        try_to_timestamp_tz(
            link_status:lastLinkAttemptStartTime::varchar
        ) as last_link_attempt_at,
        coalesce(
            array_size(link_status:failureDetails),
            0
        ) as failure_detail_count,
        link_status:failureDetails as failure_details
    from catalog_link_status
),
evaluated as (
    select
        *,
        case
            when link_state is null
                then 'CLD_LINK_STATUS_UNAVAILABLE'
            when upper(link_state) != 'RUNNING'
                then concat('CLD_LINK_', upper(link_state))
            when failure_detail_count > 0
                then 'CLD_LINK_ENTITY_FAILURES'
            else null
        end as failure_reason
    from catalog_link_signal
)
select
    iff(failure_reason is null, 'PASS', 'FAIL') as check_status,
    failure_reason,
    link_state,
    failure_reason_detail,
    failure_error_code,
    last_link_attempt_at,
    failure_detail_count,
    failure_details
from evaluated;
```

| Result column | Healthy value or use |
|---|---|
| `check_status` | `PASS` |
| `failure_reason` | `NULL` |
| `link_state` | `RUNNING` |
| `failure_reason_detail`, `failure_error_code` | Diagnostic fields; normally `NULL` while the link is healthy |
| `last_link_attempt_at` | Last discovery attempt; returned for diagnosis but not used as a freshness watermark |
| `failure_detail_count` | `0` |
| `failure_details` | Entity-level failures to investigate when the count is greater than zero |

`PASS` means the CLD discovery pipe is `RUNNING` and reports no entity-level
failures. Continue to the table check even when this passes.

### 2. Check the table auto-refresh pipe

This checks the pipe that applies new Iceberg metadata to this table in
Snowflake. The example allows no queued snapshots.

```sql
with auto_refresh_status as (
    select
        parse_json(
            system$auto_refresh_status(
                '"PAYMENTS_PROD"."RAW"."PAYMENT_EVENTS"'
            )
        ) as refresh_status
),
auto_refresh_signal as (
    select
        refresh_status:executionState::varchar as refresh_state,
        refresh_status:invalidExecutionStateReason::varchar as refresh_error,
        try_to_timestamp_tz(
            refresh_status:lastSnapshotTime::varchar
        ) as last_snapshot_at,
        try_to_timestamp_tz(
            refresh_status:lastUpdatedTime::varchar
        ) as last_updated_at,
        coalesce(
            try_to_number(
                refresh_status:pendingSnapshotCount::varchar
            ),
            0
        ) as pending_snapshot_count
    from auto_refresh_status
),
evaluated as (
    select
        *,
        case
            when refresh_state is null
                then 'ICEBERG_AUTO_REFRESH_STATUS_UNAVAILABLE'
            when upper(refresh_state) != 'RUNNING'
                then concat(
                    'ICEBERG_AUTO_REFRESH_',
                    upper(refresh_state)
                )
            when nullif(trim(refresh_error), '') is not null
                then 'ICEBERG_AUTO_REFRESH_ERROR'
            when pending_snapshot_count > 0
                then 'ICEBERG_AUTO_REFRESH_PENDING'
            else null
        end as failure_reason
    from auto_refresh_signal
)
select
    iff(failure_reason is null, 'PASS', 'FAIL') as check_status,
    failure_reason,
    refresh_state,
    refresh_error,
    last_snapshot_at,
    last_updated_at,
    pending_snapshot_count
from evaluated;
```

| Result column | Healthy value or use |
|---|---|
| `check_status` | `PASS` |
| `failure_reason` | `NULL` |
| `refresh_state` | `RUNNING` |
| `refresh_error` | `NULL` or blank |
| `last_snapshot_at` | Snapshot creation time in the external catalog; diagnostic only for a quiet source |
| `last_updated_at` | Time Snowflake applied that snapshot; diagnostic only for a quiet source |
| `pending_snapshot_count` | `0` for this example |

`PASS` means the table pipe is `RUNNING`, has no error, and has applied every
queued snapshot. It does not prove the producer ran for the expected cycle.

### 3. Check the mapped Airflow task

This checks one table-specific validation task. It collapses retries first, then
evaluates the latest task across the allowed DAGs and the age of the latest
successful task.

```sql
with task_runs as (
    select
        dag_id,
        task_id,
        lower(state) as task_state,
        start_date as started_at,
        try_number,
        row_number() over (
            partition by
                dag_id,
                task_id,
                start_date
            order by
                coalesce(try_number, 0) desc
        ) as attempt_rank
    from OBSERVABILITY_PROD.AIRFLOW.TASK_INSTANCE
    where task_id = 'validate_payments_payment_events'
      and dag_id in (
          'replay_payments_hourly',
          'replay_payments_daily'
      )
      and start_date >= dateadd(
          'hour',
          -72,
          current_timestamp()
      )
),
deduped_task_runs as (
    select
        dag_id,
        task_id,
        task_state,
        started_at,
        try_number
    from task_runs
    where attempt_rank = 1
),
latest_task as (
    select
        dag_id,
        task_id,
        task_state,
        started_at,
        try_number
    from deduped_task_runs
    qualify row_number() over (
        order by
            started_at desc,
            coalesce(try_number, 0) desc
    ) = 1
),
task_summary as (
    select
        count(*) as run_count,
        max(
            case
                when task_state = 'success'
                    then started_at
            end
        ) as last_success_at
    from deduped_task_runs
),
airflow_signal as (
    select
        summary.run_count,
        summary.last_success_at,
        latest.dag_id,
        latest.task_id,
        latest.task_state,
        latest.started_at as latest_started_at,
        datediff(
            'minute',
            summary.last_success_at,
            current_timestamp()
        ) / 60.0 as success_age_hours
    from task_summary as summary
    left join latest_task as latest
        on true
),
evaluated as (
    select
        *,
        case
            when run_count = 0
                then 'NO_AIRFLOW_RUN_IN_LOOKBACK'
            when task_state != 'success'
                then concat(
                    'AIRFLOW_TASK_',
                    upper(coalesce(task_state, 'UNKNOWN'))
                )
            when last_success_at is null
                then 'NO_AIRFLOW_SUCCESS_IN_LOOKBACK'
            when success_age_hours > 26
                then 'STALE_AIRFLOW_SUCCESS'
            else null
        end as failure_reason
    from airflow_signal
)
select
    iff(failure_reason is null, 'PASS', 'FAIL') as check_status,
    failure_reason,
    dag_id,
    task_id,
    task_state,
    latest_started_at,
    last_success_at,
    success_age_hours
from evaluated;
```

| Result column | Healthy value or use |
|---|---|
| `check_status` | `PASS` |
| `failure_reason` | `NULL` |
| `dag_id` | The allowed DAG containing the latest mapped task; both DAGs do not have to run |
| `task_id` | `validate_payments_payment_events` |
| `task_state` | `success` |
| `latest_started_at` | Start time of the latest deduplicated task |
| `last_success_at` | Start time of the latest successful task |
| `success_age_hours` | At most `26` |

`PASS` means the latest mapped task succeeded and a successful task started no
more than 26 hours ago. It does not prove that Snowflake discovered or refreshed
the corresponding table.

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
