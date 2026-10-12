# Recover a stopped Iceberg table auto-refresh

A catalog-linked database (CLD) can be healthy and the upstream Airflow task can
succeed while one table's auto-refresh pipe is stopped. Diagnose the CLD and the
table separately, preserve before-state evidence, reset only a stopped table,
and prove both metadata and row recovery afterward.

## Prerequisites

- Replace `<database>.<schema>.<table>` with the fully qualified Iceberg table.
- Use a role with `MONITOR` on the CLD for `SYSTEM$CATALOG_LINK_STATUS` and
  `OWNERSHIP` on the Iceberg table for `SYSTEM$AUTO_REFRESH_STATUS` and `ALTER`.
- Choose a stable business key and the row columns that prove which source file
  and version Snowflake currently exposes.
- Run the recovery statements outside a transaction.

Set the owner role before the warehouse so the active role is proven to have
warehouse access:

```sql
USE ROLE <table_owner_role>;

USE WAREHOUSE <warehouse>;
```

## 1. Inventory Iceberg table status

Start broad. The `auto_refresh_status` column exposes the same payload as
`SYSTEM$AUTO_REFRESH_STATUS` and can reveal other affected tables.

```sql
SHOW ICEBERG TABLES IN DATABASE <database>;
```

Relevant output:

| `database_name` | `schema_name` | `name` | `auto_refresh_status` |
|---|---|---|---|
| `<database>` | `<schema>` | `<table>` | JSON containing the execution state, error, timestamps, and pending snapshots |

## 2. Parse the exact table status

This is the working query for a single table. It turns the status JSON into a
row that can be saved with the incident or compared before and after recovery.

```sql
SELECT
    fqn,
    PARSE_JSON(status_json):executionState::STRING AS execution_state,
    NULLIF(
        TRIM(
            PARSE_JSON(status_json):invalidExecutionStateReason::STRING
        ),
        ''
    ) AS stop_reason,
    PARSE_JSON(status_json):lastSnapshotTime::STRING AS last_snapshot_time,
    PARSE_JSON(status_json):lastUpdatedTime::STRING AS last_updated_time,
    PARSE_JSON(status_json):pendingSnapshotCount::INT AS pending_snapshots
FROM (
    SELECT
        '<database>.<schema>.<table>' AS fqn,
        SYSTEM$AUTO_REFRESH_STATUS(
            '<database>.<schema>.<table>'
        ) AS status_json
) AS status
ORDER BY
    fqn;
```

| `fqn` | `execution_state` | `stop_reason` | `last_snapshot_time` | `last_updated_time` | `pending_snapshots` |
|---|---|---|---|---|---:|
| `<database>.<schema>.<table>` | `GENERALIZED_PIPE_STOPPED` | `EP file commit failure` | `<external snapshot time>` | `<Snowflake refresh time>` | `0` |

Interpret the fields before acting:

| Field | Meaning |
|---|---|
| `execution_state` | `RUNNING` is healthy; any other value fails the readiness test, including `STALLED`, `STOPPED`, or a more specific stopped state |
| `stop_reason` | The table-level failure that the CLD and Airflow checks can miss |
| `last_snapshot_time` | When the external catalog created the current snapshot |
| `last_updated_time` | When Snowflake last processed that snapshot |
| `pending_snapshots` | Snapshots still queued for refresh |

Use the raw query when the incident also needs `currentSnapshotId`,
`currentMetadataFile`, or `currentSnapshotSummary`:

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS(
    '<database>.<schema>.<table>'
) AS status_json;
```

| `status_json` |
|---|
| JSON containing the complete table auto-refresh state |

## 3. Check the CLD independently

```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('<database>') AS link_status_json;
```

| JSON field | Pass condition |
|---|---|
| `executionState` | `RUNNING` |
| `failureDetails` | Empty |
| `lastLinkAttemptStartTime` | Consistent with the configured discovery interval |

Resolve catalog connection or discovery failures before resetting the table. If
discovery was suspended, resume it only after correcting the cause:

```sql
ALTER DATABASE <database>
RESUME DISCOVERY;
```

## 4. Inspect snapshot refresh history

```sql
SELECT
    refreshed_on,
    metadata_file_name,
    snapshot_id,
    is_current_snapshot
FROM TABLE(
    SNOWFLAKE.INFORMATION_SCHEMA.ICEBERG_TABLE_SNAPSHOT_REFRESH_HISTORY(
        TABLE_NAME => '<database>.<schema>.<table>'
    )
)
ORDER BY
    refreshed_on DESC
LIMIT 5;
```

| `refreshed_on` | `metadata_file_name` | `snapshot_id` | `is_current_snapshot` |
|---|---|---|---|
| `<refresh time>` | `<metadata file path>` | `<snapshot ID>` | `TRUE` for the snapshot Snowflake currently exposes |

Save the newest row. Recovery should produce or select a newer current snapshot
when the external catalog has advanced.

## 5. Capture a row before recovery

Adapt the selected columns to the table. Keep the business key, source version,
update timestamp, and source-file metadata together.

```sql
SELECT
    '<change-ticket>_before' AS qa_tag,
    record_id,
    record_uuid,
    version,
    updated_at,
    _load_metadata:source_file_name::STRING AS source_file_name
FROM <database>.<schema>.<table>
WHERE record_id = <known_record_id>;
```

| `qa_tag` | `record_id` | `record_uuid` | `version` | `updated_at` | `source_file_name` |
|---|---|---|---:|---|---|
| `<change-ticket>_before` | `<known ID>` | `<UUID>` | `<version>` | `<timestamp>` | `<source file>` |

## 6. Reset, refresh, and restore auto-refresh

Run the statements in order:

```sql
ALTER ICEBERG TABLE <database>.<schema>.<table>
SET AUTO_REFRESH = FALSE;

ALTER ICEBERG TABLE <database>.<schema>.<table>
REFRESH;

ALTER ICEBERG TABLE <database>.<schema>.<table>
SET AUTO_REFRESH = TRUE;
```

| State | What changed |
|---|---|
| Before | The table can remain on an older metadata file even when the CLD link is healthy |
| `AUTO_REFRESH = FALSE` | Stops the broken automatic pipe before manual recovery |
| `REFRESH` | Synchronizes Snowflake to the latest reachable external metadata |
| `AUTO_REFRESH = TRUE` | Creates or restores the automatic refresh path |

## 7. Prove table recovery

Run the parsed status query from step 2 again. Do not stop at `RUNNING` alone.
The complete pass condition is:

| Check | Pass condition |
|---|---|
| `execution_state` | `RUNNING` |
| `stop_reason` | `NULL` |
| `pending_snapshots` | At or below the accepted threshold and decreasing if initially queued |
| `last_updated_time` | Advances when Snowflake processes the recovered snapshot |

Run the history query from step 4 again and confirm the current metadata file and
snapshot. Then repeat the row query with an `after` tag:

```sql
SELECT
    '<change-ticket>_after' AS qa_tag,
    record_id,
    record_uuid,
    version,
    updated_at,
    _load_metadata:source_file_name::STRING AS source_file_name
FROM <database>.<schema>.<table>
WHERE record_id = <known_record_id>;
```

| `qa_tag` | `record_id` | `record_uuid` | `version` | `updated_at` | `source_file_name` |
|---|---|---|---:|---|---|
| `<change-ticket>_after` | `<same ID>` | `<same logical row>` | `<expected version>` | `<expected timestamp>` | `<new source file when applicable>` |

## 8. Verify the downstream relation or share

The table can be recovered while a downstream model or shared relation still
serves the old row. Query the same business key from that consumer path:

```sql
SELECT
    '<change-ticket>_consumer' AS qa_tag,
    record_id,
    record_uuid,
    version,
    updated_at,
    meta_load_ts
FROM <consumer_database>.<consumer_schema>.<consumer_relation>
WHERE record_id = <known_record_id>;
```

| `qa_tag` | `record_id` | `record_uuid` | `version` | `updated_at` | `meta_load_ts` |
|---|---|---|---:|---|---|
| `<change-ticket>_consumer` | `<same ID>` | `<same logical row>` | `<expected version>` | `<expected timestamp>` | `<consumer load time>` |

Recovery is complete only when the table status, refresh history, Iceberg row,
and required downstream relation agree.
