# Recover a stopped Iceberg table auto-refresh

## Prerequisites

- Use the role that owns the Iceberg table. `SYSTEM$AUTO_REFRESH_STATUS` and the
  recovery commands require `OWNERSHIP`.
- Replace `<database>.<schema>.<table>` with the fully qualified table name.

## 1. Confirm the table-level failure

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS(
    '<database>.<schema>.<table>'
);
```

| `executionState` | Meaning |
|---|---|
| `RUNNING` | The auto-refresh pipe is operating normally |
| `STALLED` | Snowflake encountered an error and is retrying |
| `STOPPED` | The pipe cannot recover without intervention |
| `ICEBERG_TABLE_NOT_INITIALIZED` | Snowflake could not initialize the linked table |

Record `invalidExecutionStateReason`, `pendingSnapshotCount`,
`lastSnapshotTime`, and `lastUpdatedTime` before changing the table.

## 2. Reset, refresh, and restore auto-refresh

Run the statements in order and outside a transaction:

```sql
ALTER ICEBERG TABLE <database>.<schema>.<table>
SET AUTO_REFRESH = FALSE;

ALTER ICEBERG TABLE <database>.<schema>.<table>
REFRESH;

ALTER ICEBERG TABLE <database>.<schema>.<table>
SET AUTO_REFRESH = TRUE;
```

## 3. Verify recovery

```sql
SELECT SYSTEM$AUTO_REFRESH_STATUS(
    '<database>.<schema>.<table>'
);
```

Pass when `executionState` is `RUNNING`, `invalidExecutionStateReason` is empty,
and `pendingSnapshotCount` returns to the accepted threshold. Confirm that
`lastUpdatedTime` advances after the next source snapshot.

If the table remains stopped, check the database-level link before another
reset:

```sql
SELECT SYSTEM$CATALOG_LINK_STATUS('<database>');
```

Resolve any catalog connection or discovery error first. If discovery was
suspended, resume it after the cause is fixed:

```sql
ALTER DATABASE <database>
RESUME DISCOVERY;
```
