# Redshift Query Guardrail DAG

An Airflow DAG that cancels Redshift queries running longer than a configured
threshold, so a runaway query cannot hold warehouse capacity until someone
notices it.

## Why it exists

- Runaway BI or ad-hoc queries block warehouse capacity.
- Long-running sessions create queue pressure for scheduled dbt builds.
- DataOps wants an automated bound on query runtime without handing out broad
  console access.

## How it works

`sys_query_history` is the Redshift monitoring view that supersedes the older
`stl_query` tables. A row with `end_time IS NULL` is a query still executing, and
`elapsed_time` on that row is its runtime in microseconds. The DAG selects rows
past the threshold and issues `CANCEL <pid>` against each one, where the pid is
the query's `session_id`.

```text
every 2 hours
  └─ kill_hanging_queries
       ├─ read REDSHIFT_MAX_QUERY_MINUTES (default 90)
       ├─ SELECT from sys_query_history WHERE end_time IS NULL
       │                                  AND elapsed_time >= threshold
       └─ for each row: log user + duration, then CANCEL <pid>
```

`CANCEL` ends the query and leaves the session open, so a client holding the
connection stays connected and sees its statement aborted. Every cancellation is
logged with the user and the readable duration before it is issued, so the
Airflow task log is the audit trail for what the guardrail killed.

## Runtime inputs

Connection details come from Airflow Variables. The values are placeholders; do
not commit real credentials.

| Variable | Purpose |
|---|---|
| `REDSHIFT_HOST` | Redshift host |
| `REDSHIFT_DBNAME` | Database name |
| `REDSHIFT_USER` | Username |
| `REDSHIFT_PASSWORD` | Password |
| `REDSHIFT_PORT` | Optional, defaults to `5439` |
| `REDSHIFT_MAX_QUERY_MINUTES` | Optional, defaults to `90` |

The connecting user needs `CANCEL` rights on other users' queries, which means
either a superuser or the system-defined `sys:operator` role. Without it the select
succeeds and every cancel fails.

## Deploy and verify

1. Copy `redshift_hanging_queries.py` into the Airflow `dags/` directory.
2. Set the Variables above.
3. Raise `REDSHIFT_MAX_QUERY_MINUTES` well above normal runtime for the first
   run, trigger the DAG manually, and confirm the task log reports
   `Found 0 query/queries over N minutes.`
4. Lower the threshold to the real bound.

Pass condition: the task succeeds and its log lists either no queries or only
queries you expect to be cancelled. The task raises on any `psycopg2` error, so a
failed run means the guardrail did not execute, not that nothing was running.

## Related

[`data_warehouse/reference/redshift/`](https://github.com/dymytryo/data_warehouse/tree/main/reference/redshift)
holds the console queries behind this DAG: the live long-running query lookup it
automates, and a dbt incremental model that preserves `sys_query_history` past
the roughly one week Redshift retains.
