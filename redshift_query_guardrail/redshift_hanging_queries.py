import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from psycopg2.extras import DictCursor


default_args = {
    "owner": "dataops",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


dag = DAG(
    "redshift_hanging_queries",
    default_args=default_args,
    description="Cancel Redshift queries that exceed a runtime threshold.",
    schedule_interval="0 */2 * * *",
    max_active_runs=1,
    catchup=False,
    tags=["redshift", "dataops", "query-guardrail"],
)


def kill_hanging_queries() -> None:
    """Terminate Redshift queries running longer than the configured threshold."""
    max_minutes = int(Variable.get("REDSHIFT_MAX_QUERY_MINUTES", default_var=90))
    max_microseconds = max_minutes * 60 * 1000 * 1000
    dsn = "host={host} dbname={dbname} user={user} password={password} port={port}".format(
        host=Variable.get("REDSHIFT_HOST"),
        dbname=Variable.get("REDSHIFT_DBNAME"),
        user=Variable.get("REDSHIFT_USER"),
        password=Variable.get("REDSHIFT_PASSWORD"),
        port=Variable.get("REDSHIFT_PORT", default_var=5439),
    )

    query = f"""
SELECT
    q.session_id AS pid,
    q.start_time AS start_time_utc,
    TRIM(u.usename) AS user_name,
    TRIM(q.query_text) AS query_text,
    LPAD(CAST(q.elapsed_time / 1000000 / 3600 AS VARCHAR), 2, '0') || ':' ||
    LPAD(CAST((q.elapsed_time / 1000000 % 3600) / 60 AS VARCHAR), 2, '0') || ':' ||
    LPAD(CAST(q.elapsed_time / 1000000 % 60 AS VARCHAR), 2, '0') AS readable_duration
FROM pg_catalog.sys_query_history q
LEFT JOIN pg_user u ON u.usesysid = q.user_id
WHERE q.end_time IS NULL
  AND q.elapsed_time >= {max_microseconds}
ORDER BY q.elapsed_time DESC
"""

    try:
        with psycopg2.connect(dsn) as conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                cur.execute(query)
                rows = cur.fetchall()
                logging.info("Found %s query/queries over %s minutes.", len(rows), max_minutes)
                for row in rows:
                    pid = row["pid"]
                    user = row["user_name"]
                    duration = row["readable_duration"]
                    logging.info("Canceling query %s by user %s, running for %s.", pid, user, duration)
                    cur.execute(f"CANCEL {pid};")
    except psycopg2.Error:
        logging.exception("Redshift query guardrail failed.")
        raise


kill_hanging_queries_task = PythonOperator(
    task_id="kill_hanging_queries",
    python_callable=kill_hanging_queries,
    dag=dag,
)
