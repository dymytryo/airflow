from __future__ import annotations

"""
Self-healing critical pipelines DAG.

Automatically refreshes stale dbt models identified by critical_pipeline_control.
Runs hourly at :30 after pipeline_refresh_lineage (8:30am-6:30pm PT weekdays).
Worst-case delay: 1 hour after SLA deadline passes.

Flow: rebuild_control_table -> find_stale_models -> heal_models -> complete_healing

Slack alerts:
- Green: Self-heal starting/completed
- Yellow: Dry run, or models blocked by stale sources (need upstream fix)
- Red: Self-heal failed

Runtime overrides (Trigger DAG > conf):
- dry_run: true|false (default: false)
- priority_filter: P0,P1 (default: P0,P1)
- max_models: int (default: 5)
"""

import json
import logging
import os
import urllib.request
from typing import Any, Dict, List, Optional

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable

from common.clients.starburst_client import StarburstClient
from operators.dbt_container_operator import DbtContainerOperator
from utilities.slack_operator import task_fail_slack_alert

# The webhook is configuration, never source: set the SELF_HEAL_SLACK_WEBHOOK Airflow Variable.
SLACK_WEBHOOK_URL = Variable.get("SELF_HEAL_SLACK_WEBHOOK", default_var="")

IS_DEV = os.environ.get("ENVIRONMENT") == "dev"
DATABASE = "enterprise_preprod" if IS_DEV else "lakehouse_prod"
SCHEMA = "observability_dev" if IS_DEV else "elementary"


def _send_slack(title: str, message: str, color: str = "#36a64f") -> None:
    """Send Slack alert. Colors: #36a64f (green), #ff0000 (red), #ffcc00 (yellow)"""
    # Use legacy attachment format for visible color bar
    payload = {
        "attachments": [{
            "color": color,
            "title": title,
            "text": message,
            "mrkdwn_in": ["text"]
        }]
    }
    try:
        req = urllib.request.Request(
            SLACK_WEBHOOK_URL,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logging.error(f"Slack alert failed: {e}")


def _query_stale_models(
    client: StarburstClient, priority_filter: List[str], max_models: int
) -> List[Dict[str, Any]]:
    """Query critical_pipeline_control for models needing self-heal."""
    priorities_sql = ", ".join(f"'{p}'" for p in priority_filter)

    # Log control table status
    count_query = f"""
        SELECT COUNT(DISTINCT model_name),
               COUNT(DISTINCT CASE WHEN requires_self_heal = TRUE THEN model_name END),
               COUNT(DISTINCT CASE WHEN model_is_stale = TRUE THEN model_name END)
        FROM {DATABASE}.{SCHEMA}.critical_pipeline_control
    """
    result = client.execute(count_query)
    if result:
        total, needs_heal, stale = result[0]
        logging.info(f"Control table: {total} monitored, {stale} stale, {needs_heal} need healing")

    # Get models to heal (priority order)
    # Filter to models that exist in dbt_models (avoids repeated failures for deleted models)
    query = f"""
        SELECT cpc.model_name, cpc.model_priority, cpc.target_asset
        FROM (
            SELECT DISTINCT model_name, model_priority, target_asset
            FROM {DATABASE}.{SCHEMA}.critical_pipeline_control
            WHERE requires_self_heal = TRUE AND model_priority IN ({priorities_sql})
        ) cpc
        INNER JOIN {DATABASE}.{SCHEMA}.dbt_models dm ON cpc.model_name = dm.name
        ORDER BY CASE cpc.model_priority WHEN 'P0' THEN 1 WHEN 'P1' THEN 2 WHEN 'P2' THEN 3 ELSE 4 END, cpc.model_name
        LIMIT {max_models}
    """
    rows = client.execute(query)
    return [{"model_name": row[0], "model_priority": row[1], "target_asset": row[2]} for row in rows]


def _query_blocked_by_source(
    client: StarburstClient, priority_filter: List[str]
) -> List[Dict[str, Any]]:
    """Query models blocked by stale sources (can't self-heal)."""
    priorities_sql = ", ".join(f"'{p}'" for p in priority_filter)

    query = f"""
        SELECT
            source_schema,
            source_table_name,
            source_job_name,
            source_job_status,
            source_job_last_run_at,
            array_agg(DISTINCT model_name ORDER BY model_name) AS blocked_models
        FROM {DATABASE}.{SCHEMA}.critical_pipeline_control
        WHERE model_is_stale = TRUE
          AND source_is_stale = TRUE
          AND model_priority IN ({priorities_sql})
          AND source_table_name IS NOT NULL
        GROUP BY source_schema, source_table_name, source_job_name, source_job_status, source_job_last_run_at
        ORDER BY array_join(array_agg(DISTINCT model_name), ', ')
        LIMIT 10
    """
    rows = client.execute(query)
    return [{
        "source_schema": row[0],
        "source_table_name": row[1],
        "source_job_name": row[2],
        "source_job_status": row[3],
        "source_job_last_run_at": row[4],
        "blocked_models": row[5]
    } for row in rows]


def _query_orphaned_models(client: StarburstClient) -> List[str]:
    """Find models in critical_pipeline_control that no longer exist in dbt_models (stale config)."""
    query = f"""
        SELECT DISTINCT cpc.model_name
        FROM {DATABASE}.{SCHEMA}.critical_pipeline_control cpc
        LEFT JOIN {DATABASE}.{SCHEMA}.dbt_models dm ON cpc.model_name = dm.name
        WHERE dm.name IS NULL
        ORDER BY cpc.model_name
        LIMIT 20
    """
    rows = client.execute(query)
    return [row[0] for row in rows]


def _expire_stale_pending(client: StarburstClient) -> int:
    """Expire 'pending' entries older than 2 hours (likely failed without callback)."""
    # First, count how many will be expired
    count_result = client.execute(f"""
        SELECT COUNT(*) FROM {DATABASE}.{SCHEMA}.self_heal_state
        WHERE status = 'pending'
          AND triggered_at < current_timestamp - INTERVAL '2' HOUR
    """)
    count = count_result[0][0] if count_result else 0

    if count > 0:
        client.execute(f"""
            UPDATE {DATABASE}.{SCHEMA}.self_heal_state
            SET completed_at = current_timestamp,
                status = 'timeout',
                error_message = 'Pending for >2 hours, likely orphaned'
            WHERE status = 'pending'
              AND triggered_at < current_timestamp - INTERVAL '2' HOUR
        """)

    return count


def _mark_pending(client: StarburstClient, model_name: str) -> None:
    client.execute(f"""
        INSERT INTO {DATABASE}.{SCHEMA}.self_heal_state
        (model_name, triggered_at, completed_at, status, error_message)
        VALUES ('{model_name}', current_timestamp, NULL, 'pending', NULL)
    """)


def _mark_complete(
    client: StarburstClient,
    model_name: str,
    status: str,
    error_msg: Optional[str] = None
) -> None:
    error_sql = f"'{error_msg[:500]}'" if error_msg else "NULL"
    client.execute(f"""
        UPDATE {DATABASE}.{SCHEMA}.self_heal_state
        SET completed_at = current_timestamp, status = '{status}', error_message = {error_sql}
        WHERE model_name = '{model_name}' AND status = 'pending'
          AND triggered_at = (
              SELECT MAX(triggered_at) FROM {DATABASE}.{SCHEMA}.self_heal_state
              WHERE model_name = '{model_name}' AND status = 'pending'
          )
    """)


def _on_heal_failure(context) -> None:
    """Mark all models as failed. If some actually succeeded, they won't be stale next run."""
    ti = context.get("task_instance")
    model_names_str = ti.xcom_pull(task_ids="find_stale_models") or ""
    if not model_names_str:
        return

    model_names = model_names_str.split()
    with StarburstClient() as client:
        for name in model_names:
            _mark_complete(client, name, "failed", "dbt run failed - check Airflow logs")

    model_list = ", ".join(f"`{n}`" for n in model_names)
    _send_slack(
        "Self-Heal Failed",
        f"Failed to refresh {len(model_names)} model(s): {model_list}\nCheck Airflow logs for details.",
        "#ff0000"
    )


@dag(
    dag_id="self_heal_critical_pipelines",
    schedule="30 8-18 * * 1-5",  # Hourly at :30, 8:30am-6:30pm PT weekdays
    start_date=pendulum.datetime(2024, 1, 1, tz="US/Pacific"),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "DataOps", "retries": 0, "on_failure_callback": task_fail_slack_alert},
    tags=["observability", "self-heal", "critical-pipelines"],
    doc_md=__doc__,
    params={"dry_run": False, "priority_filter": "P0,P1", "max_models": 5},
)
def self_heal_critical_pipelines():

    @task
    def find_stale_models(**context) -> str:
        """Find stale models, mark pending, alert. Returns space-separated model names."""
        params = context.get("params", {})
        dry_run = params.get("dry_run", False)
        priority_filter = params.get("priority_filter", "P0,P1").split(",")
        max_models = int(params.get("max_models", 5))

        with StarburstClient() as client:
            # Cleanup orphaned 'pending' entries (DAG crashed before completion)
            expired = _expire_stale_pending(client)
            if expired:
                logging.warning(f"Expired {expired} stale 'pending' entries (>2 hours old)")

            # Check for orphaned models (in critical_data_assets but deleted from dbt)
            orphaned = _query_orphaned_models(client)
            if orphaned:
                orphan_list = ", ".join(f"`{m}`" for m in orphaned[:10])
                logging.warning(f"Orphaned models (not in dbt_models): {orphan_list}")
                _send_slack(
                    "Orphaned Models in critical_data_assets",
                    f"These models are configured but no longer exist in dbt. Remove from critical_data_assets seed:\n{orphan_list}",
                    "#ffcc00"
                )

            models = _query_stale_models(client, priority_filter, max_models)
            blocked = _query_blocked_by_source(client, priority_filter)

            # Alert on models blocked by stale sources (can't self-heal, need upstream fix)
            if blocked:
                def _format_blocked_models(models) -> str:
                    """Safely format blocked_models from Trino array_agg (could be list, tuple, or None)."""
                    if not models:
                        return "unknown"
                    if isinstance(models, (list, tuple)):
                        return ", ".join(f"`{m}`" for m in models)
                    return f"`{models}`"  # Single value fallback

                blocked_list = "\n".join(
                    f"* `{b['source_schema']}.{b['source_table_name']}` "
                    f"(job: {b['source_job_name'] or 'unknown'}, status: {b['source_job_status'] or 'unknown'}) - "
                    f"blocks: {_format_blocked_models(b['blocked_models'])}"
                    for b in blocked
                )
                logging.warning(f"Models blocked by stale sources:\n{blocked_list}")
                _send_slack(
                    "Models Blocked by Stale Sources",
                    f"These models are stale but can't self-heal until upstream sources are refreshed:\n{blocked_list}",
                    "#ffcc00"
                )

            if not models:
                logging.info("No models require self-healing.")
                return ""

            model_list = "\n".join(
                f"* `{m['model_name']}` ({m['model_priority']}) -> `{m['target_asset']}`"
                for m in models
            )
            logging.info(f"Found {len(models)} stale models:\n{model_list}")

            if dry_run:
                _send_slack(
                    "[DRY RUN] Self-Heal Would Start",
                    f"Would refresh {len(models)} model(s):\n{model_list}",
                    "#ffcc00"
                )
                return ""

            _send_slack(
                "Self-Heal Starting",
                f"Refreshing {len(models)} stale model(s):\n{model_list}"
            )
            with StarburstClient() as client:
                for m in models:
                    _mark_pending(client, m["model_name"])

            return " ".join(m["model_name"] for m in models)

    @task
    def complete_healing(model_names_str: str, **context) -> None:
        """Mark healed models as success and send completion alert."""
        if not model_names_str:
            logging.info("No models were healed.")
            return

        model_names = model_names_str.split()
        with StarburstClient() as client:
            for name in model_names:
                _mark_complete(client, name, "success")

        _send_slack(
            "Self-Heal Completed",
            f"Refreshed {len(model_names)} model(s): {', '.join(f'`{n}`' for n in model_names)}"
        )

    # Task 1: Rebuild control table + dependencies (includes self_heal_state)
    rebuild_control_table = DbtContainerOperator(
        task_id="rebuild_control_table",
        command='bash -lc "set -euo pipefail; cd /code/dbt-project && dbt run --select +critical_pipeline_control --target enterprise_prod"',
    )

    # Task 2: Find stale models
    stale_models = find_stale_models()

    # Task 3: Heal models (skips if no models found)
    heal_models = DbtContainerOperator(
        task_id="heal_models",
        command=(
            'bash -lc "set -euo pipefail; cd /code/dbt-project && '
            "models='{{ ti.xcom_pull(task_ids='find_stale_models') }}' && "
            '[ -z \"$models\" ] && echo \"No models to heal\" && exit 0 || dbt run --select $models --target enterprise_prod"'
        ),
        on_failure_callback=_on_heal_failure,
    )

    # Task 4: Complete healing
    completion = complete_healing(stale_models)

    rebuild_control_table >> stale_models >> heal_models >> completion


dag = self_heal_critical_pipelines()
