from __future__ import annotations

"""
Pipeline refresh lineage DAG.

Builds/updates model_source_lineage from dbt manifest to support
critical_pipeline_control observability and self-healing.

Schedule: Hourly at :00 (8am-6pm PT weekdays), 30 min before self-heal DAG

Runtime overrides (Trigger DAG > conf):
- schema: target schema for model_source_lineage
- starburst_env: dev|uat|prod|local_mwaa
- chunksize: insert batch size (default 500)
- max_depth: optional lineage depth cap
- full_refresh: true|false (force rebuild)
- skip_if_unchanged: true|false (full rebuild mode only)
"""

from typing import Any, Dict

import pendulum
from airflow import DAG

from utilities.dag_factory import build_dag
from utilities.maintenance_utils import resolve_manifest_path
from utilities.runtime_env import default_starburst_env
from operators.dbt_container_operator import DbtContainerOperator

DEFAULT_CONFIG: Dict[str, Any] = {
    "chunksize": 500,
    "starburst_env": None,
    "manifest_path": None,
}

JOBS = [
    {
        "dag_id": "pipeline_refresh_lineage",
        "schedule": "0 8,10,12,14,16,18 * * 1-5",  # Every 2 hours, 8am-6pm PT weekdays
        "start_date": pendulum.datetime(2024, 1, 1, tz="US/Pacific"),
        "dag_tags": ["observability", "lineage", "pipeline_refresh"],
    }
]


def _build_container_command(job: Dict[str, Any]) -> str:
    manifest_path = resolve_manifest_path(job.get("manifest_path"))
    env_default = default_starburst_env(job.get("starburst_env"))
    chunksize_default = int(job.get("chunksize", DEFAULT_CONFIG["chunksize"]))

    chunksize_arg = (
        "--chunksize {{ dag_run.conf.chunksize if dag_run and dag_run.conf.get('chunksize') "
        f"else {chunksize_default} }}}}"
    )
    schema_arg = "{% if dag_run and dag_run.conf.get('schema') %} --schema {{ dag_run.conf.schema }}{% endif %}"
    max_depth_arg = "{% if dag_run and dag_run.conf.get('max_depth') %} --max-depth {{ dag_run.conf.max_depth }}{% endif %}"
    full_refresh_arg = "{% if dag_run and dag_run.conf.get('full_refresh') %} --full-refresh{% endif %}"
    skip_unchanged_arg = "{% if dag_run and dag_run.conf.get('skip_if_unchanged') %} --skip-if-unchanged{% endif %}"
    env_arg = (
        "{% if dag_run and dag_run.conf.get('starburst_env') %} --env {{ dag_run.conf.starburst_env }}"
        "{% else %} --env '" + env_default + "'{% endif %}"
    )

    return (
        "bash -lc \"set -euo pipefail; set -x; cd /code/dbt-project && "
        "python -m scripts.observability.build_model_source_lineage "
        f"--manifest-path {manifest_path} "
        f"{chunksize_arg} "
        f"{schema_arg} "
        f"{max_depth_arg} "
        f"{full_refresh_arg} "
        f"{skip_unchanged_arg} "
        f"{env_arg}\\\""
    )


for job in JOBS:
    dag = build_dag(
        dag_id=job["dag_id"],
        schedule=job.get("schedule"),
        start_date=job.get("start_date", pendulum.datetime(2024, 1, 1, tz="UTC")),
        tags=job.get("dag_tags", ("observability", "lineage")),
        catchup=False,
        max_active_runs=1,
        doc_md=__doc__,
    )

    with dag:
        DbtContainerOperator(
            task_id="build_model_source_lineage",
            command=_build_container_command(job),
        )

    globals()[dag.dag_id] = dag
