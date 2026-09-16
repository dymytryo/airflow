from __future__ import annotations

"""
Run simple diagnostic commands inside the same ECS container/task definition used by the dbt jobs.

This helps debug filesystem layout and environment from the container perspective (not the Airflow worker).

Usage: trigger with conf, e.g.
{
    "command": "ls -al /code/dbt-project && find /code/dbt-project -maxdepth 3 -name manifest.json"
}
"""

import pendulum
from airflow.decorators import dag
from airflow.models.param import Param

from operators.mwaa_dbt_container_operator import MwaaDbtContainerOperator


@dag(
    dag_id="debug_ecs_container_runner",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["debug", "ecs", "mwaa", "dbt"],
    params={
        "commands": Param(
            default=["pwd", "ls -al /workspace/dbt", "find /workspace/dbt -maxdepth 3 -name manifest.json"],
            type="array",
            description="Commands to run inside the dbt container.",
        ),
    },
    doc_md="""
Diagnose the ECS/Fargate container environment used for dbt jobs.

Trigger examples:

```json
{"commands": ["pwd", "env | sort", "find /workspace/dbt -maxdepth 3 -name manifest.json"]}
```
""",
)
def ecs_container_debug_runner_dag():
    full_command = (
        "set -eo pipefail; "
        "echo '--- whoami'; whoami; "
        "echo '--- pwd'; pwd; "
        "echo '--- env subset'; "
        "echo AIRFLOW_HOME=${AIRFLOW_HOME:-<unset>}; "
        "echo ENVIRONMENT=${ENVIRONMENT:-<unset>}; "
        "echo PYTHONPATH=${PYTHONPATH:-<unset>}; "
        "echo '--- running user command'; "
        "{{ (dag_run.conf.get('commands') | join(' && ')) if dag_run and dag_run.conf.get('commands') is not none else (params.commands | join(' && ')) }}"
    )

    MwaaDbtContainerOperator(
        task_id="ecs_container_debug",
        command=full_command,
    )


ecs_container_debug_runner_dag()
