from __future__ import annotations

"""
Airflow/MWAA environment debug runner.

- Runs ad-hoc shell commands from the worker to inspect paths/permissions/env.
- Trigger with conf or params, e.g.:
  {
    "commands": ["pwd", "ls -al /usr/local/airflow/dags"]
  }
"""

import logging
import subprocess
from typing import Any, Dict, Iterable, Optional, Sequence

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context

from airflow import DAG

logger = logging.getLogger(__name__)


def run_debug_commands() -> None:
    """Execute user-provided shell commands (or defaults) and log stdout/stderr."""
    context = get_current_context()
    conf = context.get("dag_run").conf or {}
    params = context.get("params") or {}
    raw_commands = conf.get("commands") or conf.get("command") or conf.get("cmd") or params.get("commands")

    commands: list[str]
    if isinstance(raw_commands, str):
        commands = [raw_commands]
    elif isinstance(raw_commands, Iterable):
        commands = [str(c) for c in raw_commands if c]
    else:
        commands = [
            "pwd",
            "whoami",
            "ls -al .",
            "ls -al /usr/local/airflow/dags",
            "ls -al /usr/local/airflow",
            "echo $PYTHONPATH",
            "python - <<'PY'\nimport sys\nprint('\\n'.join(sys.path))\nPY",
        ]

    logger.info("Executing %s command(s): %s", len(commands), commands)
    for cmd in commands:
        logger.info("Running command: %s", cmd)
        try:
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=60)
        except subprocess.TimeoutExpired:
            logger.warning("Command timed out after 60s: %s", cmd)
            continue
        except Exception as exc:
            logger.warning("Command failed to execute: %s (%s)", cmd, exc)
            continue

        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        logger.info("Command rc=%s: %s", result.returncode, cmd)
        if stdout:
            logger.info("stdout:\n%s", stdout)
        if stderr:
            logger.info("stderr:\n%s", stderr)


DOC_MD = """
### Usage
- Trigger DAG with `commands` as a single string or a list, e.g.:
```
{
  "commands": ["pwd", "ls -al /usr/local/airflow/dags"]
}
```
- Defaults: pwd, whoami, ls on current dir, /usr/local/airflow/dags, /usr/local/airflow.
"""


dag = DAG(
    dag_id="debug_airflow_env_runner",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    tags=["debug", "mwaa"],
    default_args={"owner": "DataOps", "retries": 0},
    catchup=False,
    max_active_runs=1,
    doc_md=DOC_MD,
    params={
        "commands": Param(
            default=["pwd", "ls -al /usr/local/airflow/dags"],
            type="array",
            description="Commands to run (string or list).",
        )
    },
)

with dag:
    PythonOperator(
        task_id="run_debug_commands",
        python_callable=run_debug_commands,
    )
