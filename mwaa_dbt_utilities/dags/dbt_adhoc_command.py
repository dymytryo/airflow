from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.models.param import Param

from operators.mwaa_dbt_container_operator import MwaaDbtContainerOperator


default_args = {
    "owner": "analytics_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="dbt_adhoc_command",
    default_args=default_args,
    catchup=False,
    tags=["dbt", "adhoc", "operations"],
    schedule=None,
    params={
        "command": Param("dbt run --target redshift_preprod --select your_model", type="string")
    },
) as dag:
    dag.doc_md = """
Manual dbt command execution DAG.

Use this for controlled operational tasks such as targeted model runs, tests,
or macros. Keep this DAG restricted to trusted operators.

Examples:

```json
{"command": "dbt run --target redshift_preprod --select marts.finance.revenue"}
```

```json
{"command": "dbt test --target redshift_preprod --select marts.finance.revenue"}
```

```json
{"command": "dbt run-operation run_execute_sql --target redshift_preprod --args '{\"sql_command\": \"select 1\"}'"}
```
"""

    MwaaDbtContainerOperator(
        task_id="run_dbt_command",
        command="{{ dag_run.conf.get('command', params.command) }}",
    )
