import os
import json
from airflow import DAG
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from operators.dwh_operator import DWHOperator
from utilities.slack_operator import task_fail_slack_alert


def load_conf(conf_key, bucket_name):
    """Returns the conf we will iterate through to create DAGs."""
    conf = read_variable(conf_key, bucket_name)
    if conf_key[-5:] == '.json':
        conf = json.loads(conf)
    return conf

if os.getenv('ENVIRONMENT') == 'dev':
    # Load config from local file system if in development
    conf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'conf', 'datalake_dags.json')
    with open(conf_path, 'r') as file:
        conf = json.load(file)
else:
    # Load config from S3 if in production
    CODE_BUCKET = "XXXXXXXXXXXXXXXXXX-us-west-2-XXXXXXXXXX"
    CONF_KEY = "lake_dags.json"
    conf = load_conf(CONF_KEY, CODE_BUCKET)


default_args = {
    'owner': 'DataOps',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

for c in conf:
    # Iterate through outlets - only interested in dwh
    dwh_dataset = None
    table_name = None

    for dataset_name in c['outlets']:
        if 'dwh' in dataset_name:
            dwh_dataset = Dataset(dataset_name)
            table_name = dataset_name.split('.')[-1]
            break

    if not dwh_dataset:
        continue # Skip the DAG creation if no relevant dataset found

    dag_id = f"stg__{table_name}"

    with DAG(dag_id, default_args=default_args, schedule=[dwh_dataset], tags=['staging']) as dag:
        task_id = f"stg__{table_name}_run"
        staging_dataset = Dataset(f"stg__{table_name}")

        # Task to run DBT command
        run_staging_model = DWHOperator(
            task_id=task_id,
            command=f"dbt run --select stg__{table_name} --target prod && dbt retry --target prod",
            retries=2,
            retry_delay=timedelta(minutes=5),
            on_failure_callback=task_fail_slack_alert,
            outlets=[staging_dataset]
        )

        globals()[dag_id] = dag
