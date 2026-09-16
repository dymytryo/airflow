from airflow import DAG
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


def test_snowflake_connectivity():
    # Use aws secrets manager for connection
    hook = SnowflakeHook(snowflake_conn_id='snowflake_gainsight')

    # Simple query to validate auth
    version = hook.get_first("SELECT CURRENT_VERSION()")

    print(f"SUCCESS: Connected to Snowflake! Version: {version}")


with DAG(
    'test_snowflake_connection',
    schedule_interval=None,  # Trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['debug']
) as dag:

    test_conn = PythonOperator(
        task_id='test_conn',
        python_callable=test_snowflake_connectivity
    )


test_snowflake_connectivity()
