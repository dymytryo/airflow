from datetime import datetime, timedelta
from airflow.operators.python import ShortCircuitOperator
from airflow.decorators import dag
from my_dbt_provider.operators import DbtRunOperator
from my_dbt_provider.sensors import DbtRunSensor

default_args = {
    'conn_id': 'my_connection',
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
    'email': ['alerts@example.com'],
}

@dag(
    dag_id='monthly_job',
    default_args=default_args,
    catchup=False,
    schedule='30 17 * * *',       # 10:30 AM PT
    tags=['monthly', 'data_pipeline'],
    params={'run_type': 'scheduled'},
    doc_md="""
    Run monthly job only on weekdays that fall between 
    the 5th and 11th of each month.
    """,
)
def monthly_job():

    def is_calendar_and_business_day(check_date=None):
        if check_date is None:
            check_date = datetime.utcnow().date()
        # 1) between 5th–11th of month
        if not (5 <= check_date.day <= 11):
            return False
        # 2) Mon–Fri only
        return check_date.weekday() < 5

    gate = ShortCircuitOperator(
        task_id='check_calendar_and_business_window',
        python_callable=is_calendar_and_business_day,
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        job_name='Monthly Job',
    )

    wait = DbtRunSensor(
        task_id='wait_for_dbt',
        run_id='{{ ti.xcom_pull(task_ids="dbt_run") }}',
    )

    gate >> dbt_run >> wait

dag = monthly_job()
