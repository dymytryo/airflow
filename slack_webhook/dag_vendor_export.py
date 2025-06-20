from datetime import datetime, timedelta

# Airflow core
from airflow.decorators import dag

# Paradime dbt provider operators
from paradime_dbt_provider.operators.paradime import ParadimeBoltDbtScheduleRunOperator
from paradime_dbt_provider.sensors.paradime import ParadimeBoltDbtScheduleRunSensor

# Example: Custom operator for business logic (replace as needed)
from operators.custom_container_operator import CustomContainerOperator

# Slack alert utility function (should be in utilities/slack_operator.py)
from utilities.slack_operator import task_fail_slack_alert

# Multi-channel Slack callback (sends to two Slack webhooks)
def dag_fail_alerts(context):
    task_fail_slack_alert(context, "slack_webhook_dbt_internal")
    task_fail_slack_alert(context, "slack_webhook_dbt_alerts")

default_args = {
    "conn_id": "paradime_bolt",
    "owner": "data_team",
    "email": [
        "alerts@example.com",
        "data-platform@example.com",
    ],
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
    "on_failure_callback": dag_fail_alerts,  # Slack alerts on every task failure
}

@dag(
    dag_id="primary_and_vendors_refresh",
    schedule="0 12-23,0-5 * * *",  # Runs hourly noon to 5 AM UTC
    catchup=False,
    default_args=default_args,
    tags=["dbt", "paradime", "slack-alerts"],
)
def primary_and_vendors_refresh():
    """
    Orchestrates Paradime dbt schedule runs and syncs external vendor models.
    Slack and email alerts are sent on every task failure.
    """

    # 1. Trigger Paradime dbt schedule
    refresh_primary_and_vendors = ParadimeBoltDbtScheduleRunOperator(
        task_id="refresh_primary_and_vendors",
        schedule_name="Primary and Vendors Refresh",
    )

    # 2. Wait for the Paradime run to complete
    wait_for_refresh = ParadimeBoltDbtScheduleRunSensor(
        task_id="wait_for_refresh",
        run_id="{{ ti.xcom_pull('refresh_primary_and_vendors') }}",
    )

    # 3. Run downstream containerized step (e.g., sync vendor models in warehouse)
    sync_external_vendors = CustomContainerOperator(
        task_id="sync_external_vendors",
        command="dbt run --select marts.external_vendors marts.external_reporting --threads 2 --target prod",
        retries=1,
        retry_delay=timedelta(minutes=15)
    )

    refresh_primary_and_vendors >> wait_for_refresh >> sync_external_vendors

dag = primary_and_vendors_refresh()
