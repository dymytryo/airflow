from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def task_fail_slack_alert(context, slack_webhook_conn_id="slack_webhook_alerts"):
    """
    Send a Slack alert for a failed task.

    Args:
        context (dict): Airflow task context.
        slack_webhook_conn_id (str): Airflow HTTP connection ID for the Slack webhook.
    """
    slack_msg = """
        :red_circle: Task Failed!
        *Task*: {task}
        *DAG*: {dag}
        *Execution Time*: {exec_date}
        *Log URL*: {log_url}
    """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("logical_date"),
        log_url=context.get("task_instance").log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id="slack_failure_alert",
        slack_webhook_conn_id=slack_webhook_conn_id,
        message=slack_msg,
        username="airflow",
    )
    return failed_alert.execute(context=context)

def task_warning_slack_alert(context, slack_webhook_conn_id="slack_webhook_alerts"):
    """
    Send a Slack alert for a task that is being retried (warning).

    Args:
        context (dict): Airflow task context.
        slack_webhook_conn_id (str): Airflow HTTP connection ID for the Slack webhook.
    """
    slack_msg = """
        :large_yellow_circle: WARNING: Retrying task...
        *Task*: {task}
        *DAG*: {dag}
        *Execution Time*: {exec_date}
        *Log URL*: {log_url}
    """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("logical_date"),
        log_url=context.get("task_instance").log_url,
    )
    warning_alert = SlackWebhookOperator(
        task_id="slack_warning_alert",
        slack_webhook_conn_id=slack_webhook_conn_id,
        message=slack_msg,
        username="airflow",
    )
    return warning_alert.execute(context=context)

def task_success_slack_alert(context, slack_webhook_conn_id="slack_webhook_alerts"):
    """
    Send a Slack alert for a successfully completed task.

    Args:
        context (dict): Airflow task context.
        slack_webhook_conn_id (str): Airflow HTTP connection ID for the Slack webhook.
    """
    slack_msg = """
        :large_blue_circle: Task Succeeded!
        *Task*: {task}
        *DAG*: {dag}
        *Execution Time*: {exec_date}
        *Log URL*: {log_url}
    """.format(
        task=context.get("task_instance").task_id,
        dag=context.get("task_instance").dag_id,
        exec_date=context.get("logical_date"),
        log_url=context.get("task_instance").log_url,
    )
    success_alert = SlackWebhookOperator(
        task_id="slack_success_alert",
        slack_webhook_conn_id=slack_webhook_conn_id,
        message=slack_msg,
        username="airflow",
    )
    return success_alert.execute(context=context)
