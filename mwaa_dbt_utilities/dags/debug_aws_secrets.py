from __future__ import annotations

import json

import boto3
import pendulum
from airflow.decorators import dag, task


@dag(
    dag_id="debug_aws_secrets",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
    tags=["debug", "aws", "secrets"],
    doc_md="""
Checks that an AWS Secrets Manager secret exists and logs available keys.

This DAG never prints secret values.

Trigger example:

```json
{"secret": "analytics/example/service-account", "region": "us-west-2"}
```
""",
)
def debug_aws_secrets_dag():

    @task()
    def inspect_secret(**context):
        conf = (context.get("dag_run") or {}).conf or {}
        secret_name = conf.get("secret") or "analytics/example/service-account"
        region_name = conf.get("region") or "us-west-2"
        print(f"Inspecting secret metadata: {secret_name}")

        client = boto3.client(service_name="secretsmanager", region_name=region_name)
        try:
            response = client.get_secret_value(SecretId=secret_name)
        except Exception as exc:
            print(f"Failed to fetch secret metadata: {exc}")
            return

        payload = response.get("SecretString")
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                pass

        if isinstance(payload, dict):
            print(f"Secret keys: {sorted(payload.keys())}")
        else:
            print(f"Secret fetched with type={type(payload)}. Value not printed.")

    inspect_secret()


debug_aws_secrets_dag()
