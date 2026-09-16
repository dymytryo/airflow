import json
import os
import random
import time
from datetime import datetime
from typing import Dict, Optional

import boto3
import requests
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from dateutil import tz


class MwaaDagTriggerOperator(BaseOperator):
    """Trigger a DAG in an MWAA environment through the MWAA CLI API."""

    def __init__(
        self,
        *,
        mwaa_environment_name: str,
        target_dag_id: str,
        dag_conf: Dict,
        run_id_prefix: str = "triggered",
        delay_range_seconds=(1, 45),
        proxy_url: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.mwaa_environment_name = mwaa_environment_name
        self.target_dag_id = target_dag_id
        self.dag_conf = dag_conf
        self.run_id_prefix = run_id_prefix
        self.delay_range_seconds = delay_range_seconds
        self.proxy_url = proxy_url or os.environ.get("HTTPS_PROXY")

    def execute(self, context):
        delay_time = random.randint(*self.delay_range_seconds)
        self.log.info("Applying a %s second delay before triggering %s.", delay_time, self.target_dag_id)
        time.sleep(delay_time)

        client = boto3.client("mwaa")
        token = client.create_cli_token(Name=self.mwaa_environment_name)
        url = f"https://{token['WebServerHostname']}/aws_mwaa/cli"

        now = datetime.now().astimezone(tz.UTC)
        run_id = now.strftime(f"{self.run_id_prefix}__%Y-%m-%dT%H:%M:%S%f")
        conf_str = json.dumps(self.dag_conf)
        body = f"dags trigger {self.target_dag_id} --conf '{conf_str}' --run-id '{run_id}'"
        headers = {
            "Authorization": "Bearer " + token["CliToken"],
            "Content-Type": "text/plain",
        }
        proxies = {"https": self.proxy_url} if self.proxy_url else None

        self.log.info("Triggering DAG %s in MWAA environment %s.", self.target_dag_id, self.mwaa_environment_name)
        response = requests.post(url, data=body, headers=headers, proxies=proxies)

        if response.status_code != 200:
            self.log.error(
                "Failed to trigger MWAA DAG. status=%s response=%s",
                response.status_code,
                response.text,
            )
            raise AirflowException(
                f"MWAA DAG trigger failed with status {response.status_code}: {response.text}"
            )

        self.log.info("Successfully triggered DAG %s. response=%s", self.target_dag_id, response.text)
