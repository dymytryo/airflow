from __future__ import annotations

import json
import logging
import os
from typing import Any, Optional

import boto3
import pendulum
from airflow.decorators import dag, task
from botocore.exceptions import ClientError


log = logging.getLogger(__name__)


def get_execution_environment() -> str:
    if os.environ.get("AIRFLOW_HOME"):
        return os.environ.get("STARBURST_ENV", "prod")
    if os.environ.get("GITLAB_CI"):
        return os.environ.get("STARBURST_ENV", "preprod")
    return os.environ.get("STARBURST_ENV", "dev")


class StarburstConnector:
    """Small example connector for debugging Starburst/Trino connectivity."""

    def __init__(self, env: str, timeout: int = 120) -> None:
        self.valid_envs = ("dev", "preprod", "prod")
        if env not in self.valid_envs:
            raise ValueError(f"Environment must be one of {self.valid_envs}.")
        self.env = env
        self.timeout = timeout
        self.host = os.environ.get("TRINO_HOST", "trino.example.com")
        self.port = int(os.environ.get("TRINO_PORT", "8443"))
        self.connection: Optional[Any] = None

    def _get_credentials_from_secret(self) -> dict:
        secret_name = os.environ.get("TRINO_SECRET_NAME", "analytics/trino/service-account")
        region_name = os.environ.get("AWS_REGION", "us-west-2")
        log.info("Fetching credentials from AWS Secrets Manager: %s", secret_name)
        client = boto3.client(service_name="secretsmanager", region_name=region_name)
        try:
            response = client.get_secret_value(SecretId=secret_name)
            payload = json.loads(response["SecretString"])
            return {
                "user": payload.get("TRINO_USER"),
                "password": payload.get("TRINO_PASSWORD"),
            }
        except ClientError:
            log.exception("Failed to retrieve Starburst secret.")
            raise

    def connect(self) -> None:
        from trino.auth import BasicAuthentication, OAuth2Authentication
        from trino.dbapi import connect

        if self.connection is not None:
            return

        if self.env == "dev":
            auth = OAuth2Authentication()
        elif self.env == "preprod":
            user = os.environ.get("TRINO_USER")
            password = os.environ.get("TRINO_PASSWORD")
            if not user or not password:
                raise ValueError("Missing TRINO_USER or TRINO_PASSWORD.")
            auth = BasicAuthentication(user, password)
        else:
            creds = self._get_credentials_from_secret()
            auth = BasicAuthentication(creds["user"], creds["password"])

        log.info("Connecting to Starburst/Trino at %s:%s.", self.host, self.port)
        self.connection = connect(
            host=self.host,
            port=self.port,
            auth=auth,
            http_scheme="https",
            request_timeout=self.timeout,
        )

    def close(self) -> None:
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute(self, query: str, params: Optional[tuple] = None) -> list:
        if self.connection is None:
            raise RuntimeError("Connection not established.")
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()

    def __enter__(self) -> "StarburstConnector":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


@dag(
    dag_id="test_starburst_connection",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["debug", "starburst", "trino"],
)
def test_starburst_connection_dag():
    """Run a lightweight Starburst/Trino connectivity check."""

    @task
    def run_connection_test():
        env = get_execution_environment()
        log.info("Testing Starburst/Trino connection for environment: %s", env)
        with StarburstConnector(env=env) as connector:
            result = connector.execute("SELECT 1")
            log.info("Connection test passed. Result: %s", result)

    run_connection_test()


test_starburst_connection_dag()
