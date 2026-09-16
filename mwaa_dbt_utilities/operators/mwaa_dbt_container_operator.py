import json
import os
from dataclasses import dataclass
from typing import Dict, Optional

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.ecs import EcsRunTaskOperator


@dataclass(frozen=True)
class DbtTarget:
    """Common dbt target names used by the example DAGs."""

    redshift_preprod = "redshift_preprod"
    redshift_prod = "redshift_prod"
    trino_preprod = "trino_preprod"
    trino_prod = "trino_prod"


@dataclass(frozen=True)
class EcsDbtRuntimeConfig:
    """Placeholder ECS/MWAA config. Replace these values in a real deployment."""

    task_definition: str = "analytics-dbt-task"
    cluster: str = "analytics-dbt-fargate-cluster"
    container_name: str = "analytics-dbt-container"
    awslogs_group: str = "analytics-dbt-log-group"
    awslogs_region: str = "us-west-2"
    network_config_bucket: str = "analytics-airflow-config-bucket"
    network_config_key: str = "airflow/conf/dbt_ecs_network_config.json"
    aws_account_id: str = "000000000000"


def _runtime_config_from_env() -> EcsDbtRuntimeConfig:
    return EcsDbtRuntimeConfig(
        task_definition=os.environ.get("DBT_ECS_TASK_DEFINITION", "analytics-dbt-task"),
        cluster=os.environ.get("DBT_ECS_CLUSTER", "analytics-dbt-fargate-cluster"),
        container_name=os.environ.get("DBT_ECS_CONTAINER_NAME", "analytics-dbt-container"),
        awslogs_group=os.environ.get("DBT_ECS_LOG_GROUP", "analytics-dbt-log-group"),
        awslogs_region=os.environ.get("AWS_REGION", "us-west-2"),
        network_config_bucket=os.environ.get("DBT_ECS_NETWORK_CONFIG_BUCKET", "analytics-airflow-config-bucket"),
        network_config_key=os.environ.get("DBT_ECS_NETWORK_CONFIG_KEY", "airflow/conf/dbt_ecs_network_config.json"),
        aws_account_id=os.environ.get("AWS_ACCOUNT_ID", "000000000000"),
    )


if os.environ.get("ENVIRONMENT") != "dev":

    class BaseDbtContainerOperator(EcsRunTaskOperator):
        """Run a dbt shell command in an ECS/Fargate task from MWAA."""

        def __init__(self, command: str, runtime_config: Optional[EcsDbtRuntimeConfig] = None, **kwargs):
            self.command = command
            self.runtime_config = runtime_config or _runtime_config_from_env()
            self._setup_ecs_config(kwargs)
            super().__init__(**kwargs)

        def _setup_ecs_config(self, kwargs: Dict) -> None:
            cfg = self.runtime_config
            kwargs["task_definition"] = cfg.task_definition
            kwargs["cluster"] = cfg.cluster
            kwargs["launch_type"] = "FARGATE"
            kwargs["awslogs_group"] = cfg.awslogs_group
            kwargs["awslogs_region"] = cfg.awslogs_region
            kwargs["awslogs_stream_prefix"] = f"ecs/{cfg.container_name}"
            kwargs.setdefault("retries", 0)
            kwargs["overrides"] = {
                "containerOverrides": [
                    {
                        "name": cfg.container_name,
                        "command": [self.command],
                    }
                ]
            }

        def execute(self, context):
            self.network_configuration = self.fetch_network_configuration()
            return super().execute(context)

        def fetch_network_configuration(self) -> Dict:
            cfg = self.runtime_config
            raw = S3Hook().read_key(cfg.network_config_key, cfg.network_config_bucket)
            network_config = json.loads(raw)
            for item in network_config:
                if cfg.aws_account_id in item:
                    return item[cfg.aws_account_id]
            return {}


    class MwaaDbtContainerOperator(BaseDbtContainerOperator):
        """Template-friendly operator that can append a dbt target at execution time."""

        template_fields = (*BaseDbtContainerOperator.template_fields, "command")

        def __init__(self, command: str, dbt_target: Optional[str] = None, **kwargs):
            super().__init__(command=command, **kwargs)
            self.dbt_target = dbt_target

        def execute(self, context):
            if self.dbt_target:
                self.command = f"{self.command} --target {self.dbt_target}"
            return super().execute(context)


if os.environ.get("ENVIRONMENT") == "dev":
    from airflow.providers.docker.operators.docker import DockerOperator
    from docker.types import Mount

    class BaseDbtContainerOperator(DockerOperator):
        """Run the same dbt command in a local Docker container for development."""

        template_fields = (*DockerOperator.template_fields, "command")

        def __init__(self, command: str, **kwargs):
            self.command = command
            self._setup_docker_config(kwargs)
            super().__init__(**kwargs)

        def _setup_docker_config(self, kwargs: Dict) -> None:
            project_dir = os.environ.get("HOST_PROJECT_DIR", os.getcwd())
            home_dir = os.environ.get("HOST_HOME_DIR", os.path.expanduser("~"))
            kwargs["image"] = os.environ.get("DBT_DOCKER_IMAGE", "analytics-dbt:local")
            kwargs["entrypoint"] = ["/bin/bash", "-c", f"echo {self.command} && {self.command}"]
            kwargs["docker_url"] = os.environ.get("DOCKER_URL", "unix://var/run/docker.sock")
            kwargs["mount_tmp_dir"] = False
            kwargs["working_dir"] = "/workspace/dbt"
            kwargs["environment"] = kwargs.get("environment", {}) | {
                "PYTHONPATH": "/workspace/dbt:/workspace/dbt/scripts:$PYTHONPATH"
            }
            kwargs["mounts"] = [
                Mount(source=project_dir, target="/workspace/dbt", type="bind"),
                Mount(source=f"{home_dir}/.dbt", target="/root/.dbt", type="bind"),
                Mount(source=f"{home_dir}/.aws", target="/root/.aws", type="bind"),
            ]


    class MwaaDbtContainerOperator(BaseDbtContainerOperator):
        template_fields = (*BaseDbtContainerOperator.template_fields, "command", "entrypoint")

        def __init__(self, command: str, dbt_target: Optional[str] = None, **kwargs):
            super().__init__(command=command, **kwargs)
            self.dbt_target = dbt_target

        def execute(self, context):
            if self.dbt_target:
                self.command = f"{self.command} --target {self.dbt_target}"
            return super().execute(context)
