Use a DAG to fetch Python version and all installed packages:
```python
import datetime
from airflow.decorators import dag, task  # type: ignore

@dag(
    default_args={"owner": "airflow"},
    schedule_interval=None,
    start_date=datetime.datetime(2022, 12, 1),
    catchup=False,
    tags=["mwaa"],
)
def mwaa_info():
    @task(retries=0)
    def print_platform_info():
        import platform
        print(f"Platform: {platform.platform()}")
        print(f"Python version: {platform.python_version()}")

    @task(retries=0)
    def print_packages():
        import pkg_resources
        requirements = sorted(
            [f"{x.project_name}=={x.version}" for x in pkg_resources.working_set],
            key=str.lower
        )
        print("Packages:")
        for r in requirements:
            print(r)

    print_platform_info()
    print_packages()

mwaa_info_dag = mwaa_info()
```
