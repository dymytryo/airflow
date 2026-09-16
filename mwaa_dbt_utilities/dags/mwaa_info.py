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
    def print_mwaa_info():
        import platform
        import pkg_resources

        print(f"Platform: {platform.platform()}")
        print(f"Python version: {platform.python_version()}")
        print("Packages:")
        requirements = sorted(
            [f"{x.project_name}=={x.version}" for x in pkg_resources.working_set],
            key=str.lower
        )
        for r in requirements:
            print(r)

    print_mwaa_info()


# As of Airflow 2.4, if the DAG is the result of a @dag decorated function, we
# don't have to "register" it as a global variable in order for Airflow detect
# it, but in the meantime,
mwaa_info_dag = mwaa_info()
