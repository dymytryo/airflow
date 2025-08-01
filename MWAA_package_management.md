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
This will give you:
```
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Platform: Linux-5.10.238-234.956.amzn2.x86_64-x86_64-with-glibc2.34
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Python version: 3.11.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Packages:
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - aiobotocore==2.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - aiohttp==3.9.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - aioitertools==0.11.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - aiosignal==1.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - airflow-provider-paradime-dbt==1.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - alembic==1.13.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - amqp==5.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - annotated-types==0.6.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - anyio==4.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-amazon==8.16.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-celery==3.5.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-common-io==1.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-common-sql==1.10.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-ftp==3.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-http==4.8.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-imap==3.5.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-postgres==5.10.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-slack==8.5.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-sqlite==3.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow-providers-tableau==4.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apache-airflow==2.8.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - apispec==6.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - argcomplete==3.2.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - asgiref==3.7.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - asn1crypto==1.5.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - attrs==23.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Babel==2.14.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - backoff==2.2.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - beautifulsoup4==4.12.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - billiard==4.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - blinker==1.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - boto3==1.33.13
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - botocore==1.33.13
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - build==1.2.2.post1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cachecontrol==0.14.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cached-property==2.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cachelib==0.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - celery==5.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - certifi==2023.11.17
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cffi==1.16.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - charset-normalizer==3.3.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cleo==2.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - click-didyoumean==0.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - click-plugins==1.1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - click-repl==0.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - click==8.1.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - clickclick==20.10.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - colorama==0.4.6
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - colorlog==4.8.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - ConfigUpdater==3.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - connexion==2.14.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - crashtest==0.4.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cron-descriptor==1.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - croniter==2.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - cryptography==41.0.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - defusedxml==0.7.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Deprecated==1.2.14
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - dill==0.3.1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - distlib==0.3.8
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - distro==1.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - dnspython==2.4.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - docutils==0.20.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - dulwich==0.21.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - email-validator==1.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - fastjsonschema==2.19.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - filelock==3.13.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-AppBuilder==4.3.10
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-Babel==2.0.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-Caching==2.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-JWT-Extended==4.6.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-Limiter==3.5.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-Login==0.6.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - flask-session==0.5.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask-SQLAlchemy==2.5.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - flask-wtf==1.2.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Flask==2.2.5
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - flit-core==3.12.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - flower==2.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - fluent-logger==0.11.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - frozenlist==1.4.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - fsspec==2023.12.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - google-re2==1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - googleapis-common-protos==1.62.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - greenlet==3.0.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - grpcio==1.60.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - gunicorn==21.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - h11==0.14.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - httpcore==0.16.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - httpx==0.23.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - humanize==4.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - idna==3.6
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - importlib-metadata==6.11.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - importlib-resources==6.1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - inflection==0.5.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - installer==0.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - itsdangerous==2.1.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jaraco.classes==3.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jeepney==0.8.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Jinja2==3.1.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jmespath==0.10.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jsonpath-ng==1.6.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jsonschema-specifications==2023.12.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - jsonschema==4.20.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - keyring==24.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - kombu==5.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - lazy-object-proxy==1.10.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - limits==3.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - linkify-it-py==2.0.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - lockfile==0.12.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - loguru==0.7.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - lxml==5.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Mako==1.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - markdown-it-py==3.0.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Markdown==3.5.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - MarkupSafe==2.1.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - marshmallow-oneofschema==3.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - marshmallow-sqlalchemy==0.26.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - marshmallow==3.20.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - mdit-py-plugins==0.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - mdurl==0.1.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - more-itertools==10.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - msgpack==1.1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - multidict==6.0.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - nest-asyncio==1.5.9
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - numpy==1.24.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - openai==1.7.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-api==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-exporter-otlp-proto-common==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-exporter-otlp-proto-grpc==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-exporter-otlp-proto-http==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-exporter-otlp==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-proto==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-sdk==1.22.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - opentelemetry-semantic-conventions==0.43b0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - ordered-set==4.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - packaging==23.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pandas==2.1.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pathspec==0.12.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pendulum==3.0.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pexpect==4.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pinecone-client==2.2.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pip==25.1.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pkginfo==1.9.6
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - platformdirs==3.11.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pluggy==1.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - ply==3.11
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - poetry-core==1.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - poetry-plugin-export==1.8.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - poetry==1.8.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - prison==0.2.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - prometheus-client==0.19.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - prompt-toolkit==3.0.43
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - protobuf==4.25.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - psutil==5.9.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - psycopg2-binary==2.9.9
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - psycopg2==2.9.10
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - ptyprocess==0.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pycparser==2.21
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pycurl==7.45.6
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pydantic-core==2.14.6
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pydantic==2.5.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pygments==2.17.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - PyJWT==2.8.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pyproject-hooks==1.2.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - python-daemon==3.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - python-dateutil==2.8.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - python-nvd3==0.15.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - python-slugify==8.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - pytz==2023.3.post1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - PyYAML==6.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rapidfuzz==3.13.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - redshift-connector==2.0.918
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - referencing==0.32.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - regex==2023.12.25
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - requests-toolbelt==1.0.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - requests==2.31.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rfc3339-validator==0.1.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rfc3986==1.5.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rich-argparse==1.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rich==13.7.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - rpds-py==0.17.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - s3transfer==0.8.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - scramp==1.4.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - SecretStorage==3.3.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - setproctitle==1.3.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - setuptools-scm==8.3.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - setuptools==65.5.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - shellingham==1.5.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - six==1.16.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - slack-sdk==3.26.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - smart-open==7.0.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - sniffio==1.3.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - soupsieve==2.5
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - SQLAlchemy-JSONField==1.0.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - sqlalchemy-redshift==0.8.14
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - SQLAlchemy-Utils==0.41.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - SQLAlchemy==1.4.51
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - sqlparse==0.4.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - statsd==4.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tableauserverclient==0.29
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tabulate==0.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tenacity==8.2.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - termcolor==2.4.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - text-unidecode==1.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tiktoken==0.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - time-machine==2.13.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tomlkit==0.12.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tornado==6.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tqdm==4.66.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - trove-classifiers==2024.1.8
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - typing-extensions==4.9.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - tzdata==2023.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - uc-micro-py==1.0.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - unicodecsv==0.14.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - universal-pathlib==0.1.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - urllib3==2.0.7
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - vine==5.1.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - virtualenv==20.25.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - watchtower==3.0.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - wcwidth==0.2.13
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - Werkzeug==2.2.3
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - wheel==0.45.1
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - wrapt==1.16.0
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - wtforms==3.1.2
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - yarl==1.9.4
[2025-07-31, 20:55:12 UTC] {{logging_mixin.py:188}} INFO - zipp==3.17.0
```
