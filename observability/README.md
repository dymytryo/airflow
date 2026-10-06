# Airflow Observability

Monitoring Airflow itself: whether DAGs ran, whether they succeeded, whether
they were late, and whether the data they were supposed to produce arrived.

Two distinct signals, and they are not interchangeable:

- **Job plane**: did the DAG run and exit successfully. Lives in Airflow.
- **Data plane**: is the table fresh and complete for the business date. Lives in
  the table.

A job can succeed and produce nothing, and a DAG that is paused or never
scheduled sends no failure notification at all. Absence of an alert is not proof
of success, so anything that matters needs a positive signal, not silence.

For catalog-linked Iceberg tables that do not create a snapshot on an empty
cycle, [`dbt_source_readiness/`](dbt_source_readiness/) exposes CLD refresh health
and Airflow task freshness as two independent dbt tests. Neither test reads the
other; the observability state model combines their results downstream.

## Options

| Option | Signal | Latency | Needs |
|---|---|---|---|
| Metastore export to the warehouse | Job | Minutes to a day, depending on method | Metastore or API access |
| REST API polling | Job | Polling interval | Read-only credential; blocked by a private web server on MWAA |
| StatsD metrics to Datadog or Grafana | Job | Near real time | Agent already collecting; evaluates no-data natively |
| `on_failure_callback` to Slack, Jira, or a webhook | Job | Immediate | A change in the DAG; reports failures only, never absence |
| Deadline alerts | Job | At the deadline | Airflow 3.1+; the Airflow 2 SLA mechanism was removed in 3.0 and was unreliable before that |
| Iceberg snapshot metadata checks | Data | Your schedule | Table read access only |
| `dbt source freshness` | Data | Your schedule | dbt, with `loaded_at_query` for metadata-based checks |
| Producer-published watermark table | Data | Immediate | A final task in the producing DAG |
| OpenLineage to a collector | Both | Near real time | Config on Airflow and Spark, plus a backend |

For the reasoning behind these, the readiness state model, the empty-batch
problem, the config-driven registry, and alert and ticket routing, see
[`design_notes.md`](design_notes.md).

## Implementations

The two run-history implementations land Airflow metadata in the warehouse so
it can be queried, trended, and joined to data-plane checks. Which one applies
depends on how Airflow is hosted, because that determines whether you can reach
the metastore directly.

| | [`mwaa_dagrun_export/`](mwaa_dagrun_export/) | [`eks_metastore_cdc/`](eks_metastore_cdc/) |
|---|---|---|
| Hosting | MWAA | Self-managed on EKS |
| Method | Scheduled DAG reading `DagRun` through the Airflow session, writing to Redshift | Change data capture (CDC) replay off the metastore database into the lake |
| Latency | As often as the export runs, typically daily | A couple of minutes |
| Why this one | AWS owns the metastore; there is no direct database access | The metastore is your own PostgreSQL or MySQL, so it can be replicated |
| Good for | Trends, reliability reporting, SLA history | Trends plus near-real-time gating |

The reason the two differ at all is access. On MWAA the metadata database is
inside an AWS-owned account, so the only way in is Airflow's own session or the
REST API, and that pushes you to a periodic batch export. On EKS the metastore
is an ordinary database you run, so it can be replicated continuously like any
other source.

See also [`../hosting/`](../hosting/) for the hosting trade-offs themselves, and
[`../slack_webhook/`](../slack_webhook/) for the alert-delivery callbacks both
implementations can feed.
