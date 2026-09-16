# Monitoring upstream Airflow pipelines you do not own

A downstream consumer needs two different signals from an upstream pipeline, and
they are not interchangeable:

1. **Job-plane**: did their directed acyclic graph (DAG) run, and did it exit
   successfully. Lives in their Airflow. Requires their cooperation or their
   credentials.
2. **Data-plane**: is the table fresh, complete, and usable for the business date
   you need. Lives in the table. Requires nothing but read access you already
   have.

The governing principle for a consumer with no ownership: **absence of a failure
alert is not proof of success.** A DAG that is paused, unscheduled, deleted, or
silently committing zero rows sends no failure notification at all. Job success
is also not proof of data: a change data capture (CDC) replay can exit zero
having written an empty commit. You need a positive assertion that data for date
D arrived, not the absence of a complaint.

That single fact decides the architecture: build detection on the data plane,
use the job plane for triage.

---

## Recommendation

| Layer | What | Depends on upstream team |
|---|---|---|
| **Primary detection** | Iceberg snapshot metadata checks (freshness + volume) from a YAML registry, gating your dbt build | No |
| **Contract** | Ask the producers to publish a completion watermark table, one row per application per business date | Yes, small |
| **Triage** | Read-only Airflow API credential, used when a check fails, not for alerting | Yes, near zero |
| **Routing** | Zapier or Slack webhook delivers the message your checker produces | No |
| **Longer term** | OpenLineage collector covering their Spark, their Airflow, and your dbt | Yes, medium |
| **Do not rely on** | Being added to their failure notifications as your monitor | n/a |

Start with the primary layer. It is the only option that works on day one, keeps
working when their team reorganizes, and measures the thing you actually care
about.

---

## Option 1: Iceberg snapshot metadata (recommended primary)

Every Iceberg table carries queryable metadata tables. The `snapshots` table
gives you exact commit times and per-commit row counts with no data scan and no
producer involvement.

Columns: `committed_at`, `snapshot_id`, `parent_id`, `operation`
(`append` / `overwrite` / `delete` / `replace`), `manifest_list`, and `summary`,
a string map holding `added-records`, `deleted-records`, `total-records`,
`added-data-files`, and related counters.

Freshness, costing essentially nothing:

```sql
select
    max(committed_at) as last_commit_at
from lakehouse_prod.app_billing.invoices.snapshots
```

Freshness and volume together, which catches the empty-commit case that a job
success signal misses:

```sql
select
    max(committed_at)                                   as last_commit_at,
    sum(cast(summary['added-records'] as bigint))       as records_added,
    count(*)                                            as commits
from lakehouse_prod.app_billing.invoices.snapshots
where committed_at >= current_timestamp - interval 24 hours
```

Dialect note: the path above is Spark. Trino addresses the same table as
`lakehouse_prod.app_billing."invoices$snapshots"`. Pick one engine for the
checker and stay there.

**Why this beats a row-level freshness scan:** it reads metadata only, it needs
no `loaded_at` column on the table, and it is accurate even when the replay
rewrites history rather than appending.

**Wire it into dbt** with `loaded_at_query` on the source, so `dbt source
freshness` uses the metadata path instead of scanning:

```yaml
sources:
  - name: app_billing
    schema: app_billing
    tables:
      - name: invoices
        loaded_at_query: |
          select max(committed_at)
          from lakehouse_prod.app_billing.invoices.snapshots
        freshness:
          warn_after: {count: 90, period: minute}
          error_after: {count: 180, period: minute}
```

Then gate the build, and let dbt skip what did not change:

```bash
dbt source freshness
dbt build --select source_status:fresher+ --state ./artifacts
```

Cost: one scheduled job. Ask of the upstream team: none.

---

## Option 2: Producer-published completion watermark (the ask worth making)

The cleanest contract, and cheap for them: one insert at the end of each replay
DAG.

```sql
create table if not exists lakehouse_prod.control.replay_watermark (
    application     varchar,
    table_name      varchar,
    business_date   date,
    status          varchar,
    records_written bigint,
    completed_at    timestamp
)
```

This is better than job metadata because it is semantic. It states "billing is
complete for 2026-09-14 with 412,338 records", which is the question you are
actually asking. A DagRun record only states that a process exited zero.

Once it exists, your monitoring, your dbt tests, and your gating all become
ordinary SQL against a table you can join. Ask for this before asking for
anything else on the job plane.

---

## Option 3: Airflow REST API polling

A read-only credential plus a scheduled poller gives you run states across every
application DAG.

- Airflow 3 exposes `/api/v2` with JSON Web Token (JWT) auth: POST credentials to
  `/auth/token`, then send `Authorization: Bearer <token>`. Airflow 2 exposes
  `/api/v1`. Confirm the exact paths against their instance's own specification.
- List runs per DAG with `GET /api/v{1,2}/dags/{dag_id}/dagRuns`, filtered by
  `state` and a start-date window. The `~` wildcard in place of `dag_id` returns
  runs across all DAGs, which is what makes this scale to one poller for N
  applications.
- If their Airflow is Amazon Managed Workflows for Apache Airflow (MWAA), you do
  not need network access into their virtual private cloud. The
  `mwaa:InvokeRestApi` identity and access management (IAM) permission reaches the
  API directly, which makes this a clean, auditable, read-only grant.

**Verdict:** excellent for triage and for trend reporting, weak as your only
detector. It tells you nothing when a DAG was never scheduled, and it still does
not tell you whether rows landed.

---

## Option 4: DagRun metadata export to a shared table

The pattern in [`medata_export/`](../medata_export/): pull `DagRun`
records from the metastore and land them in the warehouse.

Assessment:

- **Good** as a reporting substrate. Success rate by application, chronic
  lateness, mean time to recovery (MTTR), and the evidence you need for an
  escalation conversation.
- **Poor** as a gating substrate. It runs on someone else's schedule, it is as
  stale as its export cadence (the implementation in this repository exports
  daily over a five-day window), and it is a DAG that can itself fail silently,
  giving you a monitor that needs a monitor.

So: ask for it, use it for trends, and do not point your alerting at it.

---

## Option 5: Cross-instance notification

[`cross_account_dags/`](../cross_account_dags/) generates consumer DAGs from a JSON
configuration and triggers them off `Dataset` updates published by the producing
instance. This is the right shape when both sides run Airflow and the producer
agrees to publish outlets.

Two conditions limit it: it requires the producers to add outlets to every
replay DAG, and it assumes the consumer side runs Airflow. Where dbt is
orchestrated outside Airflow, the trigger has nowhere to land.

Two adjacent variants worth knowing:

- **Airflow assets and event-driven scheduling.** Airflow 3 replaces datasets
  with assets and adds `AssetWatcher`, which triggers a DAG from an external
  message queue. Amazon Simple Queue Service (SQS) and Kafka are supported. If
  the producers will drop a message per completed replay, any consumer can
  subscribe without touching their code again.
- **`ExternalTaskSensor` does not work across instances.** It reads the shared
  metastore. It is a common wrong turn, so rule it out explicitly.

---

## Option 6: OpenLineage

The standards-based answer, and the only one that covers Spark, Airflow, and dbt
in a single graph.

- Airflow has shipped `apache-airflow-providers-openlineage` since 2.7. It is
  enabled by configuration, not by rewriting DAGs.
- Spark emits through `OpenLineageSparkListener`, configured with
  `spark.openlineage.transport.url` and `spark.openlineage.namespace`. Their CDC
  replay jobs would report the Iceberg tables they wrote, with row counts,
  automatically.
- dbt emits through the OpenLineage dbt integration, so your side joins the same
  graph.
- Marquez is the reference collector.

**Verdict:** highest ceiling, and genuinely config-driven rather than
code-driven. It needs a backend someone operates and buy-in from the platform
team, so treat it as the target state rather than the first move.

---

## Option 7: Their alert routing (Slack, email)

Adding your channel to their `on_failure_callback` or `default_args['email']` is
the cheapest thing to ask for and the weakest thing to depend on.

- No positive confirmation. Silence is ambiguous.
- It breaks silently the first time they refactor, and you will not notice.
- It delivers their severity model, not yours. You will be paged for retries
  that self-heal and missed for a paused DAG.

Take it as a supplement. Never let it be the monitor.

One version-dependent trap: Airflow's `sla` and `sla_miss_callback` were removed
in Airflow 3.0, replaced by Deadline Alerts (AIP-86) landing in 3.1. In Airflow 2
the SLA mechanism is unreliable anyway, because a miss is only evaluated once a
run exists, so a DAG that never starts never misses its service-level agreement
(SLA). Do not accept "we have SLAs configured" as coverage.

---

## The config-driven design

This mirrors the registry and runner split already in `repositories/observability/`,
so it slots into the scorecards and transition alerting you have rather than
starting a parallel system.

**1. Registry.** Adding an application is one entry. That is the scalability
test for this design.

```yaml
version: 1

defaults:
  catalog: lakehouse_prod
  severity: medium
  channel_map:
    high: [pagerduty, slack]
    medium: [slack]

applications:
  - application: billing
    upstream_dag_id: cdc-replay-billing
    owner_slack: "#billing-data"
    priority: P1
    cadence: hourly
    tables:
      - name: app_billing.invoices
        sla_minutes: 90
        min_records_per_day: 50000
      - name: app_billing.payments
        sla_minutes: 90

  - application: fleet
    upstream_dag_id: cdc-replay-fleet
    owner_slack: "#fleet-data"
    priority: P2
    cadence: daily
    tables:
      - name: app_fleet.site_daily
        sla_minutes: 240
        min_records_per_day: 10000
```

**2. Generic checker.** Loop the registry, query `<table>.snapshots` once per
entry, grade against `sla_minutes` and `min_records_per_day`, write one row per
table per run to a results table. No per-application code.

**3. Transition-based alerting.** Alert on state change only, PASS to FAIL and
FAIL back to PASS. Re-alerting every cycle is how a channel gets muted, and a
muted channel is worse than no channel.

**4. Gate, then run.** The checker runs before your dbt trigger. On failure it
stops the build and posts the reason. Zapier delivers the message; it does not
detect the condition.

**5. Scorecard.** Daily grade per application. This is the artifact that makes
the conversation with the producing team factual rather than anecdotal.

---

## What to ask for, ranked by size of ask

1. **Confirm whether Airflow metrics already flow somewhere.** Airflow emits
   StatsD and OpenTelemetry (OTel) metrics natively, covering task failures, DAG
   run duration, and scheduling delay. If the platform team already ships these
   to Datadog or Grafana, you may need a dashboard and an alert rule, nothing
   more. Check this first; it is the cheapest possible win.
2. **A read-only API credential**, or `mwaa:InvokeRestApi` if it is MWAA.
3. **A written per-application delivery SLA**: "table X is complete for business
   date D by HH:MM." This costs them nothing and is what makes every threshold
   above defensible instead of guessed.
4. **The completion watermark table** (Option 2).
5. **DagRun export** to a shared schema (Option 4), for trends.
6. **OpenLineage emission** from Spark and Airflow to a shared collector
   (Option 6).

---

## Common questions

**Should I set up notifications to Slack, Gmail, or Zapier?** Set up Slack as the
delivery channel for checks *you* own. Do not subscribe to their failure
notifications as your monitoring strategy. Keep Zapier as the router and the dbt
trigger, and put a freshness gate in front of that trigger so a stale upstream
stops your build instead of quietly producing wrong marts.

**Should I push for the DagRun export table?** Ask for it, but for reporting, not
alerting. It answers "which application is chronically late" well and "can I run
right now" badly.

**Is cross-notification the answer?** It is the right pattern when both sides run
Airflow and the producer publishes outlets. Here the equivalent is the watermark
table or an SQS message, both of which give you the same event semantics without
requiring you to run an Airflow instance for dbt you orchestrate elsewhere.
