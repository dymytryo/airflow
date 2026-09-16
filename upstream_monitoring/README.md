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
| **Disambiguation** | Read-only Airflow API credential, to tell a running pipeline from a failed one and a quiet table from a broken one | Yes, near zero |
| **Routing** | Zapier or Slack webhook delivers the message your checker produces | No |
| **Ticketing** | Your checker raises its own Jira issue, deduplicated by a deterministic key and closed on recovery | No |
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

Freshness and volume together, which catches a commit that landed but carried
nothing. Read "The empty batch problem" before setting a row-count floor: an
empty batch is sometimes the correct outcome, and a fixed floor turns every quiet
weekend into a page.

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
- If their Airflow is Amazon Managed Workflows for Apache Airflow (MWAA), the
  `airflow:InvokeRestApi` identity and access management (IAM) permission reaches
  the API through the AWS application programming interface (API), scoped to an
  Airflow role such as `Viewer`. That makes it a clean, auditable, read-only
  grant.

**The constraint that decides the design.** From the MWAA documentation: while
configuring a private webserver, `InvokeRestApi` cannot be invoked from outside
of a virtual private cloud (VPC). Private webservers are the common enterprise
setup, so establish this before evaluating any hosted tool. If the webserver is
private, nothing running outside the VPC reaches it, no matter how capable.

Both MWAA authentication paths begin with a Signature Version 4 (SigV4) signed
AWS call, which matters for any tool that only speaks plain HTTP:

| Path | Mechanism | Limits |
|---|---|---|
| `airflow:InvokeRestApi` | `invoke_rest_api` with AWS credentials | 10 second timeout, 6 MB response, about 10 transactions per second |
| `airflow:CreateWebLoginToken` | Web login token (60 second life) exchanged for a session token (12 hour life), then `Authorization: Bearer` | Higher throughput |

The second path is friendlier to an external tool, because after the bootstrap it
is ordinary HTTPS with a bearer token. The bootstrap itself still needs AWS
signing.

### Driving the poll from an integration platform

| | Zapier | Workato |
|---|---|---|
| Native Airflow app | None; build on generic webhook, schedule, and code steps | None; generic HTTP connector |
| Private VPC access | No path | On-prem agent (OPA): outbound only on TCP 443, mutual TLS, no inbound firewall rules, and HTTP calls can route through it rather than from the vendor's addresses |
| SigV4 | Achievable in a code step, which runs Node.js with npm packages on paid plans | Handled by the AWS connectors and custom connector authentication with token refresh |
| Cadence | Minutes, plan dependent, jitters under load; 100 new items per poll after deduplication | Finer control through scheduled and polling triggers |

If it has to be an integration platform, use Workato, and the reason is the
on-prem agent rather than any feature of the connectors. Hand-rolling AWS request
signing inside a Zap to watch another team's pipeline produces a monitor that
nobody monitors.

**Verdict:** excellent for triage and for trend reporting, weak as your only
detector. It tells you nothing when a DAG was never scheduled, and it still does
not tell you whether rows landed.

---

## Option 3b: Airflow metrics to Datadog or Grafana

Airflow emits StatsD metrics natively, and the Datadog Agent consumes them
through DogStatsD. The mapper promotes `dag_id` and `task_id` to tags, so a
monitor can filter to one application.

Two reasons this outranks polling when it is already in place:

1. A monitor evaluates a **no-data** condition natively. That is the "the DAG
   never ran" case, which polling handles badly and failure callbacks miss
   entirely.
2. It costs nothing to consume. If the platform team already ships these metrics,
   the work is a dashboard and an alert rule.

Coverage caveat: reported metrics vary by executor. `airflow.ti_failures` and
`ti_successes`, `airflow.operator_failures` and `operator_successes`, and
`airflow.dag.task.duration` are not reported under `KubernetesExecutor`. Confirm
which executor the producing team runs before designing around a specific metric.

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

pipelines:
  - application: billing
    upstream_dag_id: cdc-replay-billing
    owner_slack: "#billing-data"
    priority: P1
    cadence: hourly
    expected_by: "06:00"
    commits_when_empty: true      # verified, see "The empty batch problem"
    tables:
      - name: app_billing.invoices
        sla_minutes: 90
      - name: app_billing.payments
        sla_minutes: 90

  - application: fleet
    upstream_dag_id: cdc-replay-fleet
    owner_slack: "#fleet-data"
    priority: P2
    cadence: daily
    expected_by: "04:30"
    commits_when_empty: false
    tables:
      - name: app_fleet.site_daily
        sla_minutes: 240
```

The unit of the registry is the pipeline, not the table, because the unit of work
upstream is the DAG. One replay DAG owning fifteen tables is one thing to poll,
one readiness decision, and one alert, rather than fifteen of each. At a hundred
tables this is the difference between ten API calls and a hundred.

**2. Two-tier checker.** Poll the cheap signal, fan out to the expensive one:

1. One batch call for DAG run states across the fleet. The REST API accepts a
   wildcard in place of `dag_id` and has a batch list form taking multiple DAG
   identifiers, so this stays one request as the registry grows.
2. On a transition to success, query `<table>.snapshots` for that pipeline's
   tables only.
3. Independently, on a slower cycle, sweep snapshots for every registered table
   regardless of job state.

Step 3 is not redundant. It is what keeps the design working when the API
credential expires, the DAG is renamed, or the webserver goes private. Job-plane
polling should make you faster, never be the thing you depend on.

**3. Let the data plane decide READY.** The two signals combine into a state
model, and the direction of the dependency matters:

| State | Job plane | Data plane | Action |
|---|---|---|---|
| `READY` | any, including unknown | qualifying commit present | Build |
| `RUNNING` | run in progress | no commit yet | Wait |
| `UPSTREAM_FAILED` | run failed | no commit | Alert, do not wait |
| `LATE` | no run, past `expected_by` | no commit | Alert |
| `DELIVERY_ERROR` | run succeeded | no qualifying commit followed | Alert, this is the interesting one |
| `WAITING` | no run, before `expected_by` | no commit | Do nothing |

`READY` is decided by the data plane alone. If the table holds a qualifying
commit for the business date, you can build, whatever Airflow says and whether or
not you can reach it. The job plane refines the reasons for *not* ready, which is
what turns a single unhelpful "stale" into wait, alert, or escalate.

Inverting this is tempting and wrong. If job success is the gate and the data
check is only verification, then losing API access blocks a build whose data
arrived perfectly.

**4. Transition-based alerting.** Alert on state change only, and treat the state
above as the state, not just pass and fail. Re-alerting every cycle is how a
channel gets muted, and a muted channel is worse than no channel.

**5. Gate, then run.** The checker runs before your dbt trigger. On failure it
stops the build and posts the reason. Zapier delivers the message; it does not
detect the condition.

**6. Scorecard.** Daily grade per application. This is the artifact that makes
the conversation with the producing team factual rather than anecdotal.

---

## The empty batch problem

A change data capture (CDC) replay runs, the source genuinely had no changes in
that window, and zero rows land. The job succeeds and the table is correct and
current. A freshness check on `max(committed_at)` fails anyway, because nothing
committed.

This is the case that breaks naive freshness monitoring, and it is why a fixed
row-count floor such as `min_records_per_day: 50000` is a false-positive
generator. It will fire every weekend, every holiday, and permanently for any
low-volume application.

The underlying issue is that two different questions get collapsed into one:

- **Was the table processed?** Did the pipeline consider this table in this cycle.
- **Was the table changed?** Did any rows actually move.

`max(committed_at)` answers the second. Freshness monitoring needs the first.

### First, find out empirically whether their writer commits on empty

Some writers commit unconditionally, producing a snapshot with `added-records`
of zero. Those are a processing heartbeat and freshness works normally. Others
skip the commit entirely when there is nothing to write, and then commit time is
a change signal that cannot carry an SLA. Which one you are dealing with is a
property of their job, not something to assume:

```sql
select
    date(committed_at)                                   as commit_date,
    count(*)                                             as commits,
    sum(case
            when cast(summary['added-records'] as bigint) = 0
            then 1 else 0
        end)                                             as empty_commits
from lakehouse_prod.app_billing.invoices.snapshots
where committed_at >= current_timestamp - interval 30 days
group by 1
order by 1
```

Empty commits present means the table has a heartbeat, so record it as
`commits_when_empty: true` and carry on. Absent means you need one of the
following.

### If they do not commit on empty

**This is where the job plane stops being optional.** With no commit and no run
status, a quiet table and a broken pipeline are indistinguishable until the SLA
expires. DAG success is the only thing that separates them, which makes the
read-only API credential or the watermark table a requirement rather than a
convenience for these applications.

Ranked:

1. **The watermark table** (Option 2) states "billing processed 2026-09-14, 0
   records" and ends the ambiguity outright. For low-volume applications this is
   the single most valuable thing to ask the producing team for.
2. **DAG run success** (Option 3) as the processed-marker, with the snapshot
   check confirming rows when rows were expected.
3. **Ask them to commit unconditionally.** Sometimes a one-line change in their
   writer, and it gives every consumer a heartbeat for free.

### Replace fixed floors with relative expectations

For tables that do move regularly, a static threshold is still the wrong shape.
Better, in increasing order of effort:

- **Compare against the table's own history.** Alert when the gap since the last
  commit exceeds a high percentile of the trailing thirty day gap distribution.
  This calibrates itself per table and needs no hand-set SLA, which is what makes
  it survive a hundred tables.
- **Match the weekday.** Compare volume against the trailing median for the same
  day of week, so Sunday is judged against Sundays.
- **Alert on a band, not a floor.** A tenfold spike is as much a delivery defect
  as a shortfall, and a full reload landing where an increment was expected is a
  common and expensive one.
- **Count consecutive empties.** One empty cycle is unremarkable. Five in a row
  for a table whose historical maximum is one is a broken source, even though no
  individual cycle looked wrong.

The last of these catches the failure mode nothing else does: a source that
quietly stopped producing, where every single check passes on its own terms.

Volume anomaly detection of this kind is what packaged tooling such as Elementary
already implements, which is a reasonable argument for configuring it rather than
building the statistics yourself.

---

## Raising a ticket on the consumer side

A consumer can raise its own Jira issue rather than waiting to be copied on the
producing team's alert. This is usually the right move: it puts the work in your
queue, with your priority and your service-level agreement (SLA), and it leaves a
record that a chat notification does not.

**Ticket from your own graded check, never from their alert.** Subscribing to
their failure notifications and auto-ticketing them imports their noise, so a
retry that self-heals in four minutes becomes an issue somebody has to close. The
check you own already knows the difference between a blip and a breach.

**Jira has no idempotent create.** The REST API will happily create the same
issue twice, and integrations that call it per alert cycle produce one ticket per
cycle. A twelve hour outage on an hourly check becomes twelve tickets. You have
to build the deduplication yourself:

1. Derive a deterministic key per condition, for example
   `upstream-freshness-billing-invoices`. Put it in a label or a custom field.
2. Search before creating. Query for an open issue carrying that key.
3. If one exists, comment on it with the new observation instead of creating a
   second. If none exists, create.
4. On the FAIL to PASS transition, transition the issue to done or comment that
   it recovered. Tickets that never close are how a queue stops being read.

This is the same transition-based rule as the Slack channel, with the extra
requirement that the open issue itself acts as the state.

**Make Jira a channel, not a special case.** It slots into the existing severity
map rather than becoming separate machinery:

```yaml
defaults:
  channel_map:
    high:   [pagerduty, slack, jira]
    medium: [slack, jira]
    low:    [slack]
```

**Carry the evidence in the issue body**, so triage does not start with a
question: table name, business date, last commit time, how far past SLA, records
added on the last commit, the upstream DAG identifier, and a deep link to that
run in their Airflow user interface.

**Routes, in rough order of directness:**

| Route | Notes |
|---|---|
| Your checker calls the Jira REST API directly | Most control, and the deduplication logic lives with the check that knows the state |
| Datadog monitor to Jira | Native integration; an issue template handle such as `@jira-<template>` on the monitor creates the issue on trigger. Case Management adds two-way sync through a Jira webhook, and Jira Service Management syncs acknowledge and close back to Datadog |
| Workato or Zapier Jira connector | Reasonable when the checker already reports into one of them, but you still own the deduplication |

**Two ticket types worth separating.** An incident issue says data is late right
now and needs action today. A debt issue says this pipeline missed its SLA
fourteen times this quarter and belongs in the producing team's backlog. The
second one is what actually changes behaviour, and it is fed by the trend data
from Option 4 and the scorecard, not by any single failure.

---

## What to ask for, ranked by size of ask

1. **Confirm whether Airflow metrics already flow somewhere.** Airflow emits
   StatsD and OpenTelemetry (OTel) metrics natively, covering task failures, DAG
   run duration, and scheduling delay. If the platform team already ships these
   to Datadog or Grafana, you may need a dashboard and an alert rule, nothing
   more. Check this first; it is the cheapest possible win (Option 3b).
2. **Whether the MWAA webserver is public or private**, which determines whether
   any externally hosted tool can reach the API at all (Option 3).
3. **A read-only API credential**, or `airflow:InvokeRestApi` if it is MWAA.
4. **A written per-application delivery SLA**: "table X is complete for business
   date D by HH:MM." This costs them nothing and is what makes every threshold
   above defensible instead of guessed.
5. **The completion watermark table** (Option 2).
6. **DagRun export** to a shared schema (Option 4), for trends.
7. **OpenLineage emission** from Spark and Airflow to a shared collector
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

**Can an integration platform poll DAG status?** Workato can, including into a
private VPC through its on-prem agent. Zapier has no path into a private
webserver. Either way this is triage, not detection (Option 3).

**Can a callback be embedded in the producing DAG instead?** Yes, and a webhook
in their `on_success_callback` and `on_failure_callback` avoids the
authentication and network problems entirely. Two caveats: it is a change in
their repository, which is the same size of ask as the watermark table for less
return, and a fire-and-forget webhook cannot report a run that never happened, so
it still needs a deadman timer on the consumer side.

**Should the consumer raise its own ticket?** Yes. Ticket from your own graded
check rather than from their alert, deduplicate on a deterministic key because
Jira has no idempotent create, and close on recovery.
