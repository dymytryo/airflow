{#
  A source-readiness test for catalog-linked Iceberg tables that do not always
  create a snapshot when an upstream cycle has no changes.

  The signals retain separate meanings:
    * CLD auto-refresh state proves Snowflake can keep following the table.
    * Snapshot recency proves data changed and Snowflake processed that snapshot.
    * A mapped Airflow task success proves the table was processed when no
      snapshot was expected.

  Readiness requires a healthy CLD table with no queued snapshots, plus either a
  recent snapshot or a recent success from the exact table-level Airflow task.
  DAG identifiers, the task-history relation, task identifiers, and thresholds
  are arguments; none are tied to a particular application.
#}

{% test cld_airflow_source_readiness(
    model,
    airflow_relation,
    validate_task_id,
    dag_ids,
    max_snapshot_age_hours=26,
    max_success_age_hours=26,
    lookback_hours=none,
    max_pending_snapshots=0
) %}

    {% if dag_ids is none or dag_ids is string or dag_ids | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "cld_airflow_source_readiness requires dag_ids as a non-empty YAML list"
        ) }}
    {% endif %}

    {% set source_database = model.database %}
    {% set source_schema = model.schema %}
    {% set source_table = model.identifier %}
    {% set safe_source_table = source_table | string | replace("'", "''") %}
    {% set safe_validate_task_id = validate_task_id | string | replace("'", "''") %}

    {% if lookback_hours is none %}
        {% set lookback_hours = max_success_age_hours * 2 %}
        {% if lookback_hours < 48 %}
            {% set lookback_hours = 48 %}
        {% endif %}
    {% endif %}

    {% if execute %}
        {% set show_sql %}
            show iceberg tables like '{{ safe_source_table }}'
            in schema {{ adapter.quote(source_database) }}.{{ adapter.quote(source_schema) }}
        {% endset %}
        {% set show_results = run_query(show_sql) %}
        {% set cld = namespace(found=false, refresh_status='{}') %}

        {% if show_results is not none %}
            {% for row in show_results.rows %}
                {% set result_name = row.get('name', row.get('NAME')) %}
                {% if result_name is not none
                    and result_name | string | lower == source_table | string | lower %}
                    {% set cld.found = true %}
                    {% set refresh_status = row.get(
                        'auto_refresh_status',
                        row.get('AUTO_REFRESH_STATUS')
                    ) %}
                    {% if refresh_status is not none %}
                        {% set cld.refresh_status = refresh_status | string %}
                    {% endif %}
                {% endif %}
            {% endfor %}
        {% endif %}

        {% set safe_refresh_status = cld.refresh_status | replace("'", "''") %}

        with task_runs as (
            select
                dag_id,
                task_id,
                lower(state) as task_state,
                start_date as started_at,
                try_number,
                row_number() over (
                    partition by
                        dag_id,
                        task_id,
                        start_date
                    order by
                        coalesce(try_number, 0) desc
                ) as attempt_rank
            from {{ airflow_relation }}
            where task_id = '{{ safe_validate_task_id }}'
              and dag_id in (
                {% for dag_id in dag_ids %}
                    '{{ dag_id | string | replace("'", "''") }}'{% if not loop.last %},{% endif %}
                {% endfor %}
              )
              and start_date >= dateadd(
                  'hour',
                  -{{ lookback_hours }},
                  current_timestamp()
              )
        ),
        deduped_task_runs as (
            select
                dag_id,
                task_id,
                task_state,
                started_at,
                try_number
            from task_runs
            where attempt_rank = 1
        ),
        latest_task as (
            select
                dag_id,
                task_id,
                task_state,
                started_at,
                try_number
            from deduped_task_runs
            qualify row_number() over (
                order by started_at desc, coalesce(try_number, 0) desc
            ) = 1
        ),
        task_summary as (
            select
                count(*) as run_count,
                max(case when task_state = 'success' then started_at end) as last_success_at
            from deduped_task_runs
        ),
        airflow_signal as (
            select
                summary.run_count,
                summary.last_success_at,
                latest.dag_id,
                latest.task_id,
                latest.task_state,
                latest.started_at as latest_started_at,
                datediff(
                    'minute',
                    summary.last_success_at,
                    current_timestamp()
                ) / 60.0 as success_age_hours
            from task_summary as summary
            left join latest_task as latest on true
        ),
        cld_status as (
            select
                {{ 'true' if cld.found else 'false' }} as table_found,
                parse_json('{{ safe_refresh_status }}') as refresh_status
        ),
        cld_signal as (
            select
                table_found,
                refresh_status:executionState::varchar as refresh_state,
                refresh_status:invalidExecutionStateReason::varchar as refresh_error,
                try_to_timestamp_tz(
                    refresh_status:lastSnapshotTime::varchar
                ) as last_snapshot_at,
                try_to_timestamp_tz(
                    refresh_status:lastUpdatedTime::varchar
                ) as last_updated_at,
                coalesce(
                    try_to_number(refresh_status:pendingSnapshotCount::varchar),
                    0
                ) as pending_snapshot_count
            from cld_status
        ),
        signals as (
            select
                cld.*,
                airflow.*,
                datediff(
                    'minute',
                    cld.last_snapshot_at,
                    current_timestamp()
                ) / 60.0 as snapshot_age_hours
            from cld_signal as cld
            cross join airflow_signal as airflow
        ),
        evaluated as (
            select
                *,
                case
                    when not table_found
                        then 'CLD_TABLE_NOT_FOUND'
                    when refresh_state is null
                        then 'CLD_AUTO_REFRESH_STATUS_MISSING'
                    when upper(refresh_state) != 'RUNNING'
                        then concat('CLD_REFRESH_', upper(refresh_state))
                    when pending_snapshot_count > {{ max_pending_snapshots }}
                        then 'CLD_REFRESH_PENDING'
                    when snapshot_age_hours <= {{ max_snapshot_age_hours }}
                        then null
                    when run_count = 0
                        then 'NO_AIRFLOW_RUN_IN_LOOKBACK'
                    when task_state != 'success'
                        then concat('AIRFLOW_TASK_', upper(coalesce(task_state, 'UNKNOWN')))
                    when last_success_at is null
                        then 'NO_AIRFLOW_SUCCESS_IN_LOOKBACK'
                    when success_age_hours > {{ max_success_age_hours }}
                        then 'STALE_AIRFLOW_SUCCESS'
                    else null
                end as failure_reason
            from signals
        )
        select
            failure_reason,
            '{{ source_database | string | replace("'", "''") }}' as source_database,
            '{{ source_schema | string | replace("'", "''") }}' as source_schema,
            '{{ safe_source_table }}' as source_table,
            refresh_state as cld_refresh_state,
            refresh_error as cld_refresh_error,
            last_snapshot_at as cld_last_snapshot_at,
            last_updated_at as cld_last_updated_at,
            pending_snapshot_count as cld_pending_snapshot_count,
            snapshot_age_hours,
            {{ max_snapshot_age_hours }} as max_snapshot_age_hours,
            dag_id as airflow_dag_id,
            '{{ safe_validate_task_id }}' as airflow_task_id,
            task_state as airflow_task_state,
            latest_started_at as airflow_latest_started_at,
            last_success_at as airflow_last_success_at,
            success_age_hours,
            {{ max_success_age_hours }} as max_success_age_hours
        from evaluated
        where failure_reason is not null
    {% else %}
        select
            cast(null as varchar) as failure_reason
        where false
    {% endif %}

{% endtest %}
