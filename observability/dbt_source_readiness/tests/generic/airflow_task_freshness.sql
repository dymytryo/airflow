{#
  Checks only the Airflow task-instance signal for one source table.

  The current-state task-instance relation, task identifier, map index, DAG
  identifiers, and thresholds are explicit arguments. This test has no
  dependency on CLD metadata or state.
#}

{% test airflow_task_freshness(
    model,
    airflow_relation,
    validate_task_id,
    dag_ids,
    validate_map_index=-1,
    max_success_age_hours=26,
    lookback_hours=none
) %}

    {% if dag_ids is none or dag_ids is string or dag_ids | length == 0 %}
        {{ exceptions.raise_compiler_error(
            "airflow_task_freshness requires dag_ids as a non-empty YAML list"
        ) }}
    {% endif %}

    {% set safe_validate_task_id = validate_task_id | string | replace("'", "''") %}

    {% if lookback_hours is none %}
        {% set lookback_hours = max_success_age_hours * 2 %}
        {% if lookback_hours < 48 %}
            {% set lookback_hours = 48 %}
        {% endif %}
    {% endif %}

    with task_runs as (
        select
            dag_id,
            task_id,
            run_id,
            coalesce(map_index, -1) as map_index,
            lower(state) as task_state,
            start_date as started_at,
            end_date as completed_at,
            try_number
        from {{ airflow_relation }}
        where task_id = '{{ safe_validate_task_id }}'
          and coalesce(map_index, -1) = {{ validate_map_index }}
          and dag_id in (
            {% for dag_id in dag_ids %}
                '{{ dag_id | string | replace("'", "''") }}'{% if not loop.last %},{% endif %}
            {% endfor %}
          )
          and coalesce(end_date, start_date) >= dateadd(
              'hour',
              -{{ lookback_hours }},
              current_timestamp()
          )
    ),
    latest_task as (
        select
            dag_id,
            task_id,
            run_id,
            map_index,
            task_state,
            started_at,
            completed_at,
            try_number
        from task_runs
        qualify row_number() over (
            order by
                started_at desc,
                coalesce(completed_at, started_at) desc,
                coalesce(try_number, 0) desc,
                dag_id desc,
                run_id desc,
                map_index desc
        ) = 1
    ),
    task_summary as (
        select
            count(*) as run_count,
            max(case when task_state = 'success' then completed_at end) as last_success_at
        from task_runs
    ),
    airflow_signal as (
        select
            summary.run_count,
            summary.last_success_at,
            latest.dag_id,
            latest.task_id,
            latest.run_id,
            latest.map_index,
            latest.task_state,
            latest.started_at as latest_started_at,
            latest.completed_at as latest_completed_at,
            datediff(
                'minute',
                summary.last_success_at,
                current_timestamp()
            ) / 60.0 as success_age_hours
        from task_summary as summary
        left join latest_task as latest on true
    ),
    evaluated as (
        select
            *,
            case
                when run_count = 0
                    then 'NO_AIRFLOW_RUN_IN_LOOKBACK'
                when coalesce(task_state, '') != 'success'
                    then concat('AIRFLOW_TASK_', upper(coalesce(task_state, 'UNKNOWN')))
                when last_success_at is null
                    then 'NO_AIRFLOW_SUCCESS_IN_LOOKBACK'
                when success_age_hours > {{ max_success_age_hours }}
                    then 'STALE_AIRFLOW_SUCCESS'
                else null
            end as failure_reason
        from airflow_signal
    )
    select
        failure_reason,
        '{{ model.database | string | replace("'", "''") }}' as source_database,
        '{{ model.schema | string | replace("'", "''") }}' as source_schema,
        '{{ model.identifier | string | replace("'", "''") }}' as source_table,
        dag_id as airflow_dag_id,
        run_id as airflow_run_id,
        map_index as airflow_map_index,
        '{{ safe_validate_task_id }}' as airflow_task_id,
        task_state as airflow_task_state,
        latest_started_at as airflow_latest_started_at,
        latest_completed_at as airflow_latest_completed_at,
        last_success_at as airflow_last_success_at,
        success_age_hours,
        {{ max_success_age_hours }} as max_success_age_hours
    from evaluated
    where failure_reason is not null

{% endtest %}
