{#
  Checks only the per-table Snowflake Iceberg auto-refresh pipe.

  A healthy catalog link does not prove that this table's pipe is running. The
  status function is executed with run_query because Snowflake rejects it when
  dbt wraps the test SQL in its count subquery.
#}

{% test iceberg_auto_refresh_status(model, max_pending_snapshots=0) %}

    {% set source_database = model.database %}
    {% set source_schema = model.schema %}
    {% set source_table = model.identifier %}
    {% set qualified_table = (
        adapter.quote(source_database)
        ~ '.' ~ adapter.quote(source_schema)
        ~ '.' ~ adapter.quote(source_table)
    ) %}
    {% set safe_qualified_table = qualified_table | string | replace("'", "''") %}

    {% if execute %}
        {% set status_sql %}
            select system$auto_refresh_status('{{ safe_qualified_table }}')
                as refresh_status
        {% endset %}
        {% set status_results = run_query(status_sql) %}
        {% set auto_refresh = namespace(status='{}') %}

        {% if status_results is not none
            and status_results.rows | length > 0
            and status_results.rows[0][0] is not none %}
            {% set auto_refresh.status = status_results.rows[0][0] | string %}
        {% endif %}

        {% set safe_status = auto_refresh.status | replace("'", "''") %}

        with auto_refresh_status as (
            select parse_json('{{ safe_status }}') as refresh_status
        ),
        auto_refresh_signal as (
            select
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
            from auto_refresh_status
        ),
        evaluated as (
            select
                *,
                case
                    when refresh_state is null
                        then 'ICEBERG_AUTO_REFRESH_STATUS_UNAVAILABLE'
                    when upper(refresh_state) != 'RUNNING'
                        then concat('ICEBERG_AUTO_REFRESH_', upper(refresh_state))
                    when nullif(trim(refresh_error), '') is not null
                        then 'ICEBERG_AUTO_REFRESH_ERROR'
                    when pending_snapshot_count > {{ max_pending_snapshots }}
                        then 'ICEBERG_AUTO_REFRESH_PENDING'
                    else null
                end as failure_reason
            from auto_refresh_signal
        )
        select
            failure_reason,
            '{{ source_database | string | replace("'", "''") }}' as source_database,
            '{{ source_schema | string | replace("'", "''") }}' as source_schema,
            '{{ source_table | string | replace("'", "''") }}' as source_table,
            refresh_state as iceberg_auto_refresh_state,
            refresh_error as iceberg_auto_refresh_error,
            last_snapshot_at as iceberg_last_snapshot_at,
            last_updated_at as iceberg_last_updated_at,
            pending_snapshot_count as iceberg_pending_snapshot_count,
            {{ max_pending_snapshots }} as max_pending_snapshots
        from evaluated
        where failure_reason is not null
    {% else %}
        select
            cast(null as varchar) as failure_reason
        where false
    {% endif %}

{% endtest %}

{#
  Alternative for a monitoring role that cannot own the table: Snowflake also
  exposes the same auto-refresh payload in SHOW ICEBERG TABLES. Preserve this
  lookup if the deployment chooses the broader SHOW access model.

        {% set safe_source_table = source_table | string | replace("'", "''") %}
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
#}
