{#
  Checks only the Snowflake catalog-linked database signal for one Iceberg table.

  Snapshot timestamps are returned as diagnostic evidence but are not graded.
  A table can be healthy and current after a valid no-change cycle without a new
  snapshot. Airflow task freshness is intentionally checked by a separate test.
#}

{% test cld_table_refresh_health(model, max_pending_snapshots=0) %}

    {% set source_database = model.database %}
    {% set source_schema = model.schema %}
    {% set source_table = model.identifier %}
    {% set safe_source_table = source_table | string | replace("'", "''") %}

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

        with cld_status as (
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
                    else null
                end as failure_reason
            from cld_signal
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
            {{ max_pending_snapshots }} as max_pending_snapshots
        from evaluated
        where failure_reason is not null
    {% else %}
        select
            cast(null as varchar) as failure_reason
        where false
    {% endif %}

{% endtest %}
