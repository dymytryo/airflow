{#
  Checks only the Snowflake catalog-linked database discovery signal.

  Instantiate this generic test once per CLD, using one source table as the
  registration point. Attaching it to every table repeats the same CLD call.

  RUNNING does not guarantee that every entity synced, so failureDetails is
  graded separately. Per-table auto-refresh and Airflow are separate tests.
#}

{% test cld_catalog_link_status(model) %}

    {% set source_database = model.database %}
    {% set safe_database = source_database | string | replace("'", "''") %}

    {% if execute %}
        {% set status_sql %}
            select system$catalog_link_status('{{ safe_database }}') as link_status
        {% endset %}
        {% set status_results = run_query(status_sql) %}
        {% set catalog_link = namespace(status='{}') %}

        {% if status_results is not none
            and status_results.rows | length > 0
            and status_results.rows[0][0] is not none %}
            {% set catalog_link.status = status_results.rows[0][0] | string %}
        {% endif %}

        {% set safe_status = catalog_link.status | replace("'", "''") %}

        with catalog_link_status as (
            select parse_json('{{ safe_status }}') as link_status
        ),
        catalog_link_signal as (
            select
                link_status:executionState::varchar as link_state,
                link_status:failedExecutionStateReason::varchar as failure_reason_detail,
                link_status:failedExecutionStateErrorCode::varchar as failure_error_code,
                try_to_timestamp_tz(
                    link_status:lastLinkAttemptStartTime::varchar
                ) as last_link_attempt_at,
                coalesce(array_size(link_status:failureDetails), 0)
                    as failure_detail_count,
                link_status:failureDetails as failure_details
            from catalog_link_status
        ),
        evaluated as (
            select
                *,
                case
                    when link_state is null
                        then 'CLD_LINK_STATUS_UNAVAILABLE'
                    when upper(link_state) != 'RUNNING'
                        then concat('CLD_LINK_', upper(link_state))
                    when failure_detail_count > 0
                        then 'CLD_LINK_ENTITY_FAILURES'
                    else null
                end as failure_reason
            from catalog_link_signal
        )
        select
            failure_reason,
            '{{ safe_database }}' as source_database,
            link_state as cld_link_state,
            failure_reason_detail as cld_link_failure_reason,
            failure_error_code as cld_link_failure_error_code,
            last_link_attempt_at as cld_last_link_attempt_at,
            failure_detail_count as cld_failure_detail_count,
            failure_details as cld_failure_details
        from evaluated
        where failure_reason is not null
    {% else %}
        select
            cast(null as varchar) as failure_reason
        where false
    {% endif %}

{% endtest %}
