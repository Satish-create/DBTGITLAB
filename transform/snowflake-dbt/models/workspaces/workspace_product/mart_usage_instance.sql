{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_usage_event', 'mart_usage_event'),
    ])
}}

WITH usage_events AS (
    SELECT
        {{ dbt_utils.surrogate_key(['event_date', 'event_name', 'dim_instance_id','plan_was_paid_at_event_date']) }}       AS mart_usage_instance_id,
        event_date,
        event_name,
        source,
        dim_instance_id,
        COUNT(*) AS event_count
    FROM mart_usage_event
        GROUP BY 1,2,3,4,5
), results AS (

    SELECT *
    FROM usage_events

)


{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper-acp",
    updated_by="@icooper-acp",
    created_date="2022-02-15",
    updated_date="2022-02-16"
) }}
