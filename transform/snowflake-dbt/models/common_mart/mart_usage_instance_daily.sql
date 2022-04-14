{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart_usage_event', 'mart_usage_event'),
    ])
}},

mart_usage_instance_daily AS (
    
  SELECT
    {{ dbt_utils.surrogate_key(['event_date', 'event_name']) }} AS mart_usage_instance_id,
    event_date,
    event_name,
    data_source,
    COUNT(*) AS event_count,
    COUNT(DISTINCT(dim_user_id)) AS user_count,
    COUNT(DISTINCT(dim_ultimate_parent_namespace_id)) AS ultimate_parent_namespace_count
  FROM mart_usage_event
  WHERE dim_user_id IS NOT NULL
  {{ dbt_utils.group_by(n=4) }}
  
)

{{ dbt_audit(
    cte_ref="mart_usage_instance_daily",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-02-15",
    updated_date="2022-04-09"
) }}
