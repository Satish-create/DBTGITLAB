{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric') }}

, final AS (
    
    SELECT *
    FROM sorted
    WHERE DATE_TRUNC(MONTH, sorted.ping_created_at::DATE) >= DATEADD(MONTH, -24, DATE_TRUNC(MONTH,CURRENT_DATE))
        
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-03-11",
    updated_date="2022-07-18"
) }}

