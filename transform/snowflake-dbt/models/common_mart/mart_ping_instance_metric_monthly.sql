{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric_monthly') }}

{{ dbt_audit(
    cte_ref="sorted",
    created_by="@icooper-acp",
    updated_by="@iweeks",
    created_date="2022-03-11",
    updated_date="2022-07-18"
) }}
