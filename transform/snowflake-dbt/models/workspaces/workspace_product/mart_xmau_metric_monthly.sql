{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_namespace', 'dim_namespace'),
    ('dim_date','dim_date'),
    ('fct_usage_event', 'fct_usage_event'),
    ('xmau_metrics', 'gitlab_dotcom_xmau_metrics'),
    ])
}}

, fact_raw AS (

    SELECT
        event_id,
        CAST(EVENT_CREATED_AT AS DATE)                                                                  AS event_date,
        dim_user_id,
        event_name,
        source,
        plan_id_at_event_date,
        plan_name_at_event_date,
        plan_was_paid_at_event_date,
        dim_namespace_id,
        DATE_TRUNC('MONTH', event_date)                                                                 AS reporting_month,
        QUARTER(event_date)                                                                             AS reporting_quarter,
        YEAR(event_date)                                                                                AS reporting_year
    FROM fct_usage_event as fact

), fact_with_date_range AS (

    SELECT
        fact.event_id,
        fact.event_date,
        dim_date.last_day_of_month                                                                      AS last_day_of_month,
        dim_date.last_day_of_quarter                                                                    AS last_day_of_quarter,
        dim_date.last_day_of_fiscal_year                                                                AS last_day_of_fiscal_year,
        fact.dim_user_id,
        fact.event_name,
        fact.source,
        fact.plan_was_paid_at_event_date,
        fact.dim_namespace_id,
        fact.reporting_month,
        fact.reporting_quarter,
        fact.reporting_year
    FROM fact_raw as fact
    LEFT JOIN dim_date
        ON fact.event_date = dim_date.DATE_ACTUAL
    WHERE fact.event_date BETWEEN DATEADD('day', -27, last_day_of_month) AND last_day_of_month

), paid_flag_by_month AS (

      SELECT
          dim_namespace_id,
          reporting_month,
          plan_was_paid_at_event_date
      FROM fact_with_date_range
          QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_namespace_id, reporting_month
                                     ORDER BY reporting_month DESC) = 1

 ), fact_w_paid_deduped AS (

     SELECT
         fact_with_date_range.event_id,
         fact_with_date_range.event_date,
         fact_with_date_range.last_day_of_month,
         fact_with_date_range.last_day_of_quarter,
         fact_with_date_range.last_day_of_fiscal_year,
         fact_with_date_range.dim_user_id,
         fact_with_date_range.event_name,
         fact_with_date_range.source,
         fact_with_date_range.dim_namespace_id,
         fact_with_date_range.reporting_month,
         fact_with_date_range.reporting_quarter,
         fact_with_date_range.reporting_year,
         paid_flag_by_month.plan_was_paid_at_event_date
     FROM fact_with_date_range
         LEFT JOIN paid_flag_by_month
             ON fact_with_date_range.dim_namespace_id = paid_flag_by_month.dim_namespace_id AND fact_with_date_range.reporting_month = paid_flag_by_month.reporting_month

), total_results AS (

   SELECT
       {{ dbt_utils.surrogate_key(['reporting_month', 'event_name']) }}                               AS mart_xmau_metric_monthly_id,
       reporting_month,
       event_name,
       COUNT(*)                                                                                        AS total_event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                               AS total_namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                    AS total_user_count
   FROM fact_w_paid_deduped
      {{ dbt_utils.group_by(n=3) }}
   ORDER BY reporting_month DESC

), free_results AS (

   SELECT
       {{ dbt_utils.surrogate_key(['reporting_month', 'event_name']) }}                               AS mart_xmau_metric_monthly_id,
       reporting_month,
       event_name,
       COUNT(*)                                                                                       AS free_event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                              AS free_namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                   AS free_user_count
   FROM fact_w_paid_deduped
   WHERE plan_was_paid_at_event_date = FALSE
       {{ dbt_utils.group_by(n=3) }}
   ORDER BY reporting_month DESC

), paid_results AS (

   SELECT
       {{ dbt_utils.surrogate_key(['reporting_month', 'event_name']) }}                                AS mart_xmau_metric_monthly_id,
       reporting_month,
       event_name,
       COUNT(*)                                                                                        AS paid_event_count,
       COUNT(DISTINCT(dim_namespace_id))                                                               AS paid_namespace_count,
       COUNT(DISTINCT(dim_user_id))                                                                    AS paid_user_count
   FROM fact_w_paid_deduped
   WHERE plan_was_paid_at_event_date = TRUE
       {{ dbt_utils.group_by(n=3) }}
   ORDER BY reporting_month DESC

), final_results AS (

   SELECT
       total_results.*,
       free_results.free_event_count,
       free_results.free_namespace_count,
       free_results.free_user_count,
       paid_results.paid_event_count,
       paid_results.paid_namespace_count,
       paid_results.paid_user_count
   FROM total_results
       LEFT JOIN free_results
           ON total_results.reporting_month = free_results.reporting_month AND total_results.event_name = free_results.event_name
       LEFT JOIN paid_results
           ON total_results.reporting_month = paid_results.reporting_month AND total_results.event_name = paid_results.event_name

)

{{ dbt_audit(
    cte_ref="results",
    created_by="@icooper_acp",
    updated_by="@icooper_acp",
    created_date="2022-02-23",
    updated_date="2022-02-23"
) }}
