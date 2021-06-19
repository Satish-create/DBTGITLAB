{{config({
    "schema": "common_mart_marketing"
  })
}}

{{ simple_cte([
    ('dim_crm_person','dim_crm_person'),
    ('dim_bizible_marketing_channel_path','dim_bizible_marketing_channel_path'),
    ('dim_sales_segment','dim_sales_segment'),
    ('fct_crm_person','fct_crm_person'),
    ('dim_date','dim_date')
]) }}

, final AS (

    SELECT
      fct_crm_person.dim_crm_person_id,
      mql_date_first.date_id                   AS mql_date_first_id,
      mql_date_first.date_day                  AS mql_date_first,
      mql_date_first_pt.date_day               AS mql_date_first_pt,
      mql_date_first.first_day_of_month        AS mql_month_first,
      mql_date_first_pt.first_day_of_month     AS mql_month_first_pt,
      mql_date_latest.date_day                 AS mql_date_lastest,
      mql_date_latest_pt.date_day              AS mql_date_lastest_pt,
      mql_date_latest.first_day_of_month       AS mql_month_latest,
      mql_date_latest_pt.first_day_of_month    AS mql_month_latest_pt,
      created_date.date_day                    AS created_date,
      created_date_pt.date_day                 AS created_date_pt,
      created_date.first_day_of_month          AS created_month,
      created_date_pt.first_day_of_month       AS created_month_pt,
      inquiry_date.date_day                    AS inquiry_date,
      inquiry_date_pt.date_day                 AS inquiry_date_pt,
      inquiry_date.first_day_of_month          AS inquiry_month,
      inquiry_date_pt.first_day_of_month       AS inquiry_month_pt,
      accepted_date.date_day                   AS accepted_date,
      accepted_date_pt.date_day                AS accepted_date_pt,
      accepted_date.first_day_of_month         AS accepted_month,
      accepted_date_pt.first_day_of_month      AS accepted_month_pt,
      qualifying_date.date_day                 AS qualifying_date,
      qualifying_date_pt.date_day              AS qualifying_date_pt,
      qualifying_date.first_day_of_month       AS qualifying_month,
      qualifying_date_pt.first_day_of_month    AS qualifying_month_pt,
      qualified_date.date_day                  AS qualified_date,
      qualified_date_pt.date_day               AS qualified_date_pt,
      qualified_date.first_day_of_month        AS qualified_month,
      qualified_date_pt.first_day_of_month     AS qualified_month_pt,
      converted_date.date_day                  AS converted_date,
      converted_date_pt.date_day               AS converted_date_pt,
      converted_date.first_day_of_month        AS converted_month,
      converted_date_pt.first_day_of_month     AS converted_month_pt,
      dim_crm_person.email_domain,
      dim_crm_person.email_domain_type,
      dim_crm_person.email_hash,
      dim_crm_person.status,
      dim_crm_person.lead_source,
      dim_crm_person.source_buckets,
      dim_bizible_marketing_channel_path.bizible_marketing_channel_path_name,
      dim_sales_segment.sales_segment_name,
      dim_sales_segment.sales_segment_grouped,
      CASE
        WHEN dim_sales_segment.sales_segment_name NOT IN ('Large', 'PubSec') THEN dim_sales_segment.sales_segment_name
        WHEN dim_sales_segment.sales_segment_name IN ('Large', 'PubSec') THEN  'Large MQLs & Trials'
        ELSE 'Missing sales_segment_region_mapped'
      END                                      AS sales_segment_region_mapped,
      fct_crm_person.is_mql,
      CASE
        WHEN LOWER(dim_crm_person.lead_source) LIKE '%trial - gitlab.com%' THEN TRUE
        WHEN LOWER(dim_crm_person.lead_source) LIKE '%trial - enterprise%' THEN TRUE
        ELSE FALSE
      END                                                        AS is_lead_source_trial
    FROM fct_crm_person
    LEFT JOIN dim_crm_person
      ON fct_crm_person.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN dim_sales_segment
      ON fct_crm_person.dim_account_sales_segment_id = dim_sales_segment.dim_sales_segment_id
    LEFT JOIN dim_bizible_marketing_channel_path
      ON fct_crm_person.dim_bizible_marketing_channel_path_id = dim_bizible_marketing_channel_path.dim_bizible_marketing_channel_path_id
    LEFT JOIN dim_date AS created_date
      ON fct_crm_person.created_date_id = created_date.date_id
    LEFT JOIN dim_date AS created_date_pt
      ON fct_crm_person.created_date_pt_id = created_date_pt.date_id
    LEFT JOIN dim_date AS inquiry_date
      ON fct_crm_person.inquiry_date_id = inquiry_date.date_id
    LEFT JOIN dim_date AS inquiry_date_pt
      ON fct_crm_person.inquiry_date_pt_id = inquiry_date_pt.date_id
    LEFT JOIN dim_date AS mql_date_first
      ON fct_crm_person.mql_date_first_id = mql_date_first.date_id
    LEFT JOIN dim_date AS mql_date_first_pt
      ON fct_crm_person.mql_date_first_pt_id = mql_date_first_pt.date_id
    LEFT JOIN dim_date AS mql_date_latest
      ON fct_crm_person.mql_date_latest_id = mql_date_latest.date_id
    LEFT JOIN dim_date AS mql_date_latest_pt
      ON fct_crm_person.mql_date_latest_pt_id = mql_date_latest_pt.date_id
    LEFT JOIN dim_date AS accepted_date
      ON fct_crm_person.accepted_date_id = accepted_date.date_id
    LEFT JOIN dim_date AS accepted_date_pt
      ON fct_crm_person.accepted_date_pt_id = accepted_date_pt.date_id
    LEFT JOIN dim_date AS qualified_date
      ON fct_crm_person.qualified_date_id = qualified_date.date_id
    LEFT JOIN dim_date AS qualified_date_pt
      ON fct_crm_person.qualified_date_pt_id = qualified_date_pt.date_id
    LEFT JOIN dim_date AS qualifying_date
      ON fct_crm_person.qualifying_date_id = qualifying_date.date_id
    LEFT JOIN dim_date AS qualifying_date_pt
      ON fct_crm_person.qualifying_date_pt_id = qualifying_date_pt.date_id
    LEFT JOIN dim_date converted_date
      ON fct_crm_person.converted_date_id = converted_date.date_id
    LEFT JOIN dim_date converted_date_pt
      ON fct_crm_person.converted_date_pt_id = converted_date_pt.date_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@jpeguero",
    created_date="2020-12-07",
    updated_date="2021-06-19",
  ) }}
