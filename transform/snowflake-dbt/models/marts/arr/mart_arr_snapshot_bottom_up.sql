{{ config({
        "materialized": "incremental",
        "unique_key": "primary_key",
        "tags": ["arr_snapshots"],
        "schema": "common_mart_sales"
    })
}}

WITH dim_billing_account AS (

    SELECT *
    FROM {{ ref('dim_billing_account') }}

), dim_crm_account AS (

    SELECT *
    FROM {{ ref('dim_crm_account') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), dim_product_detail AS (

    SELECT *
    FROM {{ ref('dim_product_detail') }}

), dim_subscription AS (

    SELECT *
    FROM {{ ref('dim_subscription') }}

), fct_mrr_snapshot_bottom_up AS (

     SELECT
      mrr_snapshot_id,
      mrr_id,
      snapshot_id,
      dim_date_id,
      dim_subscription_id,
      dim_product_detail_id,
      dim_billing_account_id,
      dim_crm_account_id,
      SUM(mrr)                                                               AS mrr,
      SUM(arr)                                                               AS arr,
      SUM(quantity)                                                          AS quantity,
      ARRAY_AGG(unit_of_measure)                                             AS unit_of_measure
    FROM {{ ref('fct_mrr_snapshot_bottom_up') }}
    WHERE LOWER(subscription_status) NOT IN ('draft', 'expired')

    {% if is_incremental() %}

    -- this filter will only be applied on an incremental run
      AND snapshot_id > (SELECT max(dim_date.date_id)
                         FROM {{ this }}
                         INNER JOIN dim_date
                           ON dim_date.date_actual = snapshot_date
                            )

    {% endif %}

    {{ dbt_utils.group_by(n=8) }}

), joined AS (

    SELECT
      --keys
      fct_mrr_snapshot_bottom_up.mrr_snapshot_id                                            AS primary_key,
      fct_mrr_snapshot_bottom_up.mrr_id,

      --date info
      snapshot_dates.date_actual                                                            AS snapshot_date,
      arr_month.date_actual                                                                 AS arr_month,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_quarter, arr_month.fiscal_quarter_name_fy, NULL)
                                                                                            AS fiscal_quarter_name_fy,
      IFF(arr_month.is_first_day_of_last_month_of_fiscal_year, arr_month.fiscal_year, NULL)
                                                                                            AS fiscal_year,
      dim_subscription.term_start_date                                                      AS term_start_date,
      dim_subscription.term_end_date                                                        AS term_end_date,
      dim_subscription.subscription_start_month                                             AS subscription_start_month,
      dim_subscription.subscription_end_month                                               AS subscription_end_month,
      dim_subscription.subscription_start_date                                              AS subscription_start_date,
      dim_subscription.subscription_end_date                                                AS subscription_end_date,

      --billing account info
      dim_billing_account.dim_billing_account_id                                            AS dim_billing_account_id,
      dim_billing_account.sold_to_country                                                   AS sold_to_country,
      dim_billing_account.billing_account_name                                              AS billing_account_name,
      dim_billing_account.billing_account_number                                            AS billing_account_number,

      -- crm account info
      dim_crm_account.dim_crm_account_id                                                    AS dim_crm_account_id,
      dim_crm_account.crm_account_name                                                      AS crm_account_name,
      dim_crm_account.dim_parent_crm_account_id                                             AS dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name                                               AS parent_crm_account_name,
      dim_crm_account.parent_crm_account_billing_country                                    AS parent_crm_account_billing_country,
      dim_crm_account.parent_crm_account_sales_segment                                      AS parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry                                           AS parent_crm_account_industry,
      dim_crm_account.parent_crm_account_owner_team                                         AS parent_crm_account_owner_team,
      dim_crm_account.parent_crm_account_sales_territory                                    AS parent_crm_account_sales_territory,
      dim_crm_account.parent_crm_account_tsp_region                                         AS parent_crm_account_tsp_region,
      dim_crm_account.parent_crm_account_tsp_sub_region                                     AS parent_crm_account_tsp_sub_region,
      dim_crm_account.parent_crm_account_tsp_area                                           AS parent_crm_account_tsp_area,
      dim_crm_account.parent_crm_account_tsp_account_employees                              AS parent_crm_account_tsp_account_employees,
      dim_crm_account.parent_crm_account_tsp_max_family_employees                           AS parent_crm_account_tsp_max_family_employees,
      dim_crm_account.parent_crm_account_employee_count_band                                AS parent_crm_account_employee_count_band,
      dim_crm_account.crm_account_tsp_region                                                AS crm_account_tsp_region,
      dim_crm_account.crm_account_tsp_sub_region                                            AS crm_account_tsp_sub_region,
      dim_crm_account.crm_account_tsp_area                                                  AS crm_account_tsp_area,
      dim_crm_account.health_score                                                          AS health_score,
      dim_crm_account.health_score_color                                                    AS health_score_color,
      dim_crm_account.health_number                                                         AS health_number,
      dim_crm_account.is_jihu_account                                                       AS is_jihu_account,

      --subscription info
      dim_subscription.dim_subscription_id                                                  AS dim_subscription_id,
      dim_subscription.dim_subscription_id_original                                         AS dim_subscription_id_original,
      dim_subscription.subscription_status                                                  AS subscription_status,
      dim_subscription.subscription_sales_type                                              AS subscription_sales_type,
      dim_subscription.subscription_name                                                    AS subscription_name,
      dim_subscription.subscription_name_slugify                                            AS subscription_name_slugify,
      dim_subscription.oldest_subscription_in_cohort                                        AS oldest_subscription_in_cohort,
      dim_subscription.subscription_lineage                                                 AS subscription_lineage,
      dim_subscription.subscription_cohort_month                                            AS subscription_cohort_month,
      dim_subscription.subscription_cohort_quarter                                          AS subscription_cohort_quarter,
      MIN(arr_month.date_actual) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                          AS billing_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_billing_account.dim_billing_account_id)                          AS billing_account_cohort_quarter,
      MIN(arr_month.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                                  AS crm_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_crm_account_id)                                  AS crm_account_cohort_quarter,
      MIN(arr_month.date_actual) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                           AS parent_account_cohort_month,
      MIN(dim_subscription.subscription_cohort_quarter) OVER (
          PARTITION BY dim_crm_account.dim_parent_crm_account_id)                           AS parent_account_cohort_quarter,
      dim_subscription.turn_on_cloud_licensing                                              AS turn_on_cloud_licensing,
      dim_subscription.turn_on_operational_metrics                                          AS turn_on_operational_metrics,
      dim_subscription.contract_operational_metrics                                         AS contract_operational_metrics,
      dim_subscription.contract_auto_renewal                                                AS contract_auto_renewal,
      dim_subscription.turn_on_auto_renewal                                                 AS turn_on_auto_renewal,
      dim_subscription.contract_seat_reconciliation                                         AS contract_seat_reconciliation,
      dim_subscription.turn_on_seat_reconciliation                                          AS turn_on_seat_reconciliation,

      --product info
      dim_product_detail.dim_product_detail_id                                              AS dim_product_detail_id,
      dim_product_detail.product_tier_name                                                  AS product_tier_name,
      dim_product_detail.product_delivery_type                                              AS product_delivery_type,
      dim_product_detail.service_type                                                       AS service_type,
      dim_product_detail.product_rate_plan_name                                             AS product_rate_plan_name,

      --charge information
      fct_mrr_snapshot_bottom_up.unit_of_measure                                            AS unit_of_measure,
      fct_mrr_snapshot_bottom_up.mrr                                                        AS mrr,
      fct_mrr_snapshot_bottom_up.arr                                                        AS arr,
      fct_mrr_snapshot_bottom_up.quantity                                                   AS quantity
    FROM fct_mrr_snapshot_bottom_up
    INNER JOIN dim_subscription
      ON dim_subscription.dim_subscription_id = fct_mrr_snapshot_bottom_up.dim_subscription_id
    INNER JOIN dim_product_detail
      ON dim_product_detail.dim_product_detail_id = fct_mrr_snapshot_bottom_up.dim_product_detail_id
    INNER JOIN dim_billing_account
      ON dim_billing_account.dim_billing_account_id = fct_mrr_snapshot_bottom_up.dim_billing_account_id
    LEFT JOIN dim_date AS arr_month
      ON arr_month.date_id = fct_mrr_snapshot_bottom_up.dim_date_id
    LEFT JOIN dim_date AS snapshot_dates
      ON snapshot_dates.date_id = fct_mrr_snapshot_bottom_up.snapshot_id
    LEFT JOIN dim_crm_account
      ON dim_billing_account.dim_crm_account_id = dim_crm_account.dim_crm_account_id

), cohort_diffs AS (

  SELECT
    joined.*,
    DATEDIFF(month, billing_account_cohort_month, arr_month)     AS months_since_billing_account_cohort_start,
    DATEDIFF(quarter, billing_account_cohort_quarter, arr_month) AS quarters_since_billing_account_cohort_start,
    DATEDIFF(month, crm_account_cohort_month, arr_month)         AS months_since_crm_account_cohort_start,
    DATEDIFF(quarter, crm_account_cohort_quarter, arr_month)     AS quarters_since_crm_account_cohort_start,
    DATEDIFF(month, parent_account_cohort_month, arr_month)      AS months_since_parent_account_cohort_start,
    DATEDIFF(quarter, parent_account_cohort_quarter, arr_month)  AS quarters_since_parent_account_cohort_start,
    DATEDIFF(month, subscription_cohort_month, arr_month)        AS months_since_subscription_cohort_start,
    DATEDIFF(quarter, subscription_cohort_quarter, arr_month)    AS quarters_since_subscription_cohort_start
  FROM joined

), parent_arr AS (

    SELECT
      snapshot_date,
      arr_month,
      dim_parent_crm_account_id,
      SUM(arr)                                   AS arr
    FROM joined
    {{ dbt_utils.group_by(n=3) }}

), parent_arr_band_calc AS (

    SELECT
      snapshot_date,
      arr_month,
      dim_parent_crm_account_id,
      CASE
        WHEN arr > 5000 THEN 'ARR > $5K'
        WHEN arr <= 5000 THEN 'ARR <= $5K'
      END                                        AS arr_band_calc
    FROM parent_arr

), final AS (

    SELECT
      cohort_diffs.*,
      arr_band_calc
    FROM cohort_diffs
    LEFT JOIN parent_arr_band_calc
      ON cohort_diffs.snapshot_date = parent_arr_band_calc.snapshot_date
      AND cohort_diffs.arr_month = parent_arr_band_calc.arr_month
      AND cohort_diffs.dim_parent_crm_account_id = parent_arr_band_calc.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-07-29",
    updated_date="2021-07-29"
) }}
