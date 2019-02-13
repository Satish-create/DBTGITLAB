WITH zuora_accts AS (

    SELECT *
    FROM {{ ref('zuora_account') }}

),

    zuora_subscriptions_xf AS (

    SELECT *
    FROM {{ ref('zuora_subscription_xf') }}

),

    zuora_rp AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan') }}

),

    zuora_rpc AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge') }}

),

    date_table AS (

     SELECT
      last_day_of_month
     FROM {{ ref('date_details') }}

),

    base_mrr AS (
    SELECT zuora_accts.account_number,
           zuora_subscriptions_xf.subscription_name,
           zuora_subscriptions_xf.subscription_name_slugify,
           zuora_subscriptions_xf.oldest_subscription_in_cohort,
           zuora_subscriptions_xf.lineage,
           zuora_rp.rate_plan_name,
           zuora_rpc.rate_plan_charge_name,
           zuora_rpc.mrr,
           zuora_rpc.tcv,
           date_trunc('month', zuora_subscriptions_xf.subscription_start_date :: date)                      AS sub_start_month,
           date_trunc('month', dateadd('month', -1, zuora_subscriptions_xf.subscription_end_date :: DATE))  AS sub_end_month,
           date_trunc('month', zuora_rpc.effective_start_date :: DATE)                                      AS effective_start_month,
           date_trunc('month', dateadd('month', -1, zuora_rpc.effective_end_date :: DATE))                  AS effective_end_month,
           datediff(month, zuora_rpc.effective_start_date :: date,
                    zuora_rpc.effective_end_date :: date)                                                   AS month_interval,
           zuora_rpc.effective_start_date,
           zuora_rpc.effective_end_date,
           zuora_subscriptions_xf.cohort_month,
           zuora_subscriptions_xf.cohort_quarter,
           zuora_rpc.unit_of_measure,
           zuora_rpc.quantity
    FROM zuora_accts
             LEFT JOIN zuora_subscriptions_xf ON zuora_accts.account_id = zuora_subscriptions_xf.account_id
             LEFT JOIN zuora_rp ON zuora_rp.subscription_id = zuora_subscriptions_xf.subscription_id
             LEFT JOIN zuora_rpc ON zuora_rpc.rate_plan_id = zuora_rp.rate_plan_id
    WHERE zuora_rpc.mrr > 0
      AND zuora_rpc.tcv > 0
    )

SELECT *
FROM base_mrr
WHERE effective_end_month >= effective_start_month
