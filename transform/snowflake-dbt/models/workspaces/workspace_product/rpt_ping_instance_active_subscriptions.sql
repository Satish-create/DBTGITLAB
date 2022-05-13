{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('metric_opt_in', 'rpt_ping_counter_statistics'),
    ('mart_charge', 'mart_charge'),
    ('dim_charge', 'dim_charge'),
    ('fct_charge', 'fct_charge'),
    ('dim_subscription', 'dim_subscription'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_date', 'dim_date'),
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly')
    ])

}}

/*
Determine latest version for each subscription to determine if the potential metric is valid for a given month
*/

, subscriptions_w_versions AS (

  SELECT
      ping_created_at_month             AS ping_created_at_month,
      latest_active_subscription_id     AS latest_active_subscription_id,
      ping_edition                      AS ping_edition,
      major_minor_version               AS major_minor_version,
      instance_user_count               AS instance_user_count
  FROM mart_ping_instance_metric_monthly
      WHERE time_frame = '28d'
        AND ping_delivery_type = 'Self-Managed'
      QUALIFY ROW_NUMBER() OVER (
            PARTITION BY ping_created_at_month, latest_active_subscription_id
              ORDER BY major_minor_version_id DESC) = 1

/*
Deduping the mart to ensure instance_user_count isn't counted 2+ times
*/

), deduped_subscriptions_w_versions AS (

    SELECT
        ping_created_at_month             AS ping_created_at_month,
        latest_active_subscription_id     AS latest_active_subscription_id,
        ping_edition                      AS ping_edition,
        major_minor_version               AS major_minor_version,
        MAX(instance_user_count)          AS instance_user_count
    FROM mart_ping_instance_metric_monthly
      {{ dbt_utils.group_by(n=4)}}
/*
Get the count of pings each month per subscription_name_slugify
*/

), ping_counts AS (

  SELECT
    ping_created_at_month                       AS ping_created_at_month,
    latest_active_subscription_id               AS latest_active_subscription_id,
    COUNT(DISTINCT(dim_ping_instance_id))       AS count_of_pings
  FROM mart_ping_instance_metric_monthly
      {{ dbt_utils.group_by(n=2)}}

/*
Join subscription information with count of pings
*/

), joined_subscriptions AS (

  SELECT
    deduped_subscriptions_w_versions.*,
    ping_counts.count_of_pings
  FROM deduped_subscriptions_w_versions
    INNER JOIN ping_counts
  ON deduped_subscriptions_w_versions.ping_created_at_month = ping_counts.ping_created_at_month
    AND deduped_subscriptions_w_versions.latest_active_subscription_id = ping_counts.latest_active_subscription_id

/*
Aggregate mart_charge information (used as the basis of truth), this gets rid of host deviation
*/

), mart_charge_cleaned AS (

  SELECT
       dim_date.date_actual               AS arr_month,
       fct_charge.dim_subscription_id     AS dim_subscription_id,
       SUM(mrr)                           AS mrr,
       SUM(arr)                           AS arr,
       SUM(quantity)                      AS licensed_user_count
  FROM fct_charge
  INNER JOIN dim_date
    ON effective_start_month <= dim_date.date_actual
    AND (effective_end_month > dim_date.date_actual OR effective_end_month IS NULL)
    AND dim_date.day_of_month = 1
  INNER JOIN dim_charge
    ON fct_charge.dim_charge_id = dim_charge.dim_charge_id
  INNER JOIN dim_subscription
    ON fct_charge.dim_subscription_id = dim_subscription.dim_subscription_id
  INNER JOIN dim_billing_account
    ON fct_charge.dim_billing_account_id = dim_billing_account.dim_billing_account_id
  LEFT JOIN dim_crm_account
    ON dim_crm_account.dim_crm_account_id = dim_billing_account.dim_crm_account_id
  WHERE subscription_status IN ('Active','Cancelled')
    ---AND --add exclude the storage
    --AND --filter to only self-managed
    --AND dim_crm_account.is_jihu_account != 'TRUE'
      {{ dbt_utils.group_by(n=2)}}

/*
Join mart_charge information bringing in mart_charge subscriptions which DO NOT appear in ping fact data
*/

), arr_counts_joined AS (

  SELECT
    {{ dbt_utils.surrogate_key(['ping_created_at_month', 'latest_active_subscription_id']) }}               AS rpt_ping_instance_active_subscriptions_id,
    mart_charge_cleaned.arr_month                                                                           AS ping_created_at_month,
    mart_charge_cleaned.dim_subscription_id                                                                 AS latest_active_subscription_id,
    joined_subscriptions.ping_edition                                                                       AS ping_edition,
    joined_subscriptions.major_minor_version                                                                AS major_minor_version,
    joined_subscriptions.instance_user_count                                                                AS instance_user_count,
    mart_charge_cleaned.licensed_user_count                                                                 AS licensed_user_count,
    mart_charge_cleaned.arr                                                                                 AS arr,
    mart_charge_cleaned.mrr                                                                                 AS mrr,
    joined_subscriptions.count_of_pings                                                                     AS has_sent_pings,
    IFF(ping_edition IS NULL, FALSE, TRUE)                                                                  AS reported_flag
  FROM mart_charge_cleaned
    LEFT OUTER JOIN joined_subscriptions
  ON joined_subscriptions.latest_active_subscription_id = mart_charge_cleaned.dim_subscription_id
      AND joined_subscriptions.ping_created_at_month = mart_charge_cleaned.arr_month

)

 {{ dbt_audit(
     cte_ref="arr_counts_joined",
     created_by="@icooper-acp",
     updated_by="@icooper-acp",
     created_date="2022-05-05",
     updated_date="2022-05-05"
 ) }}