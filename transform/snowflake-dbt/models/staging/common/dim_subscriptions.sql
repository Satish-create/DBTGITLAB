WITH zuora_subscription AS (
  SELECT *
  FROM {{ ref('zuora_subscription_source') }}

), zuora_subscription_snapshots AS (

  SELECT
  rank() over (partition by name order by DBT_VALID_FROM desc) as rank,
  *
  FROM {{ ref('zuora_subscription_snapshots_source') }}
  WHERE --status in ('Cancelled', 'Active')
  CURRENT_TIMESTAMP()::TIMESTAMP_TZ >= dbt_valid_from
    AND {{ coalesce_to_infinity('dbt_valid_to') }} > current_timestamp()::TIMESTAMP_TZ

), zuora_account AS (
  SELECT
    account_id,
    crm_id
  FROM {{ ref('zuora_account_source') }}
)

SELECT
  zuora_subscription.subscription_id,
  zuora_account.crm_id,
  zuora_account.account_id,
  zuora_subscription.subscription_name_slugify,
  zuora_subscription.subscription_status,
  zuora_subscription.version                                                AS subscription_version,
  zuora_subscription.auto_renew                                             AS is_auto_renew,
  zuora_subscription.zuora_renewal_subscription_name,
  zuora_subscription.zuora_renewal_subscription_name_slugify,
  zuora_subscription.renewal_term,
  zuora_subscription.renewal_term_period_type,
  zuora_subscription.subscription_start_date                                AS subscription_start_date,
  zuora_subscription.subscription_end_date                                  AS subscription_end_date,
  DATE_TRUNC('month', zuora_subscription.subscription_start_date)           AS subscription_start_month,
  DATE_TRUNC('month', zuora_subscription.subscription_end_date)             AS subscription_end_month
FROM zuora_subscription
INNER JOIN zuora_subscription_snapshots
  ON zuora_subscription_snapshots.subscription_id = zuora_subscription.subscription_id
  AND zuora_subscription_snapshots.rank = 1
INNER JOIN zuora_account
  ON zuora_account.account_id = zuora_subscription.account_id
WHERE is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')
