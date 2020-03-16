-- depends_on: {{ ref('zuora_excluded_accounts') }}

WITH zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

)

SELECT
      zuora_subscription.subscription_id,
      zuora_subscription.subscription_name_slugify,
      zuora_subscription.subscription_status,
      zuora_subscription.version                          AS subscription_version,
      zuora_subscription.auto_renew as is_auto_renew,
      zuora_subscription.zuora_renewal_subscription_name,
      zuora_subscription.zuora_renewal_subscription_name_slugify,
      zuora_subscription.renewal_term,
      zuora_subscription.renewal_term_period_type,
      zuora_subscription.quote_type,
      zuora_subscription.renewal_setting
    FROM zuora_subscription
      WHERE is_deleted = FALSE
  AND exclude_from_analysis IN ('False', '')

