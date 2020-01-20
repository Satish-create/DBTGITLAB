WITH zuora_subscription AS (
  
  SELECT *
  FROM "ANALYTICS".mpeychet_scratch_staging.zuora_subscription
  
)

, zuora_account AS (
  
  SELECT *
  FROM "ANALYTICS".mpeychet_scratch_staging.zuora_account
  
)

, transform AS (

    SELECT 
      zuora_subscription.subscription_name,
      zuora_subscription.subscription_name_slugify,    
      zuora_subscription.subscription_status, 
      zuora_subscription.version,
      
      zuora_subscription.account_id, 
      zuora_account.account_number,
      zuora_account.account_name,
      subscription_start_date, 
      subscription_end_date           AS subscription_version_end_date, 
      term_end_date                   AS subscription_version_term_end_date, 
      term_start_date                 AS subscription_version_term_start_date,
      MIN(term_start_date) 
        OVER (PARTITION BY subscription_name_slugify 
              ORDER BY version DESC 
              ROWS BETWEEN UNBOUNDED PRECEDING
                     AND 1 PRECEDING) AS min_following_subscription_version_term_start_date
    FROM zuora_subscription
    LEFT JOIN zuora_account
      ON zuora_subscription.account_id = zuora_account.account_id
    
)

SELECT 
  subscription_name,
  subscription_name_slugify,    
  subscription_status, 
  version,
  account_id, 
  account_number,
  account_name,
  subscription_start_date, 
  subscription_version_end_date,
  subscription_version_term_end_date,
  subscription_version_term_start_date
FROM transform
WHERE subscription_version_term_start_date  < min_following_subscription_version_term_start_date
  OR min_following_subscription_version_term_start_date IS NULL
