WITH crm_account AS (

    SELECT
     {{ dbt_utils.surrogate_key(['retention_month','salesforce_account_id']) }}  AS retention_id,
      salesforce_account_id                           as dim_crm_account_id,
      NULL                                            as dim_subscription_id,
      'crm_account'                                   as retention_type,
      arr_segmentation                                as arr_segmentation,
      churn_type                                      as churn_type,
      gross_retention_mrr                             as gross_retention_mrr,
      net_retention_mrr                               as net_retention_mrr,
      original_mrr                                    as original_mrr,
      retention_month                                 as retention_month,
      months_since_sfdc_account_cohort_start          as months_since_cohort_start,
      sfdc_account_cohort_month                       as cohort_month,
      sfdc_account_cohort_quarter                     as cohort_quarter,
      quarters_since_sfdc_account_cohort_start        as quarters_since_cohort_start
    FROM {{ ref ('retention_sfdc_account_') }}

), subscription AS (

    SELECT
      {{ dbt_utils.surrogate_key(['retention_month', 'zuora_subscription_id']) }}  AS retention_id,
      salesforce_account_id                           as dim_crm_account_id,
      zuora_subscription_id                           as dim_subscription_id,
      'dim_subscription'                              as retention_type,
      arr_segmentation                                as arr_segmentation,
      churn_type                                      as churn_type,
      gross_retention_mrr                             as gross_retention_mrr,
      net_retention_mrr                               as net_retention_mrr,
      original_mrr                                    as original_mrr,
      retention_month                                 as retention_month,
      months_since_zuora_subscription_cohort_start    as months_since_cohort_start,
      zuora_subscription_cohort_month                 as cohort_month,
      zuora_subscription_cohort_quarter               as cohort_quarter,
      quarters_since_zuora_subscription_cohort_start  as quarters_since_cohort_start
    FROM {{ ref('retention_zuora_subscription_') }}

), parent_crm_account AS (

    SELECT
      {{ dbt_utils.surrogate_key(['retention_month','salesforce_account_id']) }}  AS retention_id,
      salesforce_account_id                           as dim_crm_account_id,
      NULL                                            as dim_subscription_id,
      'parent_crm_account'                            as retention_type,
      arr_segmentation                                as arr_segmentation,
      churn_type                                      as churn_type,
      gross_retention_mrr                             as gross_retention_mrr,
      net_retention_mrr                               as net_retention_mrr,
      original_mrr                                    as original_mrr,
      retention_month                                 as retention_month,
      months_since_parent_account_cohort_start        as months_since_cohort_start,
      parent_account_cohort_month                     as cohort_month,
      parent_account_cohort_quarter                   as cohort_quarter,
      quarters_since_parent_account_cohort_start      as quarters_since_cohort_start
    FROM {{ ref('retention_parent_account_') }}

), final AS (

    SELECT *
    FROM crm_account

    UNION

    SELECT parent_crm_account.*
    FROM parent_crm_account
    LEFT JOIN crm_account
      -- Exclude duplicates
      ON crm_account.retention_id = parent_crm_account.retention_id
      AND crm_account.retention_month = parent_crm_account.retention_month
      AND crm_account.gross_retention_mrr = parent_crm_account.gross_retention_mrr
    WHERE crm_account.retention_id IS NULL

    UNION

    SELECT *
    FROM subscription

)




{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2021-05-22",
    updated_date="2021-05-22"
) }}