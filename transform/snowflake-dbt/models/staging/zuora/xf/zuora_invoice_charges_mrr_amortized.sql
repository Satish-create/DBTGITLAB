WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

), zuora_account AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}

), zuora_contact AS (

    SELECT *
    FROM {{ ref('zuora_contact') }}

), zuora_invoice_charges AS (

    SELECT *
    FROM {{ ref('zuora_invoice_charges') }}

), zuora_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), base_mrr AS (

    SELECT
      --keys
      zuora_invoice_charges.rate_plan_charge_id,

      --account info
      zuora_invoice_charges.account_id,
      zuora_contact.country,
      zuora_account.account_name,
      zuora_account.account_number,
      zuora_invoice_charges.crm_id,

      --subscription info
      zuora_invoice_charges.subscription_id,
      zuora_invoice_charges.subscription_name_slugify,
      zuora_invoice_charges.subscription_status,

      --rate_plan info
      zuora_invoice_charges.rate_plan_charge_name,
      zuora_invoice_charges.rate_plan_charge_number,
      zuora_invoice_charges.rate_plan_name,
      zuora_invoice_charges.unit_of_measure,
      zuora_invoice_charges.quantity,
      zuora_invoice_charges.mrr,
      zuora_invoice_charges.product_name,

      --date info
      DATE_TRUNC('month', zuora_subscription.subscription_start_date::DATE)   AS sub_start_month,
      DATE_TRUNC('month', zuora_subscription.subscription_end_date::DATE)     AS sub_end_month,
      DATE_TRUNC('month', zuora_invoice_charges.effective_start_date::DATE)   AS effective_start_month,
      DATE_TRUNC('month', zuora_invoice_charges.effective_end_date::DATE)     AS effective_end_month,
      zuora_invoice_charges.effective_start_date,
      zuora_invoice_charges.effective_end_date
    FROM zuora_invoice_charges
    LEFT JOIN zuora_account
      ON zuora_invoice_charges.account_id = zuora_account.account_id
    LEFT JOIN zuora_subscription
      ON zuora_invoice_charges.subscription_id = zuora_subscription.subscription_id
    LEFT JOIN zuora_contact
      ON COALESCE(zuora_account.sold_to_contact_id, zuora_account.bill_to_contact_id) = zuora_contact.contact_id
    WHERE zuora_invoice_charges.is_last_segment_version = TRUE

), month_base_mrr AS (

    SELECT
      date_actual                               AS mrr_month,
      crm_id,
      account_id,
      country,
      account_name,
      account_number,
      subscription_id,
      subscription_name_slugify,
      subscription_status,
      sub_start_month,
      sub_end_month,
      effective_start_month,
      effective_end_month,
      product_name,
      rate_plan_charge_name,
      rate_plan_name,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category') }},
      CASE
        WHEN lower(rate_plan_name) like '%support%'
          THEN 'Support Only'
        ELSE 'Full Service'
      END                                       AS service_type,
      unit_of_measure,
      SUM(mrr)                                  AS mrr,
      SUM(quantity)                             AS quantity
    FROM base_mrr
    INNER JOIN date_table
      ON base_mrr.effective_start_date <= date_table.date_actual
      AND (base_mrr.effective_end_date > date_table.date_actual
           OR base_mrr.effective_end_date IS NULL)
    {{ dbt_utils.group_by(n=20) }}

)

SELECT
  dateadd('month',-1,mrr_month)                 AS mrr_month,
  crm_id,
  account_id,
  country,
  account_name,
  account_number,
  subscription_id,
  subscription_name_slugify,
  subscription_status,
  sub_start_month,
  sub_end_month,
  effective_start_month,
  effective_end_month,
  product_name,
  rate_plan_charge_name,
  rate_plan_name,
  product_category,
  delivery,
  service_type,
  unit_of_measure,
  mrr,
  quantity
FROM month_base_mrr
WHERE mrr != 0
ORDER BY mrr_month DESC, account_name
