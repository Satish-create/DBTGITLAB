WITH date_table AS (

    SELECT *
    FROM {{ ref('date_details') }}
    WHERE day_of_month = 1

), sfdc_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}
    WHERE is_deleted = FALSE

), sfdc_deleted_accounts AS (

    SELECT *
    FROM {{ ref('sfdc_deleted_accounts') }}
    WHERE is_deleted = FALSE

), zuora_accounts AS (

    SELECT *
    FROM {{ ref('zuora_account_source') }}
    WHERE is_deleted = FALSE

), zuora_invoices AS (

    SELECT *
    FROM {{ ref('zuora_invoice_charges') }}

), zuora_product AS (

    SELECT *
    FROM {{ ref('zuora_product_source') }}
    WHERE is_deleted = FALSE

), zuora_product_rp AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_source') }}
    WHERE is_deleted = FALSE

), zuora_product_rpc AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_source') }}
    WHERE is_deleted = FALSE

), zuora_product_rpct AS (

    SELECT *
    FROM {{ ref('zuora_product_rate_plan_charge_tier_source') }}
    WHERE is_deleted = FALSE

), initial_join_to_sfdc AS (

  SELECT
    invoice_number,
    zuora_accounts.crm_id                                AS invoice_crm_id,
    sfdc_accounts.account_id                             AS sfdc_account_id_int,
    zuora_accounts.account_name,
    invoice_date,
    DATE_TRUNC('month',invoice_date)                     AS invoice_month,
    product_name,
    {{ product_category('rate_plan_name') }},
    {{ delivery('product_category')}},
    rate_plan_name,
    invoice_item_unit_price,
    quantity                                             AS quantity,
    invoice_item_charge_amount                           AS invoice_item_charge_amount
  FROM zuora_invoices
  LEFT JOIN zuora_accounts
    ON zuora_invoices.invoice_account_id = zuora_accounts.account_id
  LEFT JOIN sfdc_accounts
    ON zuora_accounts.crm_id = sfdc_accounts.account_id
  WHERE invoice_item_charge_amount != 0

), replace_sfdc_account_id_with_master_record_id AS (

    SELECT
      COALESCE(initial_join_to_sfdc.sfdc_account_id_int, sfdc_master_record_id) AS sfdc_account_id,
      initial_join_to_sfdc.*
    FROM initial_join_to_sfdc
    LEFT JOIN sfdc_deleted_accounts
      ON initial_join_to_sfdc.invoice_crm_id = sfdc_deleted_accounts.sfdc_account_id

), joined AS (

    SELECT
      invoice_number,
      sfdc_account_id,
      CASE
        WHEN ultimate_parent_account_segment = 'Unknown' THEN 'SMB'
        WHEN ultimate_parent_account_segment = '' THEN 'SMB'
        ELSE ultimate_parent_account_segment
      END                                     AS ultimate_parent_segment,
      replace_account_id.account_name,
      invoice_date,
      invoice_month,
      product_name,
      product_category,
      account_type,
      rate_plan_name,
      invoice_item_unit_price,
      quantity                                AS quantity,
      invoice_item_charge_amount              AS invoice_item_charge_amount
    FROM replace_sfdc_account_id_with_master_record_id replace_account_id
    LEFT JOIN sfdc_accounts
      ON a.sfdc_account_id = sfdc_accounts.account_id

), list_price AS (

  SELECT
    zuora_product_rp.product_rate_plan_name,
    zuora_product_rpc.product_rate_plan_charge_name,
    MIN(zuora_product_rpct.price)             AS billing_list_price
  FROM zuora_product
  INNER JOIN zuora_product_rp
    ON zuora_product.product_id = zuora_product_rp.product_id
  INNER JOIN zuora_product_rpc
    ON zuora_product_rp.product_rate_plan_id = zuora_product_rpc.product_rate_plan_id
  INNER JOIN zuora_product_rpct
    ON zuora_product_rpc.product_rate_plan_charge_id = zuora_product_rpct.product_rate_plan_charge_id
  WHERE zuora_product.effective_start_date <= CURRENT_DATE
    AND zuora_product.effective_end_date > CURRENT_DATE
    AND zuora_product_rpct.currency = 'USD'
    AND zuora_product_rpct.price != 0
  GROUP BY 1,2
  ORDER BY 1,2

), date_details AS (

  SELECT *
  FROM date_table

)

SELECT
  joined.invoice_number,
  sfdc_account_id,
  account_name,
  account_type,
  invoice_date,
  joined.product_name,
  joined.rate_plan_name,
  quantity,
  invoice_item_unit_price,
  invoice_item_charge_amount,
  CASE
    WHEN lower(rate_plan_name) LIKE '%2 years%' THEN (invoice_item_unit_price/2)
    WHEN lower(rate_plan_name) LIKE '%2 year%'  THEN (invoice_item_unit_price/2)
    WHEN lower(rate_plan_name) LIKE '%3 years%' THEN (invoice_item_unit_price/3)
    WHEN lower(rate_plan_name) LIKE '%3 year%'  THEN (invoice_item_unit_price/3)
    WHEN lower(rate_plan_name) LIKE '%4 years%' THEN (invoice_item_unit_price/4)
    WHEN lower(rate_plan_name) LIKE '%4 year%'  THEN (invoice_item_unit_price/4)
    WHEN lower(rate_plan_name) LIKE '%5 years%' THEN (invoice_item_unit_price/5)
    WHEN lower(rate_plan_name) LIKE '%5 year%'  THEN (invoice_item_unit_price/5)
    ELSE invoice_item_unit_price
  END                                           AS annual_price,
  quantity * annual_price                       AS quantity_times_annual,
  ultimate_parent_segment,
  product_category,
  invoice_month,
  fiscal_quarter_name_fy                        AS fiscal_period,
  CASE
    WHEN lower(rate_plan_name) LIKE '%2 years%' THEN (billing_list_price/2)
    WHEN lower(rate_plan_name) LIKE '%2 year%'  THEN (billing_list_price/2)
    WHEN lower(rate_plan_name) LIKE '%3 years%' THEN (billing_list_price/3)
    WHEN lower(rate_plan_name) LIKE '%3 year%'  THEN (billing_list_price/3)
    WHEN lower(rate_plan_name) LIKE '%4 years%' THEN (billing_list_price/4)
    WHEN lower(rate_plan_name) LIKE '%4 year%'  THEN (billing_list_price/4)
    WHEN lower(rate_plan_name) LIKE '%5 years%' THEN (billing_list_price/5)
    WHEN lower(rate_plan_name) LIKE '%5 year%'  THEN (billing_list_price/5)
    ELSE billing_list_price
  END                                           AS list_price,
  CASE
    WHEN annual_price = list_price THEN 0
    ELSE ((annual_price - list_price)/list_price) * -1
  END                                                       AS discount,
  quantity * list_price                                     AS list_price_times_quantity
FROM joined
LEFT JOIN list_price
  ON joined.rate_plan_name = list_price.product_rate_plan_name
LEFT JOIN date_details
  ON joined.invoice_month = date_details.date_actual
ORDER BY invoice_date, invoice_number
