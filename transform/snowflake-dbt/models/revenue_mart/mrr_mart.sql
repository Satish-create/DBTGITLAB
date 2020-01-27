WITH zuora_accts AS (

	SELECT *
  FROM {{ source('zuora', 'account') }}

), zuora_subscription AS (

	SELECT *
  FROM {{ source('zuora', 'subscription') }}

), zuora_rp AS (

  SELECT *
  FROM {{ source('zuora', 'rate_plan') }}

), zuora_rpc AS (

	SELECT *
  FROM {{ source('zuora', 'rate_plan_charge') }}

), zuora_contact AS (

  SELECT *
  FROM {{ source('zuora', 'contact') }}

), zuora_acct_period AS (

  SELECT *
  FROM {{ source('zuora', 'accounting_period') }}

), zuora_product AS (

  SELECT *
  FROM {{ source('zuora', 'product') }}

), date_table AS (

  SELECT *
  FROM {{ ref('date_details') }}
  WHERE day_of_month = 1

), base_mrr AS (

  SELECT
	  --keys
    zuora_rpc.id AS rate_plan_charge_id,

    --account info
    zuora_accts.name AS account_name,
    zuora_accts.accountnumber AS account_number,
    zuora_contact.country,
    zuora_accts.currency,

    --rate_plan info
    zuora_rp.name AS rate_plan_name,
    zuora_rpc.name AS rate_plan_charge_name,
    zuora_rpc.chargenumber AS rate_plan_charge_number,
    zuora_rpc.tcv,
    zuora_rpc.uom AS unit_of_measure,
		zuora_rpc.quantity AS quantity,
    zuora_rpc.mrr AS mrr,
    zuora_product.name AS product_name,

		--date info
    date_trunc('month', zuora_rpc.effectivestartdate::DATE) AS effective_start_month,
    date_trunc('month', zuora_rpc.effectiveenddate::DATE) AS effective_end_month,
    zuora_rpc.effectivestartdate AS effective_start_date,
    zuora_rpc.effectiveenddate AS effective_end_date
  FROM zuora_accts
  INNER JOIN zuora_subscription
    ON zuora_accts.id = zuora_subscription.accountid
  INNER JOIN zuora_rp
    ON zuora_rp.subscriptionid = zuora_subscription.id
  INNER JOIN zuora_rpc
    ON zuora_rpc.rateplanid = zuora_rp.id
  LEFT JOIN zuora_contact
    ON COALESCE(zuora_accts.soldtocontactid ,zuora_accts.billtocontactid) = zuora_contact.id
  LEFT JOIN zuora_product
    ON zuora_product.id = zuora_rpc.productid
  WHERE zuora_subscription.status NOT IN ('Draft','Expired')

), month_base_mrr AS (

  SELECT
    date_actual AS mrr_month,
		account_number,
		account_name,
		country,
		{{product_category('rate_plan_name')}},
    {{ delivery('product_category')}},
    product_name,
		rate_plan_name,
		rate_plan_charge_name,
    SUM(mrr) AS mrr,
    SUM(quantity) AS quantity
  FROM base_mrr
  LEFT JOIN date_table
    ON base_mrr.effective_start_date <= date_table.date_actual
    AND (base_mrr.effective_end_date > date_table.date_actual OR base_mrr.effective_end_date IS NULL)
  {{ dbt_utils.group_by(n=9) }}

), current_mrr AS (

   SELECT
     SUM(zuora_rpc.mrr) AS current_mrr
   FROM zuora_accts
   INNER JOIN zuora_subscription
     ON zuora_accts.id = zuora_subscription.accountid
   INNER JOIN zuora_rp
     ON zuora_rp.subscriptionid = zuora_subscription.id
   INNER JOIN zuora_rpc
     ON zuora_rpc.rateplanid = zuora_rp.id
   WHERE zuora_subscription.status NOT IN ('Draft','Expired')
     AND effectivestartdate <= current_date
     AND (effectiveenddate > current_date OR effectiveenddate IS NULL)

)

SELECT
  mrr_month,
	account_number,
 	account_name,
	country,
	product_category,
	delivery,
  product_name,
	rate_plan_name,
	rate_plan_charge_name,
  MAX(current_mrr)				AS current_mrr,
	SUM(mrr)  							AS mrr
FROM month_base_mrr
LEFT JOIN current_mrr
{{ dbt_utils.group_by(n=9) }}
