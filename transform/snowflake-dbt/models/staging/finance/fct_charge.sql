WITH zuora_rate_plan AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_source') }}

), zuora_rate_plan_charge AS (

    SELECT *
    FROM {{ ref('zuora_rate_plan_charge_source') }}

), base_charges AS (

    SELECT
        zuora_rate_plan_charge.rate_plan_charge_id AS charge_id,
        zuora_rate_plan_charge.product_id,
        zuora_rate_plan.subscription_id,
        zuora_rate_plan_charge.account_id,
        zuora_rate_plan_charge.rate_plan_charge_number,
        zuora_rate_plan_charge.rate_plan_charge_name,
        zuora_rate_plan_charge.effective_start_date::DATE AS effective_start_date,
        zuora_rate_plan_charge.effective_end_date::DATE AS effective_end_date,
        date_trunc('month', zuora_rate_plan_charge.effective_start_date::DATE) AS effective_start_month,
        date_trunc('month', zuora_rate_plan_charge.effective_end_date::DATE) AS effective_end_month,
        zuora_rate_plan_charge.unit_of_measure,
        zuora_rate_plan_charge.quantity,
        zuora_rate_plan_charge.mrr,
        zuora_rate_plan_charge.delta_tcv,
        zuora_rate_plan.rate_plan_name AS rate_plan_name,
        {{product_category('zuora_rate_plan.rate_plan_name') }},
        {{delivery('product_category')}},
        zuora_rate_plan_charge.discount_level,
        zuora_rate_plan_charge.segment AS rate_plan_charge_segment,
        zuora_rate_plan_charge.version AS rate_plan_charge_version
    FROM zuora_rate_plan
        INNER JOIN zuora_rate_plan_charge
          ON zuora_rate_plan.rate_plan_id = zuora_rate_plan_charge.rate_plan_id

), invoiced_charges AS (

    SELECT DISTINCT rate_plan_charge_id
    FROM {{ ref('zuora_invoice_item_source') }}

), final AS (

    SELECT
        base_charges.*,
        ROW_NUMBER(
        ) OVER (
        PARTITION BY rate_plan_charge_number
        ORDER BY rate_plan_charge_segment, rate_plan_charge_version
        ) AS segment_version_order,
        IFF(
        ROW_NUMBER(
        ) OVER (
        PARTITION BY rate_plan_charge_number, rate_plan_charge_segment
        -- ordering by invoice_item_ids first will put charges without invoices to the end
        ORDER BY invoiced_charges.invoice_item_id, rate_plan_charge_version DESC) = 1,
        TRUE, FALSE
        ) AS is_last_segment_version
    FROM base_charges
    LEFT JOIN invoiced_charges ON invoiced_charges.rate_plan_charge_id = base_charges.rate_plan_charge_id
)

SELECT *
FROM final