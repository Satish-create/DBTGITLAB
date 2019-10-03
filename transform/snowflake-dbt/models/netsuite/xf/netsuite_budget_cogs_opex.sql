WITH budget AS (

     SELECT *
     FROM {{ ref('netsuite_budget') }}

), budget_category AS (

     SELECT *
     FROM {{ ref('netsuite_budget_category') }}

), accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

), accounting_periods AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_periods') }}

), subsidiaries AS (

     SELECT *
     FROM {{ ref('netsuite_subsidiaries') }}

), departments AS (

     SELECT *
     FROM {{ ref('netsuite_departments_xf') }}

), date_details AS (

     SELECT DISTINCT
            first_day_of_month,
            fiscal_year,
            fiscal_quarter,
            fiscal_quarter_name
     FROM {{ref('date_details')}}

), budget_forecast_cogs_opex AS (

    SELECT a.account_id,
           a.account_number || ' - ' || a.account_name                                 AS unique_account_name,
           a.account_name,
           a.account_full_name,
           a.account_number,
           a.parent_account_number,
           a.unique_account_number,
           ap.accounting_period_id,
           ap.accounting_period_starting_date::DATE                                    AS accounting_period,
           ap.accounting_period_name,
           ap.accounting_period_full_name,
           d.department_id,
           d.department_name,
           COALESCE(d.parent_department_name, 'zNeed Accounting Reclass')              AS parent_department_name,
           bc.budget_category,
           CASE WHEN account_number BETWEEN '5000' AND '5999' THEN '2-Cost of Sales'
                WHEN account_number BETWEEN '6000' AND '6999' THEN '3-Expense'
           END                                                                         AS income_statement_grouping,
           {{cost_category('account_number','account_name')}},
           SUM(CASE WHEN b.budget_amount IS NULL THEN 0
                    ELSE b.budget_amount
               END)                                                                    AS budget_amount
    FROM budget b
    LEFT JOIN budget_category bc
      ON b.category_id = bc.budget_category_id
    LEFT JOIN accounts a
      ON b.account_id = a.account_id
    LEFT JOIN accounting_periods ap
      ON b.accounting_period_id = ap.accounting_period_id
    LEFT JOIN departments d
      ON b.department_id = d.department_id
    WHERE ap.fiscal_calendar_id = 2
      AND a.account_number between '5000' and '6999'
    {{ dbt_utils.group_by(n=17) }}

)

SELECT b.*
FROM budget_forecast_cogs_opex b
LEFT JOIN date_details dd
 ON dd.first_day_of_month = b.accounting_period
WHERE account_number NOT IN ('5077','5079','5080')
ORDER BY 8,4
