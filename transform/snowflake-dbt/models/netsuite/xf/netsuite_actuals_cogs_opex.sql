WITH transactions AS (

     SELECT *
     FROM {{ref('netsuite_transactions')}}

), transaction_lines AS (

     SELECT *
     FROM {{ref('netsuite_transaction_lines_xf')}}

), accounting_periods AS (

     SELECT *
     FROM {{ref('netsuite_accounting_periods')}}

), accounts AS (

     SELECT *
     FROM {{ref('netsuite_accounts_xf')}}

), subsidiaries AS (

     SELECT *
     FROM {{ref('netsuite_subsidiaries')}}

), departments AS (

     SELECT *
     FROM {{ref('netsuite_departments_xf')}}

), consolidated_exchange_rates AS (

     SELECT *
     FROM {{ref('netsuite_consolidated_exchange_rates')}}

), date_details AS (

     SELECT DISTINCT
            first_day_of_month,
            fiscal_year,
            fiscal_quarter,
            fiscal_quarter_name
     FROM {{ref('date_details')}}

), opex_cogs AS (

     SELECT t.transaction_id,
            t.external_ref_number,
            t.transaction_ext_id,
            t.document_id,
            tl.memo                                          AS transaction_lines_memo,
            t.status,
            t.transaction_type,
            a.account_name,
            a.account_full_name,
            a.account_number,
            a.unique_account_number,
            a.parent_account_number,
            d.department_name,
            d.parent_department_name,
            ap.accounting_period_starting_date::DATE         AS accounting_period,
            ap.accounting_period_name,
            SUM(CASE WHEN tl.subsidiary_id = 1 THEN amount
                     ELSE (tl.amount * e.average_rate) END)  AS actual_amount
    FROM transaction_lines tl
    LEFT JOIN transactions t
      ON tl.transaction_id = t.transaction_id
    LEFT JOIN accounts a
      ON a.account_id = tl.account_id
    LEFT JOIN departments d
      ON d.department_id = tl.department_id
    LEFT JOIN accounting_periods ap
      ON ap.accounting_period_id = t.accounting_period_id
    LEFT JOIN subsidiaries s
      ON tl.subsidiary_id = s.subsidiary_id
    LEFT JOIN consolidated_exchange_rates e
      ON ap.accounting_period_id = e.accounting_period_id
      AND e.from_subsidiary_id = s.subsidiary_id
    WHERE a.account_number between '5000' and '6999'
      AND ap.fiscal_calendar_id = 2
      AND e.to_subsidiary_id = 1
    {{ dbt_utils.group_by(n=16) }}

), income_statement_grouping AS (

    SELECT transaction_id,
           external_ref_number,
           transaction_ext_id,
           document_id,
           account_name,
           account_full_name,
           account_number || ' - ' || account_name          AS unique_account_name,
           account_number,
           parent_account_number,
           unique_account_number,
           actual_amount,
           CASE WHEN account_number BETWEEN '5000' AND '5999' THEN '2-Cost of Sales'
                WHEN account_number BETWEEN '6000' AND '6999' THEN '3-Expense'
           END                                              AS income_statement_grouping,
           {{cost_category('account_number','account_name')}},
           transaction_lines_memo,
           status,
           transaction_type,
           department_name,
           parent_department_name,
           accounting_period,
           accounting_period_name,
           fiscal_year,
           fiscal_quarter,
           fiscal_quarter_name
       FROM opex_cogs oc
       LEFT JOIN date_details dd
         ON dd.first_day_of_month = oc.accounting_period
       WHERE account_number NOT IN ('5077','5079','5080')

)

SELECT *
FROM income_statement_grouping
