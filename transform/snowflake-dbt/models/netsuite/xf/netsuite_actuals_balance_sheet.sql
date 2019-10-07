WITH transactions AS (

     SELECT *
     FROM {{ ref('netsuite_transactions') }}

), transaction_lines AS (

     SELECT *
     FROM {{ ref('netsuite_transaction_lines_xf') }}

), accounting_periods AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_periods') }}

), accounts AS (

     SELECT *
     FROM {{ ref('netsuite_accounts_xf') }}

), subsidiaries AS (

     SELECT *
     FROM {{ ref('netsuite_subsidiaries') }}

), accounting_books AS (

     SELECT *
     FROM {{ ref('netsuite_accounting_books') }}

), consolidated_exchange_rates AS (

     SELECT *
     FROM {{ ref('netsuite_consolidated_exchange_rates') }}

), period_exchange_rate_map AS ( -- exchange rates used, by accounting period, to convert to parent subsidiary

     SELECT
       consolidated_exchange_rates.accounting_period_id,
       consolidated_exchange_rates.average_rate,
       consolidated_exchange_rates.current_rate,
       consolidated_exchange_rates.historical_rate,
       consolidated_exchange_rates.from_subsidiary_id,
       consolidated_exchange_rates.to_subsidiary_id
     FROM consolidated_exchange_rates
     WHERE consolidated_exchange_rates.to_subsidiary_id IN (
       SELECT
        subsidiary_id
       FROM subsidiaries
       WHERE parent_id IS NULL  -- constrait - only the primary subsidiary has no parent
       )
       AND consolidated_exchange_rates.accounting_book_id IN (
         SELECT
           accounting_book_id
         FROM accounting_books
         WHERE LOWER(is_primary) = 'true'
       )

), accountXperiod_exchange_rate_map AS ( -- account table with exchange rate details by accounting period

     SELECT
       period_exchange_rate_map.accounting_period_id,
       period_exchange_rate_map.from_subsidiary_id,
       period_exchange_rate_map.to_subsidiary_id,
       accounts.account_id,
       CASE WHEN LOWER(accounts.general_rate_type) = 'historical' THEN period_exchange_rate_map.historical_rate
            WHEN LOWER(accounts.general_rate_type) = 'current'    THEN period_exchange_rate_map.current_rate
            WHEN LOWER(accounts.general_rate_type) = 'average'    THEN period_exchange_rate_map.average_rate
            ELSE null
       END                AS exchange_rate
     FROM accounts
     CROSS JOIN period_exchange_rate_map
     WHERE LOWER(accounts.is_account_inactive) = 'false'

), transaction_lines_w_accounting_period AS ( -- transaction line totals, by accounts, accounting period and subsidiary

     SELECT
       transaction_lines.transaction_id,
       transaction_lines.transaction_line_id,
       transaction_lines.subsidiary_id,
       transaction_lines.account_id,
       transactions.accounting_period_id as transaction_accounting_period_id,
       COALESCE(transaction_lines.amount, 0) as unconverted_amount
     FROM transaction_lines
     INNER JOIN transactions on transaction_lines.transaction_id = transactions.transaction_id
     WHERE LOWER(transactions.transaction_type) != 'revenue arrangement'

), period_id_list_to_current_period AS ( -- period ids with all future period ids.  this is needed to calculate cumulative totals by correct exchange rates.

    SELECT
      base.accounting_period_id,
      array_agg(multiplier.accounting_period_id) WITHIN GROUP (order by multiplier.accounting_period_id) AS accounting_periods_to_include_for
    FROM accounting_periods AS base
    INNER JOIN accounting_periods AS multiplier
      ON base.accounting_period_starting_date <= multiplier.accounting_period_starting_date
      AND base.is_quarter = multiplier.is_quarter
      AND base.is_year = multiplier.is_year
      AND base.fiscal_calendar_id = multiplier.fiscal_calendar_id
      AND multiplier.accounting_period_starting_date <= CURRENT_TIMESTAMP()
    WHERE LOWER(base.is_quarter) = 'false'
      AND LOWER(base.is_year) = 'false'
      AND base.fiscal_calendar_id = (SELECT
                                       fiscal_calendar_id
                                     FROM subsidiaries
                                     WHERE parent_id IS NULL) -- fiscal calendar will align with parent subsidiary's default calendar
    {{ dbt_utils.group_by(n=1) }}

), flatten_period_id_array AS (

     SELECT
       accounting_period_id,
       reporting_accounting_period_id.value AS reporting_accounting_period_id
     FROM period_id_list_to_current_period
       ,lateral flatten (input => accounting_periods_to_include_for) reporting_accounting_period_id
     WHERE array_size(accounting_periods_to_include_for) > 1

), transactions_in_every_calculation_period AS (

     SELECT
       transaction_lines_w_accounting_period.*,
       reporting_accounting_period_id
     FROM transaction_lines_w_accounting_period
     INNER JOIN flatten_period_id_array
       ON flatten_period_id_array.accounting_period_id = transaction_lines_w_accounting_period.transaction_accounting_period_id

), transactions_in_every_calculation_period_w_exchange_rates as (

     SELECT
       transactions_in_every_calculation_period.*,
       exchange_reporting_period.exchange_rate as exchange_reporting_period,
       exchange_transaction_period.exchange_rate as exchange_transaction_period
     FROM transactions_in_every_calculation_period
     LEFT JOIN accountXperiod_exchange_rate_map as exchange_reporting_period
       ON transactions_in_every_calculation_period.account_id = exchange_reporting_period.account_id
       AND transactions_in_every_calculation_period.reporting_accounting_period_id = exchange_reporting_period.accounting_period_id
       AND transactions_in_every_calculation_period.subsidiary_id = exchange_reporting_period.from_subsidiary_id
     LEFT JOIN accountXperiod_exchange_rate_map as exchange_transaction_period
       ON transactions_in_every_calculation_period.account_id = exchange_transaction_period.account_id
       AND transactions_in_every_calculation_period.transaction_accounting_period_id = exchange_transaction_period.accounting_period_id
       AND transactions_in_every_calculation_period.subsidiary_id = exchange_transaction_period.from_subsidiary_id

), transactions_with_converted_amounts AS (

     SELECT
       transactions_in_every_calculation_period_w_exchange_rates.*,
       unconverted_amount * exchange_transaction_period as converted_amount_using_transaction_accounting_period,
       unconverted_amount * exchange_reporting_period as converted_amount_using_reporting_month
     FROM transactions_in_every_calculation_period_w_exchange_rates

), balance_sheet AS (

     SELECT
       reporting_accounting_periods.accounting_period_id                    AS accounting_period_id,
       reporting_accounting_periods.accounting_period_starting_date::DATE   AS accounting_period,
       reporting_accounting_periods.accounting_period_name                  AS accounting_period_name,
       reporting_accounting_periods.is_accounting_period_adjustment         AS is_accounting_period_adjustment,
       CASE WHEN (LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold')
              AND reporting_accounting_periods.year_id = transaction_accounting_periods.year_id) THEN 'net income'
            WHEN LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold') THEN 'retained earnings'
            ELSE LOWER(accounts.account_name)
       END                                                                  AS account_name,
       CASE WHEN (LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold')
              AND reporting_accounting_periods.year_id = transaction_accounting_periods.year_id) THEN 'net income'
            WHEN LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold') THEN 'retained earnings'
            ELSE LOWER(accounts.account_type)
       END                                                                  AS account_type_name,
       CASE WHEN LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold') THEN null
            ELSE accounts.account_id
       END                                                                  AS account_id,
       CASE WHEN LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold') THEN null
            ELSE accounts.account_number
       END                                                                  AS account_number,
       SUM(CASE WHEN LOWER(accounts.account_type) IN ('income','other income','expense','other expense','other income','cost of goods sold') THEN -converted_amount_using_transaction_accounting_period
                WHEN (LOWER(accounts.general_rate_type) = 'historical' AND LOWER(accounts.is_leftside_account) = 'false') THEN -converted_amount_using_transaction_accounting_period
                WHEN (LOWER(accounts.general_rate_type) = 'historical' AND LOWER(accounts.is_leftside_account) = 'true') THEN converted_amount_using_transaction_accounting_period
                WHEN (LOWER(accounts.is_balancesheet_account) = 'true' AND LOWER(accounts.is_leftside_account) = 'false') THEN -converted_amount_using_reporting_month
                WHEN (LOWER(accounts.is_balancesheet_account) = 'true' AND LOWER(accounts.is_leftside_account) = 'true') THEN converted_amount_using_reporting_month
                ELSE 0
           END)                                                             AS converted_amount
       FROM  transactions_with_converted_amounts AS transactions_with_converted_amounts
       LEFT JOIN accounts
         ON transactions_with_converted_amounts.account_id = accounts.account_id
       LEFT JOIN accounting_periods AS reporting_accounting_periods
         ON transactions_with_converted_amounts.reporting_accounting_period_id = reporting_accounting_periods.accounting_period_id
       LEFT JOIN accounting_periods AS transaction_accounting_periods
         ON transactions_with_converted_amounts.transaction_accounting_period_id = transaction_accounting_periods.accounting_period_id
       WHERE reporting_accounting_periods.fiscal_calendar_id    = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND transaction_accounting_periods.fiscal_calendar_id  = (SELECT
                                                                     fiscal_calendar_id
                                                                   FROM subsidiaries
                                                                   WHERE parent_id IS NULL)
         AND LOWER(accounts.account_type) != 'statistical'
        {{ dbt_utils.group_by(n=8) }}

), balance_sheet_unique_account_name AS (

      SELECT
        accounting_period_id,
        accounting_period,
        accounting_period_name,
        is_accounting_period_adjustment,
        account_id,
        account_name,
        account_number,
        account_number || ' - ' || account_name   AS unique_account_name,
        account_type_name,
        converted_amount
              
      FROM balance_sheet

)

SELECT *
FROM balance_sheet_unique_account_name
