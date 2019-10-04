WITH base_accounts AS (

    SELECT *
    FROM {{ref('netsuite_accounts')}}
    WHERE account_number IS NOT NULL

), ultimate_account AS (

    SELECT
      a.account_id,
      a.account_number,
      CASE WHEN a.parent_account_id IS NOT NULL THEN a.parent_account_id
           ELSE a.account_id
      END                                           AS parent_account_id,
      CASE WHEN b.account_number IS NOT NULL THEN b.account_number
           ELSE a.account_number
      END                                           AS parent_account_number,
      CASE WHEN b.account_number IS NOT NULL
           THEN b.account_number || ' - ' || a.account_number
           ELSE a.account_number
      END                                           AS unique_account_number,
      a.currency_id,
      a.account_name,
      a.account_full_name,
      a.account_full_description,
      a.account_type
    FROM base_accounts a
    LEFT JOIN base_accounts b
      ON a.parent_account_id = b.account_id

)

SELECT *
FROM ultimate_account
