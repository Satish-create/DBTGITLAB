{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('netsuite_stitch', 'transaction') }}

), renamed AS (

    SELECT
       address,
       applied,
       aracct['internalId']::NUMBER         AS accounts_receivable_account_id,
       aracct['name']::STRING               AS accounts_receivable_account_name,
       approvalstatus['internalId']::NUMBER AS approval_status_id,
       approvalstatus['name']::STRING       AS approval_status_name,
       balance,
       canhavestackable                     AS can_have_stackable,
       createddate                          AS created_date,
       currency['internalId']::NUMBER       AS currency_id,
       currency['name']::STRING             AS currency_name,
       custbody_adjustment_journal          AS adjustment_journal,
       custbody_blackline_source            AS blackline_source,
       custbody_cash_register               AS cash_register,
       custbody_createdfrom_expensify       AS createdfrom_expensify,
       custbody_itr_nexus                   AS itr_nexus,
       custbody_nexus_notc                  AS nexus_notc,
       custbody_nondeductible_processed     AS nondeductible_processed,
       custbody_report_timestamp            AS report_timestamp,
       customer['internalId']::NUMBER       AS customer_id,
       customer['name']::STRING             AS customer_name,
       department['internalId']::NUMBER     AS department_id,
       department['name']::STRING           AS department_name,
       duedate                              AS due_date,
       email,
       entity['name']::STRING               AS entity_name,
       entity['internalId']::STRING         AS entity_id,
       exchangerate                         AS exchange_rate,
       internalid                           AS transaction_id, -- unique id
       istaxable                            AS is_taxable,
       itemlist                             AS item_list,
       lastmodifieddate                     AS last_modified_date,
       memo,
       payment,
       paymenthold                          AS payment_hold,
       pending,
       postingperiod['internalId']::NUMBER  AS posting_period_id,
       postingperiod['name']::STRING        AS posting_period_name,
       printvoucher                         AS print_voucher,
       reversaldate                         AS reversal_date,
       reversalentry                        AS reverals_entry,
       saleseffectivedate                   AS sales_effective_date,
       shipisresidential                    AS ship_is_residential,
       shippingaddress                      AS shipping_address,
       status,
       subsidiary['internalId']::NUMBER     AS subsidiary_id,
       subsidiary['name']::STRING           AS subsidiary_name,
       subtotal,
       taxitem['internalId']::NUMBER        AS tax_item_id,
       taxitem['name']::STRING              AS tax_item_name,
       taxrate                              AS tax_rate,
       taxtotal                             AS tax_total,
       terms['internalId']::NUMBER          AS terms_id,
       terms['name']::STRING                AS terms_name,
       toach                                AS to_ach,
       tobeemailed                          AS to_be_emailed,
       tobefaxed                            AS to_be_faxed,
       tobeprinted                          AS to_be_printed,
       total,
       trandate                             AS transaction_date,
       tranid                               AS tranid, -- not the unique transaction_id
       transactionnumber                    AS transaction_number,
       _type                                AS transaction_type,
       unapplied,
       usertotal                            AS user_total,
       otherrefnum                          AS other_ref_num,
       intercotransaction                   AS interco_transaction,
       intercostatus                        AS interco_status,
       performautobalance                   AS perform_autobalance,
       message,
       supervisorapproval                   AS supervisor_approval,
       creditlist                           AS credit_list,
       shipdate                             AS ship_date,
       saveonauthdecline                    AS save_on_auth_decline,
       shipcomplete                         AS ship_complete,
       tosubsidiary                         AS to_subsidiary,
       custbody_created_by,
       source,
       voidjournal['internalId']::NUMBER    AS void_journal_id,
       voidjournal['name']::STRING          AS void_journal_name,
       paymentmethod['internalId']::NUMBER  AS payment_method_id,
       paymentmethod['name']::STRING        AS payment_method_name,
       otherlist
FROM source

)

SELECT *
FROM renamed
