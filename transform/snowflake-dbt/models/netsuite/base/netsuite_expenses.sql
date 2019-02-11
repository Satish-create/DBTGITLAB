{#
-- I can't find netsuite docs on expenses
#}

with base as (

    SELECT *
    FROM {{ var("database") }}.gcloud_postgres_stitch.netsuite_expenses

), renamed as (

        SELECT unique_id as expense_id,
            transaction_id,
            line as transaction_line,
            account_id,
            account_name,
            amount,
            gross_amt as gross_amount,
            is_billable,
            department_id,
            department_name,
            category_id,
            category_name,
            CASE
              WHEN account_name ILIKE '%Contract%'
               THEN substring(md5(memo),16)
              ELSE memo END AS memo,
            tax1_amt as tax_amount,
            tax_code_id,
            tax_code_name,
            --tax_details_reference
            tax_rate1,
            --tax_rate2
            --amortization_end_date
            --amortization_residual
            --amortization_sched_id
            --amortization_sched_name
            --amortiz_start_date
            class_id,
            class_name,
            customer_id,
            customer_name
            --location_id
            --location_name
            --mark_received
            --order_doc
            --order_line
            --project_task_id
            --project_task_name
            --created_from_id
            --created_from_name
            --is_closed
            --linked_order_list --IS JSON
            --custom_field_list --IS JSON
        FROM base
)

SELECT *
FROM renamed
