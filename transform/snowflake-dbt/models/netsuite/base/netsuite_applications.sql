{#
-- I cannot find netsuite docs on the applications model
#}

with base as (

    SELECT *
    FROM {{ var("database") }}.gcloud_postgres_stitch.netsuite_applications

), renamed as (

    SELECT unique_id as application_id,
            transaction_id,
            line as transaction_line,
            amount as application_amount,
            apply as is_applied,
            apply_date::date as apply_date,
            doc as doc_id,
            ref_num as reference_number, --IS NOT NUMERICAL
            total as application_total,
            type as application_type
    FROM base
)

SELECT *
FROM renamed

