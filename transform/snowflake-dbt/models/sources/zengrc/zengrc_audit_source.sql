WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'audits') }}

),

renamed AS (

    SELECT
        code::VARCHAR                      AS audit_code,
        created_at::TIMESTAMP              AS audit_created_at,
        description::VARCHAR               AS audidt_description,
        end_date::DATE                     AS audit_end_date,
        id::NUMBER                         AS audit_id,
        report_period_end_date::DATE       AS audit_report_period_end_date,
        report_period_start_date::DATE     AS audit_report_period_start_date,
        start_date::DATE                   AS audit_start_date,
        status::VARCHAR                    AS audit_status,
        sync_external_attachments::BOOLEAN AS has_external_attachments,
        sync_external_comments::BOOLEAN    AS has_external_coments,
        title::VARCHAR                     AS audit_title,
        type::VARCHAR                      AS zengrc_object_type,
        updated_at::TIMESTAMP              AS audit_uploaded_at,
        __loaded_at::TIMESTAMP             AS audit_loaded_at
    FROM source

)

SELECT *
FROM renamed



