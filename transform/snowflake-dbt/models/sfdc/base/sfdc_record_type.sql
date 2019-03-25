{{ config(schema='analytics') }}

WITH source AS (

    SELECT *
    FROM {{ var("database") }}.salesforce_stitch.recordtype

), renamed AS (

    SELECT
         id                AS record_type_id,
         developername     AS record_type_name,
        --keys
         businessprocessid AS business_process_id,
        --info
         name              AS record_type_label,
         description       AS record_type_description,
         sobjecttype       AS record_type_modifying_object_type

    FROM source

)

SELECT *
FROM renamed
