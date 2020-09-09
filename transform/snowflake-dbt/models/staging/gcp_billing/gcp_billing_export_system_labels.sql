{{ config({
    "materialized": "view"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

), renamed as (

    SELECT
        {{ dbt_utils.surrogate_key(['source.primary_key', 'system_labels_flat.value:key','system_labels_flat.value:value'] ) }}     AS system_label_pk,
        source.primary_key                                                                                                          AS source_primary_key,
        system_labels_flat.value:key::VARCHAR                                                                                       AS system_label_key,
        system_labels_flat.value:value::VARCHAR                                                                                     AS system_label_value
    FROM source,
    lateral flatten(input=> system_labels) system_labels_flat
)

SELECT * FROM renamed
