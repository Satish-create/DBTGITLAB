{{ config({
    "materialized": "view"
    })
}}

    SELECT *
    FROM {{ ref('gcp_billing_export_source') }}

)

SELECT
{{ dbt_utils.surrogate_key(['primary_key', 'project_label_key','project_label_value'] ) }}          AS project_label_pk,
source.primary_key                                                                                  AS source_primary_key,
project_labels_flat.value:key::VARCHAR                                                              AS project_label_key,
project_labels_flat.value:value::VARCHAR                                                            AS project_label_value,
FROM source,
lateral flatten(input=> project_labels) project_labels_flat
