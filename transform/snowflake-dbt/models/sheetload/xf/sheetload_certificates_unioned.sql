{{ config({
    "schema": "analytics"
    })
}}

{{ dbt_utils.union_relations(
    relations=[ref('sheetload_ally_certificate'),
               ref('sheetload_values_certificate'),
               ref('sheetload_communication_certificate'),
               ref('sheetload_compensation_certificate'),
               ref('sheetload_ic_collaboration_competency')]
) }}
