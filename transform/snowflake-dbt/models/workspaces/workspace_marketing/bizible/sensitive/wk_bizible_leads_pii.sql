WITH source AS (

    SELECT *
    FROM {{ ref('bizible_leads_source') }}

)

SELECT *
FROM source