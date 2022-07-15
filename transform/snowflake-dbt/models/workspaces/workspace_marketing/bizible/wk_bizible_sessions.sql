WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_sessions_source') }}
    FROM {{ ref('bizible_sessions_source') }}

)

SELECT *
FROM source