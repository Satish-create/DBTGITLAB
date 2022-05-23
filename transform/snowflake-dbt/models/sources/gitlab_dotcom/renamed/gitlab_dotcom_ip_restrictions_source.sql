WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_ip_restrictions_dedupe_source') }}
    
), renamed AS (

    SELECT
      id::NUMBER                                      AS ip_restrictions_id,
      group_id::NUMBER                                AS group_id,
      range::VARCHAR                                  AS range
    FROM source

)

SELECT *
FROM renamed
