{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'project_group_links') }}

), renamed AS (

    SELECT

      id::INTEGER                                     AS project_group_link_id,
      project_id::INTEGER                             AS project_id,
      group_id::INTEGER                               AS group_id,
      group_access::INTEGER                           AS group_access,
      created_at::TIMESTAMP                           AS project_features_created_at,
      updated_at::TIMESTAMP                           AS project_features_updated_at,
      expires_at::TIMESTAMP                           AS expires_at

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
