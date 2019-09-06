{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_job_artifacts') }}

), renamed AS (

  SELECT 
    id::INTEGER             AS ci_group_variable_id, 
    key                     AS ci_group_variable_key, 
    value                   AS ci_group_variable_value, 
    group_id::INTEGER       AS ci_group_variable_group_id, 
    created_at::TIMESTAMP   AS ci_group_variable_created_at, 
    updated_at::TIMESTAMP   AS ci_group_variable_updated_at, 
    masked                  AS ci_group_variable_masked, 
    variable_type           AS ci_group_variable_variable_type 
  FROM ci_group_variables 
  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
