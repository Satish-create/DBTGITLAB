-- disabled model until the data starts flowing in (the source table is missing from tap_postgres)
{{
  config(
    enabled = false
  )
}}

WITH source AS (

	SELECT *, ROW_NUMBER() OVER (PARTITION BY epic_id ORDER BY _uploaded_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'epic_metrics') }}

), renamed AS (

    SELECT
      epic_id :: integer                    as epic_id,
      created_at :: timestamp               as epic_metrics_created_at,
      updated_at :: timestamp               as epic_metrics_updated_at

    FROM source
    WHERE rank_in_key = 1

)


SELECT *
FROM renamed