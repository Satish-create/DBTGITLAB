{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'merge_request_metrics') }}


), renamed AS (

    SELECT

      id::INTEGER                                              AS merge_request_metric_id,
      merge_request_id::INTEGER                                AS merge_request_id,

      latest_build_started_at::TIMESTAMP                       AS latest_build_started_at,
      latest_build_finished_at::TIMESTAMP                      AS latest_build_finished_at,
      first_deployed_to_production_at::TIMESTAMP               AS first_deployed_to_production_at,
      merged_at::TIMESTAMP                                     AS merged_at,
      created_at::TIMESTAMP                                    AS merge_request_metric_created_at,
      updated_at::TIMESTAMP                                    AS merge_request_metric_updated_at,
      latest_closed_at::TIMESTAMP                              AS latest_closed_at,

      pipeline_id::INTEGER                                     AS pipeline_id,
      merged_by_id::INTEGER                                    AS merged_by_id,
      latest_closed_by_id::INTEGER                             AS latest_closed_by_id

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
