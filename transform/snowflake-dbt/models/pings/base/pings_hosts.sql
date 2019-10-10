{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *
  FROM {{ source('pings_tap_postgres', 'hosts') }}

),

renamed AS (

  SELECT
    id::INTEGER                         AS host_id,
    url::VARCHAR                        AS host_url,
    created_at::TIMESTAMP               AS created_at,
    updated_at::TIMESTAMP               AS updated_at,
    star::BOOLEAN                       AS has_star,
    fortune_rank::INTEGER               AS fortune_rank,
    in_salesforce::BOOLEAN              AS is_in_salesforce
    --current_usage_data_id // waiting on fresh data https://gitlab.com/gitlab-data/analytics/issues/2696
    --current_version_check_id // waiting on fresh data https://gitlab.com/gitlab-data/analytics/issues/2696

  FROM source

)

SELECT *
FROM renamed
