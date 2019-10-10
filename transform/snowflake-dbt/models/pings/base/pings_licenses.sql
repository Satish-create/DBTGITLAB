{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('pings_tap_postgres', 'licenses') }}

),

renamed AS (

  SELECT
    id::INTEGER                           AS licence_id,
    company::VARCHAR                      AS company,
    user_count::INTEGER                   AS user_count,
    PARSE_JSON(add_ons)                   AS add_ons,
    --md5 // waiting on fresh data https://gitlab.com/gitlab-data/analytics/issues/2696
    starts_on::TIMESTAMP                  AS starts_at,
    expires_on::TIMESTAMP                 AS expires_at,
    created_at::TIMESTAMP                 AS created_at,
    updated_at::TIMESTAMP                 AS updated_at,
    active_users_count::INTEGER           AS active_users_count,
    historical_max_users_count::INTEGER   AS historical_max_users_count,
    last_ping_received_at::TIMESTAMP      AS last_ping_received_at,
    version::VARCHAR                      AS version

  FROM source

)

SELECT *
FROM renamed
