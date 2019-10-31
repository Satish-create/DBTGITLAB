WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rank_in_key
  FROM {{ source('version', 'licenses') }}

),

renamed AS (

  SELECT
    id::INTEGER                           AS license_id,
    company::VARCHAR                      AS company,
    user_count::INTEGER                   AS user_count,
    add_ons::VARCHAR                      AS add_ons,
    md5::VARCHAR                          AS license_md5,
    starts_on::TIMESTAMP                  AS started_at,
    expires_on::TIMESTAMP                 AS expired_at,
    created_at::TIMESTAMP                 AS created_at,
    updated_at::TIMESTAMP                 AS updated_at,
    active_users_count::INTEGER           AS active_users_count,
    historical_max_users_count::INTEGER   AS historical_max_users_count,
    last_ping_received_at::TIMESTAMP      AS last_ping_received_at,
    version::VARCHAR                      AS version

  FROM source
  WHERE rank_in_key = 1

)

SELECT *
FROM renamed
