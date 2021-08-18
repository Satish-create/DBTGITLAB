WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sisense_users_roles') }}

), renamed as (

    SELECT
      id::VARCHAR                            AS id,
      updated_at::VARCHAR                    AS updated_at,
      role_id::VARCHAR                       AS role_id,
      user_id::VARCHAR                       AS user_id,
      space_id::VARCHAR                      AS space_id
      
    FROM source
)

SELECT *
FROM renamed