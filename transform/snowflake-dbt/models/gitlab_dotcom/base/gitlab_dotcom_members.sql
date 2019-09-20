WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'members') }}

), renamed AS (

    SELECT

      id::INTEGER                                    AS member_id,
      access_level::INTEGER                          AS access_level,
      source_id::INTEGER                             AS source_id,
      source_type                                    AS member_source_type,
      user_id::INTEGER                               AS user_id,
      notification_level::INTEGER                    AS notification_level,
      type                                           AS member_type,
      created_at::timestamp                          AS invite_created_at,
      created_by_id::INTEGER                         AS created_by_id,
      invite_accepted_at::timestamp                  AS invite_accepted_at,
      requested_at::timestamp                        AS requested_at,
      expires_at::timestamp                          AS expires_at,
      ldap::boolean                                  AS has_ldap,
      override::boolean                              AS has_override

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
