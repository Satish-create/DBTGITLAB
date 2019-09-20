{{ config({
    "schema": "sensitive"
    })
}}

with source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'namespaces') }}

), renamed AS (

    SELECT  id::INTEGER                                                   AS namespace_id,
            name :: varchar                                                 AS namespace_name,
            path :: varchar                                                 AS namespace_path,
            owner_id::INTEGER                                             AS owner_id,
            type                                                            AS namespace_type,
            IFF(avatar IS NULL, FALSE, TRUE)                                AS has_avatar,
            created_at::TIMESTAMP                                         AS namespace_created_at,
            updated_at::TIMESTAMP                                         AS namespace_updated_at,
            membership_lock::BOOLEAN                                      AS is_membership_locked,
            request_access_enabled::BOOLEAN                               AS has_request_access_enabled,
            share_with_group_lock::BOOLEAN                                AS has_share_with_group_locked,
            CASE
              WHEN visibility_level = '20' THEN 'public'
              WHEN visibility_level = '10' THEN 'internal'
              ELSE 'private'
            END                                                             AS visibility_level,
            ldap_sync_status,
            ldap_sync_error,
            ldap_sync_last_update_at::TIMESTAMP                             AS ldap_sync_last_update_at,
            ldap_sync_last_successful_update_at::TIMESTAMP                  AS ldap_sync_last_successful_update_at,
            ldap_sync_last_sync_at::TIMESTAMP                               AS ldap_sync_last_sync_at,
            lfs_enabled::BOOLEAN                                            AS lfs_enabled,
            parent_id::INTEGER                                              AS parent_id,
            shared_runners_minutes_limit :: number                          AS shared_runners_minutes_limit,
            repository_size_limit::number                                   AS repository_size_limit,
            require_two_factor_authentication::BOOLEAN                    AS does_require_two_factor_authentication,
            two_factor_grace_period :: number                               AS two_factor_grace_period,
            plan_id::INTEGER                                              AS plan_id,
            project_creation_level::INTEGER                                 AS project_creation_level
    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
