{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'notification_settings') }}

), renamed AS (

    SELECT

      id :: integer                                     AS notification_settings_id,
      user_id :: integer                                AS user_id,
      source_id :: integer                              AS source_id,
      source_type                                       AS source_type,
      level :: integer                                  AS settings_level,
      created_at :: timestamp                           AS notification_settings_created_at,
      updated_at :: timestamp                           AS notification_settings_updated_at,
      new_note :: boolean                               AS has_new_note_enabled,
      new_issue :: boolean                              AS has_new_issue_enabled,
      reopen_issue :: boolean                           AS has_reopen_issue_enabled,
      close_issue :: boolean                            AS has_close_issue_enabled,
      reassign_issue :: boolean                         AS has_reassign_issue_enabled,
      new_merge_request :: boolean                      AS has_new_merge_request_enabled,
      reopen_merge_request :: boolean                   AS has_reopen_merge_request_enabled,
      close_merge_request :: boolean                    AS has_close_merge_request_enabled,
      reassign_merge_request :: boolean                 AS has_reassign_merge_request_enabled,
      merge_merge_request :: boolean                    AS has_merge_merge_request_enabled,
      failed_pipeline :: boolean                        AS has_failed_pipeline_enabled,
      success_pipeline :: boolean                       AS has_success_pipeline_enabled


    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
