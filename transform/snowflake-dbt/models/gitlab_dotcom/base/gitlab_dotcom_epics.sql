{{ config({
    "schema": "sensitive"
    })
}}

WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'epics') }}

), renamed AS (

    SELECT
      id::integer                                   AS epic_id,
      milestone_id::integer                         AS milestone_id,
      group_id::integer                             AS group_id,
      author_id::integer                            AS author_id,
      assignee_id::integer                          AS assignee_id,
      iid::integer                                  AS epic_internal_id,
      updated_by_id::integer                        AS updated_by_id,
      last_edited_by_id::integer                    AS last_edited_by_id,
      lock_version::integer                         AS lock_version,
      start_date::date                              AS epic_start_date,
      end_date::date                                AS epic_end_date,
      last_edited_at::timestamp                     AS epic_last_edited_at,
      created_at::timestamp                         AS epic_created_at,
      updated_at::timestamp                         AS epic_updated_at,
      title                                         AS epic_title,
      LENGTH(title)                                 AS epic_title_length,
      LENGTH(description)                           AS epic_description_length

    FROM source
    WHERE rank_in_key = 1

)

SELECT *
FROM renamed
