WITH source AS (

	SELECT *
	FROM {{ var("database") }}.gitlab_dotcom.labels

), renamed AS (

    SELECT

      id :: integer                                as label_id,
      -- title // hidden as contains PII
      LENGTH(title)                                as title_length,
      color,
      project_id :: integer                        as project_id,
      group_id :: integer                          as group_id,
      template,
      type,
      created_at :: timestamp                      as label_created_at,
      updated_at :: timestamp                      as label_updated_at,
      TO_TIMESTAMP(_updated_at :: integer)         as labels_last_updated_at

    FROM source


)

SELECT *
FROM renamed