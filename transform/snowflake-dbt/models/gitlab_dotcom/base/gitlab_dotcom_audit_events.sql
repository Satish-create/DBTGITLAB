WITH source AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY UPDATED_AT DESC) AS rank_in_key
  FROM {{ source('gitlab_dotcom', 'audit_events') }}

), renamed AS (

    SELECT
      id::INTEGER             AS audit_event_id,
      author_id::INTEGER      AS author_id,
      type::VARCHAR           AS audit_event_type,
      entity_id::INTEGER      AS entity_id,
      entity_type::VARCHAR    AS entity_type,
      details::VARCHAR        AS audit_event_details,
      created_at::TIMESTAMP   AS audit_event_created_at,
      updated_at::TIMESTAMP   AS audit_event_updated_at

    FROM source
    WHERE rank_in_key = 1
    ORDER BY audit_event_created_at

)

SELECT * FROM renamed
