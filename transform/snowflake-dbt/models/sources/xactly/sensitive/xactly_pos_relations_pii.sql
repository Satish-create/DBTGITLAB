WITH source AS (

    SELECT *
    FROM {{ ref('xactly_pos_relations_source') }}

), renamed AS (

    SELECT

      created_by_id,
      created_by_name,
      created_date,
      from_pos_id,
      {{ nohash_sensitive_columns('xactly_pos_relations_source', 'from_pos_name') }},
      id,
      modified_by_id,
      modified_by_name,
      modified_date,
      pos_rel_type_id,
      to_pos_id,
      {{ nohash_sensitive_columns('xactly_pos_relations_source', 'to_pos_name') }}

    FROM source
    
)

SELECT *
FROM renamed