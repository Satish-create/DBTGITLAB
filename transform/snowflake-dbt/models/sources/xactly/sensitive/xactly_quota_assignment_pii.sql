WITH source AS (

    SELECT *
    FROM {{ ref('xactly_quota_assignment_source') }}

), renamed AS (

    SELECT

      amount,
      amount_unit_type_id,
      assignment_id,
      {{ nohash_sensitive_columns('xactly_quota_assignment_source', 'assignment_name') }},
      assignment_type,
      created_by_id,
      created_by_name,
      created_date,
      description,
      effective_end_period_id,
      effective_start_period_id,
      is_active,
      modified_by_id,
      modified_by_name,
      modified_date,
      period_id,
      qta_asgnmt_id,
      quota_assignment_id,
      quota_id

    FROM source

)

SELECT *
FROM renamed