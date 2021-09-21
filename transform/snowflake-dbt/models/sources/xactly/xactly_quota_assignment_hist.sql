WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota_assignment_hist') }}

), renamed AS (

    SELECT

      amount,
      amount_unit_type_id,
      assignment_id,
      md5(assignment_name) AS assignment_uuid,
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
      object_id,
      period_id,
      quota_assignment_id,
      quota_id

    FROM source

)

SELECT *
FROM renamed