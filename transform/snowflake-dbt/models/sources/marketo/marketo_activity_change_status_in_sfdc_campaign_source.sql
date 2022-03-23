WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_status_in_sfdc_campaign') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      old_status                        AS old_status,
      new_status                        AS new_status

    FROM source

)

SELECT *
FROM renamed
