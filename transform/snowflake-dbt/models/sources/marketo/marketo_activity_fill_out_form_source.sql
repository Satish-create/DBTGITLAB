WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_fill_out_form') }}

), renamed AS (

    SELECT

      id                                AS id,
      lead_id                           AS lead_id,
      activity_date                     AS activity_date,
      activity_type_id                  AS activity_type_id,
      campaign_id                       AS campaign_id,
      primary_attribute_value_id        AS primary_attribute_value_id,
      primary_attribute_value           AS primary_attribute_value,
      form_fields                       AS form_fields,
      client_ip_address                 AS client_ip_address,
      webpage_id                        AS webpage_id,
      user_agent                        AS user_agent,
      query_parameters                  AS query_parameters,
      referrer_url                      AS referrer_url

    FROM source

)

SELECT *
FROM renamed
