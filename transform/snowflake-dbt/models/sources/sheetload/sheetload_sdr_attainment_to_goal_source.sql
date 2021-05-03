WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_attainment_to_goal') }}

), renamed as (

    SELECT
      current_month::DATE                   AS current_month,
      name::VARCHAR                         AS name,
      sdr_sfdc_name::VARCHAR                AS sdr_sfdc_name,
      role::VARCHAR                         AS role,
      status::VARCHAR                       AS status,
      region::VARCHAR                       AS region,
      segment::VARCHAR                      AS segment,
      type::VARCHAR                         AS type,
      total_leads_accepted::NUMBER          AS total_leads_accepted,
      accepted_leads::NUMBER                AS accepted_leads,
      accepted_leads_qualifying::NUMBER     AS accepted_leads_qualifying,
      accepted_leads_completed::NUMBER      AS accepted_leads_completed,
      leads_worked::NUMBER                  AS leads_worked,
      qualified_leads::NUMBER               AS qualified_leads,
      unqualified_leads::NUMBER             AS unqualified_leads,
      average_working_day_calls::NUMBER     AS average_working_day_calls,
      average_working_day_emails::NUMBER    AS average_working_day_emails,
      average_working_day_other::NUMBER     AS average_working_day_other,
      saos::NUMBER                          AS saos,
      quaterly_sao_target::NUMBER           AS quaterly_sao_target,
      quaterly_sao_variance::NUMBER         AS quaterly_sao_variance,
      average_time::NUMBER                  AS average_time


    FROM source
)

SELECT *
FROM renamed
