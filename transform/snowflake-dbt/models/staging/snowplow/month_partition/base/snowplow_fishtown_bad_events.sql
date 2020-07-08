{% set year_value = var('year', run_started_at.strftime('%Y')) %}
{% set month_value = var('month', run_started_at.strftime('%m')) %}

{{config({
    "unique_key":"bad_event_surrogate"
  })
}}

WITH base as (

    SELECT *
    FROM {{ ref('fishtown_snowplow_bad_events_source') }}
    WHERE length(JSONTEXT['errors']) > 0
      AND date_part(month, JSONTEXT['failure_tstamp']::timestamp) = '{{ month_value }}'
      AND date_part(year, JSONTEXT['failure_tstamp']::timestamp) = '{{ year_value }}'

), renamed AS (

    SELECT 
      DISTINCT JSONTEXT['line']::string       AS base64_event,
      TO_ARRAY(JSONTEXT['errors'])            AS error_array,
      JSONTEXT[ 'failure_tstamp']::timestamp  AS failure_timestamp,
      'Fishtown'                              AS infra_source,
      uploaded_at,
      {{ dbt_utils.surrogate_key('base64_event', 'failure_timestamp','error_array') }} 
                                              AS bad_event_surrogate
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp
