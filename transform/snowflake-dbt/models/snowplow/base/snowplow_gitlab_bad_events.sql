{{
  config(
    materialized='incremental'
  )
}}

WITH base as (

    SELECT *
    FROM {{ source('gitlab_snowplow', 'bad_events') }}
    WHERE length(JSONTEXT['errors']) > 0

    {%- if is_incremental() %}
      AND uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif -%}

), renamed AS (

    SELECT DISTINCT JSONTEXT [ 'line' ] :: string              AS base64_event,
                    TO_ARRAY(JSONTEXT [ 'errors' ])            AS error_array,
                    JSONTEXT [ 'failure_tstamp' ] :: timestamp AS failure_timestamp,
                    'GitLab'                                   AS infra_source,
                    uploaded_at
    FROM base

)

SELECT *
FROM renamed
ORDER BY failure_timestamp