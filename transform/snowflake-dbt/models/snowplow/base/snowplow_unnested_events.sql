{{config({
    "materialized":"table",
    "unique_key":"base64_event",
    "schema":current_date_schema('snowplow')
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events') }}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}

),

unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
