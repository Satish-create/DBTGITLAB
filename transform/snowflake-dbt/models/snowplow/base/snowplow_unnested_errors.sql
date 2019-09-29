{{config({
    "materialized":"table",
    "unique_key":"bad_event_surrogate",
    "schema": current_date_schema('snowplow')
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_bad_events') }}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_bad_events') }}

), unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
