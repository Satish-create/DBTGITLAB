{% set year_value = env_var('CURRENT_YEAR') %}
{% set month_value = env_var('CURRENT_MONTH') %}

{{config({
    "materialized":"incremental",
    "unique_key":"base64_event",
    "schema":"snowplow_" + year_value|string + '_' + month_value|string, 
  })
}}

WITH fishtown as (

    SELECT *
    FROM {{ ref('snowplow_fishtown_unnested_events') }}
    {% if is_incremental() %}
       WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

), gitlab as (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events') }}
    {% if is_incremental() %}
        WHERE uploaded_at > (SELECT max(uploaded_at) FROM {{ this }})
    {% endif %}

), unioned AS (

    SELECT *
    FROM gitlab

    UNION ALL

    SELECT *
    FROM fishtown

)

SELECT *
FROM unioned
