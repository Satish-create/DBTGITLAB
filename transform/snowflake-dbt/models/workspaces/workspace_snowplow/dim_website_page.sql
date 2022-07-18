{{ 
    config(
        materialized='table'
    ) 
}}

{{ 
    simple_cte([
    ('events', 'snowplow_unnested_events_all')
    ])

}}

, page AS (

    SELECT DISTINCT
      app_id,
      page_urlhost,
      REGEXP_REPLACE(page_urlpath, '^[^a-zA-Z]*|[0-9]|(\-\/+)|\.html$|\/$', '') AS clean_urlpath,
      SPLIT_PART(clean_urlpath, '/' ,1)                                         AS page_group,
      SPLIT_PART(clean_urlpath, '/' ,2)                                         AS page_type,
      SPLIT_PART(clean_urlpath, '/' ,3)                                         AS page_sub_type,
      refr_medium                                                               AS referrer_medium
    FROM events
    WHERE event IN ('struct', 'page_view', 'unstruct')
    AND page_urlpath IS NOT NULL

), dim_with_pk AS (

    SELECT DISTINCT
      --surrogate_key
      {{ dbt_utils.surrogate_key(['page_urlhost','clean_urlpath']) }}     AS dim_website_page_sk,
      app_id,
      page_urlhost,
      RTRIM(clean_urlpath, '/')                                           AS clean_urlpath,
      page_group,
      page_type,
      page_sub_type,
      referrer_medium
    FROM page

)

SELECT *
FROM dim_with_pk
