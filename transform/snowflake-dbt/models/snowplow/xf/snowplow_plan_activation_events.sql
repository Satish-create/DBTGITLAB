{{ config({
    "materialized": "incremental",
    "unique_key": "page_view_id"
    })
}}

{%- set event_ctes = ["issue_list_viewed",
                      "issue_viewed",
                      "board_viewed",
                      "epic_list_viewed",
                      "epic_viewed",
                      "roadmap_viewed",
                      "milestones_list_viewed",
                      "milestone_viewed",
                      "todo_viewed",
                      "personal_issues_viewed",
                      "notification_settings_viewed"
                      ]
-%}

WITH snowplow_page_views AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    page_view_start,
    page_url_path,
    page_view_id
  FROM analytics.snowplow_page_views
  WHERE page_view_start >= '2019-01-01'
  {% if is_incremental() %}
    AND page_view_start >= (SELECT MAX(event_date) FROM {{this}})
  {% endif %}

)

, issue_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'issue_list_viewed'       AS event_type,
    page_view_id


  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/issues(\/)?'

)

, issue_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'issue_viewed'              AS event_type,
    page_view_id


  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/issues\/[0-9]{1,}'

)

, board_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'board_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/boards\/[0-9]{1,}'

)

, epic_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'epic_list_viewed'        AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/epics(\/)?'

)

, epic_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'epic_viewed'         AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/epics\/[0-9]{1,}'
)

, roadmap_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'roadmap_viewed'        AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/roadmap_viewed(\/)?'
)


, label_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'label_list_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/labels(\/)?'

)

, milestones_list_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'milestones_list_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/milestones(\/)?'

)

, milestone_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'milestone_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '(\/([a-zA-Z-])*){2,}\/milestones\/[0-9]{1,}'

)

, todo_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'todo_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/dashboard\/todo(\/)?'

)

, personal_issues_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'personal_issues_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/dashboard\/issues(\/)?'

)

, notification_settings_viewed AS (

  SELECT
    user_snowplow_domain_id,
    user_custom_id,
    TO_DATE(page_view_start) AS event_date,
    page_url_path,
    'notification_settings_viewed'       AS event_type,
    page_view_id

  FROM snowplow_page_views
  WHERE page_url_path REGEXP '\/profile\/notifications(\/)?'

)

, unioned AS (
  {% for event_cte in event_ctes %}

    (
      SELECT
        *
      FROM {{ event_cte }}
    )

    {%- if not loop.last -%}
        UNION
    {%- endif %}

  {% endfor -%}

)

SELECT *
FROM unioned
