-- depends on: {{ ref('snowplow_page_views') }}

{{ schema_union_limit('snowplow_', 'snowplow_page_views', 'page_view_start', 90, database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
