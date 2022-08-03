{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_sessions') }}

{{ schema_union_all('snowplow_', 'snowplow_sessions', database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
