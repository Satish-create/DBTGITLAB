{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_unstructured_events') }}

{{ schema_union_all('snowplow_', 'snowplow_unstructured_events', database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
