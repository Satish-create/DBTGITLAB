{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_unnested_errors') }}

{{ schema_union_all('snowplow_', 'snowplow_unnested_errors', database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
