{{config({
    "materialized":"view"
  })
}}

-- depends on: {{ ref('snowplow_gitlab_events_experiment_contexts') }}

{{ schema_union_all('snowplow_', 'snowplow_gitlab_events_experiment_contexts', database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
