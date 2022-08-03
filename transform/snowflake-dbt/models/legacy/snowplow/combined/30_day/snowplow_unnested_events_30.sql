-- depends on: {{ ref('snowplow_unnested_events') }}

{{ schema_union_limit('snowplow_', 'snowplow_unnested_events', 'derived_tstamp', 30, database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
