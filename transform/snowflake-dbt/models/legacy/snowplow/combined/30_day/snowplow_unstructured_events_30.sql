-- depends on: {{ ref('snowplow_unstructured_events') }}

{{ schema_union_limit('snowplow_', 'snowplow_unstructured_events', 'derived_tstamp', 30, database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
