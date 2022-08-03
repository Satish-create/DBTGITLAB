
{{ schema_union_limit('dotcom_usage_events_', 'prep_event', 'event_created_at', 800, database_name=env_var('DBT_SNOWFLAKE_PREP_DATABASE')) }}
