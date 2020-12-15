{{ config({
        "materialized": "incremental"
    })
}}

{{ datasiren.search_sample_columns_samples_for_pattern(this.identifier, env_var('SNOWFLAKE_LOAD_DATABASE'), 'TEXT', 25, 25, '.*\\d\\d\\d-\\d\\d-\\d\\d\\d\\d.*') }}