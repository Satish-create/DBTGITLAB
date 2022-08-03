{{ datasiren.search_sample_columns_samples_for_pattern(this.identifier, env_var('DBT_SNOWFLAKE_PROD_DATABASE'), 'TEXT', '100', '10', '^\w+@{1}[a-zA-Z_]+?\.{1}[a-zA-Z]{2,3}$', 'common') }}
