{{
  config(
    materialized = "table"
  )
}}

with base as (

    SELECT *,
     convert_timezone('America/Los_Angeles',convert_timezone('UTC',current_timestamp())) AS _last_dbt_run
    FROM {{ var("database") }}.salesforce_stitch.user

)

SELECT *
FROM base