{{
    config(
        materialized='incremental',
        unique_key='snowflake_query_id'
    )
}}

WITH source AS(
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY query_id ORDER BY query_id) AS dupcnt
    FROM {{source('snowflake_account_usage','query_history')}}
)

, deduped AS(
    SELECT *
    FROM source
    WHERE dupcnt = 1
) 

, renamed AS (

    SELECT 
        query_id 			AS snowflake_query_id,

        -- Foreign Keys
        database_id			AS database_id,
        schema_id			AS schema_id,
        session_id			AS snowflake_session_id,
        warehouse_id		AS warehouse_id,

        -- Logical Info
        database_name		AS database_name,
        query_text			AS query_text,
        role_name			AS snowflake_role_name,
        rows_produced		AS rows_produced,
        schema_name			AS schema_name,
        user_name			AS snowflake_user_name,
        warehouse_name		AS warehouse_name,

        -- metadata 
        end_time			AS query_end_time,
        start_time			AS query_start_time
    FROM deduped


{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  WHERE snowflake_query_start_time > (SELECT MAX(snowflake_query_start_time) from {{ this }})

{% endif %}

)

SELECT * 
from renamed