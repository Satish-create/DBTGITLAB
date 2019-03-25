with source as (

    SELECT * FROM dbt_archive.sfdc_account_archived

), enriched as (

    SELECT *, 
            "scd_id" as unique_identifier,
            "dbt_updated_at" as dbt_last_updated_timestamp
    FROM source
    {{ dbt_utils.group_by(n=70) }}

)

SELECT * 
FROM enriched