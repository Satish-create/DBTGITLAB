WITH data AS ( 
  
    SELECT * FROM {{ ref('usage_data_28_days_flattened')}}

)

, transformed AS (

    SELECT 
        *,
        DATE_TRUNC('month', created_at) AS created_month
    FROM data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, clean_metrics_name, created_month ORDER BY created_at DESC) = 1

)

SELECT *
FROM transformed
