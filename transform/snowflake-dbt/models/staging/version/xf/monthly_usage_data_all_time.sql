WITH data AS ( 
  
    SELECT * FROM {{ ref('usage_data_all_time_flattened')}}

)

, transformed AS (

    SELECT 
        *,
        DATE_TRUNC('month', created_at) AS created_month
    FROM data
    QUALIFY ROW_NUMBER() OVER (PARTITION BY uuid, clean_metrics_name, created_month ORDER BY created_at DESC) = 1

)

, monthly AS (

  SELECT 
    *,
    metric_value 
      - COALESCE(LEAD(metric_value) OVER (
                                          PARTITION BY uuid, clean_metrics_name 
                                          ORDER BY created_month
                                        ), 0) AS monthly_metric_value
  FROM transformed

)

SELECT 
  id,
  uuid,
  created_month,
  full_metrics_path,
  stage,
  clean_metrics_name,
  metric_type,
  IFF(metric_value < 0, 0, monthly_metric_value) AS monthly_metric_value
FROM monthly
