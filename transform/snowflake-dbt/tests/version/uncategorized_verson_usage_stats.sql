WITH live_data AS (

	SELECT DISTINCT
    full_ping_name
	FROM {{ref('version_usage_stats_list')}}

), category_mapping as (

	SELECT DISTINCT
    stats_used_key_name
	FROM {{ref('version_metrics_to_stage_mapping')}}

)

SELECT *
FROM live_data
  FULL OUTER JOIN category_mapping
    ON live_data.full_ping_name = category_mapping.stats_used_key_name
WHERE (category_mapping.stats_used_key_name IS NULL
  OR live_data.full_ping_name IS NULL) 
