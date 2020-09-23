
{% set metric_type = 'all_time' %}
{% set columns_to_parse = ['usage_activity_by_stage_monthly', 'stats_used', 'usage_activity_by_stage_monthly'] %}

WITH data AS ( 
  
    SELECT * FROM {{ ref('version_usage_data')}}

)

, flattened AS (
    {% for column in columns_to_parse %}
      (

        SELECT 
          uuid                          AS instance_id, 
          id                            AS ping_id,
          created_at,
          path                          AS metric_path, 
          '{{ column }}' || '.' || path AS full_metrics_path,
          value                         AS metric_value
        FROM data,
        lateral flatten(input => {{ column }},
        recursive => true) 
        WHERE typeof(value) IN ('INTEGER', 'DECIMAL')
        ORDER BY created_at DESC

      )
      {% if not loop.last %}
        UNION 
      {% endif %}
      {% endfor %}

)

SELECT 
  flattened.instance_id,
  flattened.ping_id,
  created_at,
  metrics.*, 
  flattened.metric_value
FROM flattened
INNER JOIN {{ ref('sheetload_dev_section_metrics') }} AS metrics 
  ON flattened.full_metrics_path = metrics.metrics_path
    AND time_period = '{{metric_type}}'
