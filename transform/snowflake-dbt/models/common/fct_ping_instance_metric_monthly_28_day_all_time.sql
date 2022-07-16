{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_ping_instance', 'dim_ping_instance'),
    ('fct_ping_instance_metric_monthly', 'fct_ping_instance_metric_monthly')
    ])

}},

time_frame_28_day_metrics AS (

  SELECT
    fct_ping_instance_metric_monthly.ping_instance_metric_id AS ping_instance_metric_id,
    fct_ping_instance_metric_monthly.dim_ping_instance_id AS dim_ping_instance_id,
    fct_ping_instance_metric_monthly.metrics_path AS metrics_path,
    fct_ping_instance_metric_monthly.has_timed_out AS has_timed_out,
    fct_ping_instance_metric_monthly.dim_product_tier_id AS dim_product_tier_id,
    fct_ping_instance_metric_monthly.dim_subscription_id AS dim_subscription_id,
    fct_ping_instance_metric_monthly.dim_location_country_id AS dim_location_country_id,
    fct_ping_instance_metric_monthly.dim_ping_date_id AS dim_ping_date_id,
    fct_ping_instance_metric_monthly.dim_instance_id AS dim_instance_id,
    fct_ping_instance_metric_monthly.dim_host_id AS dim_host_id,
    fct_ping_instance_metric_monthly.dim_installation_id AS dim_installation_id,
    fct_ping_instance_metric_monthly.dim_license_id AS dim_license_id,
    fct_ping_instance_metric_monthly.ping_created_at AS ping_created_at,
    fct_ping_instance_metric_monthly.umau_value AS umau_value,
    fct_ping_instance_metric_monthly.dim_subscription_license_id AS dim_subscription_license_id,
    fct_ping_instance_metric_monthly.data_source AS data_source,
    fct_ping_instance_metric_monthly.time_frame AS time_frame,
    fct_ping_instance_metric_monthly.metric_value AS original_metric_value,
    fct_ping_instance_metric_monthly.metric_value AS monthly_metric_value
  FROM fct_ping_instance_metric_monthly
  INNER JOIN dim_ping_instance
    ON fct_ping_instance_metric_monthly.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
  WHERE time_frame = '28d'
    AND has_timed_out = FALSE
    AND metric_value IS NOT NULL

),

time_frame_all_time_metrics AS (
    
    SELECT
      fct_ping_instance_metric_monthly.ping_instance_metric_id AS ping_instance_metric_id,
      fct_ping_instance_metric_monthly.dim_ping_instance_id AS dim_ping_instance_id,
      fct_ping_instance_metric_monthly.metrics_path AS metrics_path,
      fct_ping_instance_metric_monthly.has_timed_out AS has_timed_out,
      fct_ping_instance_metric_monthly.dim_product_tier_id AS dim_product_tier_id,
      fct_ping_instance_metric_monthly.dim_subscription_id AS dim_subscription_id,
      fct_ping_instance_metric_monthly.dim_location_country_id AS dim_location_country_id,
      fct_ping_instance_metric_monthly.dim_ping_date_id AS dim_ping_date_id,
      fct_ping_instance_metric_monthly.dim_instance_id AS dim_instance_id,
      fct_ping_instance_metric_monthly.dim_host_id AS dim_host_id,
      fct_ping_instance_metric_monthly.dim_installation_id AS dim_installation_id,
      fct_ping_instance_metric_monthly.dim_license_id AS dim_license_id,
      fct_ping_instance_metric_monthly.ping_created_at AS ping_created_at,
      fct_ping_instance_metric_monthly.umau_value AS umau_value,
      fct_ping_instance_metric_monthly.dim_subscription_license_id AS dim_subscription_license_id,
      fct_ping_instance_metric_monthly.data_source AS data_source,
      fct_ping_instance_metric_monthly.time_frame AS time_frame,
      fct_ping_instance_metric_monthly.metric_value AS original_metric_value,
      {{ monthly_all_time_metric_calc('fct_ping_instance_metric_monthly.metric_value', 'fct_ping_instance_metric_monthly.dim_installation_id',
                                      'fct_ping_instance_metric_monthly.metrics_path', 'fct_ping_instance_metric_monthly.ping_created_at') }}
    FROM fct_ping_instance_metric_monthly
    INNER JOIN dim_ping_instance
      ON fct_ping_instance_metric_monthly.dim_ping_instance_id = dim_ping_instance.dim_ping_instance_id
    WHERE time_frame = 'all'
      AND has_timed_out = FALSE
      AND metric_value IS NOT NULL  
      AND typeof(metric_value) IN ('INTEGER', 'DECIMAL')  
      
),

final AS (
    
    SELECT *
    FROM time_frame_28_day_metrics
    
    UNION ALL
    
    SELECT *
    FROM time_frame_all_time_metrics
    
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2022-07-20",
    updated_date="2022-07-20"
) }}
