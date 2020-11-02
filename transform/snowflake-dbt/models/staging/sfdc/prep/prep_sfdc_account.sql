WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_account_source') }}
  WHERE NOT is_deleted;

)

SELECT
    TRIM(SPLIT_PART(tsp_sub_region, '-', 1))                                                                            AS tsp_sub_region_clean
  , TRIM(SPLIT_PART(tsp_region, '-', 1))                                                                                AS tsp_region_clean
  , TRIM(SPLIT_PART(REPLACE(tsp_area,'Mid - Atlantic', 'Mid Atlantic'), '-', 1))                                        AS tsp_area_clean
  , TRIM(SPLIT_PART(tsp_territory, '-', 1))                                                                             AS tsp_territory_clean
  , TRIM(SPLIT_PART(df_industry, '-', 1))                                                                               AS df_industry_clean
  , TRIM(SPLIT_PART(ultimate_parent_sales_segment, '-', 1))                                                             AS ultimate_parent_sales_segment_clean
  , account_id                                                                                                          AS crm_account_id
  , MAX(tsp_area_clean) OVER (Partition by UPPER(TRIM(tsp_area_clean)))                                                 AS dim_geo_area_name_source
  , MAX(tsp_region_clean) OVER (Partition by UPPER(TRIM(tsp_region_clean)))                                             AS dim_geo_region_name_source
  , MAX(tsp_sub_region_clean) OVER (Partition by UPPER(TRIM(tsp_sub_region_clean)))                                     AS dim_geo_sub_region_name_source
  , MAX(tsp_territory_clean) OVER (Partition by UPPER(TRIM(tsp_territory_clean)))                                       AS dim_sales_territory_name_source
  , MAX(df_industry_clean) OVER (Partition by UPPER(TRIM(df_industry_clean)))                                           AS dim_industry_name_source
  , MAX(ultimate_parent_sales_segment_clean ) OVER (Partition by UPPER(TRIM(ultimate_parent_sales_segment_clean )))     AS dim_sales_segment_name_source
FROM source