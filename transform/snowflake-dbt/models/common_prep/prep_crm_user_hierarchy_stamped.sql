{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('sfdc_user_snapshots_source', 'sfdc_user_snapshots_source'),
    ('sheetload_sales_funnel_targets_matrix_source', 'sheetload_sales_funnel_targets_matrix_source')
])}}

, fiscal_months AS (

    SELECT DISTINCT
      fiscal_month_name_fy,
      fiscal_year,
      first_day_of_month
    FROM dim_date
  
), base_scd AS (  
/*
  Find the minimum valid from and valid to dates for each combo of segment-geo-region-area
*/

    SELECT 
      user_segment, 
      user_geo, 
      user_region, 
      user_area, 
      COALESCE(user_segment_geo_region_area,CONCAT(user_segment,'-' , user_geo, '-', user_region, '-', user_area)) AS user_segment_geo_region_area,
      MIN(dbt_valid_from)   AS valid_from, 
      MAX(dbt_valid_to)     AS valid_to
    FROM sfdc_user_snapshots_source
    {{ dbt_utils.group_by(n=5) }}
  
), base_scd_spined AS (
/*
  Expand the slowly changing dimension to the daily grain and add flags to indicate the last user hierarchy (segement-geo-region-area) in a fiscal year as well as the last user area (user_area)
  in a fiscal year. These will be used to join to the sales funnel target model. FY22 targets were set at the user_area level, while the FY23 targets (and beyond) will be set at the 
  segment-geo-region-area grain. 
*/

    SELECT 
      base_scd.*,
      dim_date.date_actual                                                                                                          AS snapshot_date,
      dim_date.fiscal_year,
      IFF(row_number() OVER (PARTITION BY user_area, user_segment_geo_region_area, fiscal_year ORDER BY date_actual DESC)=1, 1, 0)  AS is_last_user_hierarchy_in_fiscal_year,
      IFF(row_number() OVER (PARTITION BY user_area, fiscal_year ORDER BY valid_to DESC, snapshot_date DESC)=1, 1, 0)               AS is_last_user_area_in_fiscal_year
    FROM base_scd
    INNER JOIN dim_date
      ON dim_date.date_actual >= base_scd.valid_from 
        AND dim_date.date_actual < base_scd.valid_to
    WHERE user_area IS NOT NULL

), user_hierarchy_sheetload AS (

    SELECT DISTINCT 
      fiscal_months.fiscal_year,
      sheetload_sales_funnel_targets_matrix_source.user_segment,
      sheetload_sales_funnel_targets_matrix_source.user_geo,
      sheetload_sales_funnel_targets_matrix_source.user_region,
      sheetload_sales_funnel_targets_matrix_source.user_area,
      CONCAT(sheetload_sales_funnel_targets_matrix_source.user_segment, 
             sheetload_sales_funnel_targets_matrix_source.user_geo, 
             sheetload_sales_funnel_targets_matrix_source.user_region, 
             sheetload_sales_funnel_targets_matrix_source.user_area)        AS user_segment_geo_region_area,
      1                                                                     AS is_last_user_hierarchy_in_fiscal_year,
      1                                                                     AS is_last_user_area_in_fiscal_year
    FROM sheetload_sales_funnel_targets_matrix_source
    INNER JOIN fiscal_months
      ON sheetload_sales_funnel_targets_matrix_source.month = fiscal_months.fiscal_month_name_fy
    WHERE sheetload_sales_funnel_targets_matrix_source.user_area != 'N/A'
      AND sheetload_sales_funnel_targets_matrix_source.user_area IS NOT NULL
  
), unioned AS (
/*
  Filter the spined slowly changing dimension to only the last user hierarchy and user area in a given fiscal year. Union with the distinct user-segment-geo-region-area combinations
  from the target spreadsheet to ensure fidelity with the targets.
*/

    SELECT 
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_segment_geo_region_area,
      fiscal_year,
      is_last_user_hierarchy_in_fiscal_year,
      is_last_user_area_in_fiscal_year
    FROM base_scd_spined
    WHERE is_last_user_hierarchy_in_fiscal_year = 1 
      OR is_last_user_area_in_fiscal_year = 1 

    UNION

    SELECT 
      user_segment,
      user_geo,
      user_region,
      user_area,
      user_segment_geo_region_area,
      fiscal_year,
      is_last_user_hierarchy_in_fiscal_year,
      is_last_user_area_in_fiscal_year
    FROM user_hierarchy_sheetload
    WHERE is_last_user_hierarchy_in_fiscal_year = 1 
      OR is_last_user_area_in_fiscal_year = 1 

), final AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['user_segment_geo_region_area']) }}   AS dim_crm_user_hierarchy_stamped_id,
      {{ dbt_utils.surrogate_key(['user_segment']) }}                   AS dim_crm_opp_owner_sales_segment_stamped_id,
      user_segment                                                      AS crm_opp_owner_sales_segment_stamped,
      {{ dbt_utils.surrogate_key(['user_geo']) }}                       AS dim_crm_opp_owner_geo_stamped_id,
      user_geo                                                          AS crm_opp_owner_geo_stamped,
      {{ dbt_utils.surrogate_key(['user_region']) }}                    AS dim_crm_opp_owner_region_stamped_id,
      user_region                                                       AS crm_opp_owner_region_stamped,
      {{ dbt_utils.surrogate_key(['user_area']) }}                      AS dim_crm_opp_owner_area_stamped_id,
      user_area                                                         AS crm_opp_owner_area_stamped,
      user_segment_geo_region_area                                      AS crm_opp_owner_user_segment_geo_region_area_stamped,
      fiscal_year,
      is_last_user_hierarchy_in_fiscal_year,
      is_last_user_area_in_fiscal_year
    FROM unioned

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@michellecooper",
    created_date="2021-01-05",
    updated_date="2022-02-11"
) }}
