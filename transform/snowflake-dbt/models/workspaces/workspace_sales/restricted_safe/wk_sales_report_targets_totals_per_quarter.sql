
{{ config(alias='report_targets_totals_per_quarter') }}

WITH date_details AS (
  
  SELECT *
  FROM  {{ ref('wk_sales_date_details') }} 
  
), sfdc_opportunity_snapshot_history_xf AS (
  
  SELECT *
  FROM  {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  WHERE is_deleted = 0
    AND is_edu_oss = 0
  
), mart_sales_funnel_target AS (
  
  SELECT *
  FROM {{ref('wk_sales_mart_sales_funnel_target')}}

), today_date AS (

   SELECT DISTINCT first_day_of_fiscal_quarter              AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy                   AS current_fiscal_quarter_name,
                   day_of_fiscal_quarter_normalised         AS current_day_of_fiscal_quarter_normalised
   FROM date_details 
   WHERE date_actual = CURRENT_DATE

), funnel_targets_per_quarter AS (
  
  SELECT 
    target_fiscal_quarter_name,
    target_fiscal_quarter_date,
    -------------------------
    -- keys
    sales_team_cro_level,
    sales_team_vp_level,
    sales_team_avp_rd_level,
    sales_team_asm_level,

    sales_team_rd_asm_level,
    report_user_segment_geo_region_area,
    -------------------------
    sales_qualified_source,
    deal_group,
    SUM(CASE 
          WHEN kpi_name = 'Net ARR' 
            THEN allocated_target
           ELSE 0 
        END)                        AS target_net_arr,
    SUM(CASE 
          WHEN kpi_name = 'Deals' 
            THEN allocated_target
           ELSE 0 
        END)                        AS target_deal_count,
    SUM(CASE 
          WHEN kpi_name = 'Net ARR Pipeline Created' 
            THEN allocated_target
           ELSE 0 
        END)                        AS target_pipe_generation_net_arr
  FROM mart_sales_funnel_target
  GROUP BY 1,2,3,4,5,6,7,8,9,10

), totals_per_quarter AS (
  
 SELECT 
        opp_snapshot.snapshot_fiscal_quarter_name   AS close_fiscal_quarter_name,
        opp_snapshot.snapshot_fiscal_quarter_date   AS close_fiscal_quarter_date,
        -------------------------
        -- keys
        opp_snapshot.sales_team_cro_level,
        opp_snapshot.sales_team_vp_level,
        opp_snapshot.sales_team_avp_rd_level,
        opp_snapshot.sales_team_asm_level,

        opp_snapshot.sales_team_rd_asm_level,
        opp_snapshot.report_user_segment_geo_region_area,
        -------------------------
        opp_snapshot.sales_qualified_source,
        opp_snapshot.deal_group,
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.booked_net_arr
                ELSE 0
             END)                                               AS total_booked_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.churned_contraction_net_arr
                ELSE 0
             END)                                               AS total_churned_contraction_net_arr,       
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.booked_deal_count
                ELSE 0
             END)                                               AS total_booked_deal_count,
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.churned_contraction_deal_count
                ELSE 0
        END)                                                    AS total_churned_contraction_deal_count,   
        
        -- Pipe gen totals
        SUM(CASE 
                WHEN opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_created_pipeline_flag = 1
                    THEN opp_snapshot.created_in_snapshot_quarter_net_arr
                ELSE 0
             END )                                              AS total_pipe_generation_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.pipeline_created_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_created_pipeline_flag = 1
                    THEN opp_snapshot.created_in_snapshot_quarter_deal_count
                ELSE 0
             END )                                              AS total_pipe_generation_deal_count,
        
        -- SAO totals per quarter
        SUM(CASE 
                WHEN opp_snapshot.sales_accepted_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_sao_flag = 1
                    THEN opp_snapshot.net_arr
                ELSE 0
             END )                                              AS total_sao_generation_net_arr,
        SUM(CASE 
                WHEN opp_snapshot.sales_accepted_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                  AND opp_snapshot.is_eligible_sao_flag = 1
                    THEN opp_snapshot.calculated_deal_count
                ELSE 0
             END )                                              AS total_sao_generation_deal_count,
        
        -- Created & Landed totals
        SUM(CASE 
                WHEN opp_snapshot.close_fiscal_quarter_date = opp_snapshot.snapshot_fiscal_quarter_date
                    THEN opp_snapshot.created_and_won_same_quarter_net_arr
                ELSE 0
             END)                                               AS total_created_and_booked_same_quarter_net_arr
   FROM sfdc_opportunity_snapshot_history_xf opp_snapshot
   WHERE opp_snapshot.is_excluded_flag = 0
     AND opp_snapshot.is_deleted = 0
     AND opp_snapshot.snapshot_day_of_fiscal_quarter_normalised = 90
   GROUP BY 1,2,3,4,5,6,7,8,9,10

), base_fields AS (

  SELECT 
        target_fiscal_quarter_name       AS close_fiscal_quarter_name,
        target_fiscal_quarter_date       AS close_fiscal_quarter_date,
        sales_team_cro_level,
        sales_team_vp_level,
        sales_team_avp_rd_level,
        sales_team_asm_level,
        sales_qualified_source,
        deal_group,
        sales_team_rd_asm_level,
        report_user_segment_geo_region_area
  FROM funnel_targets_per_quarter
  UNION
  SELECT 
        close_fiscal_quarter_name,
        close_fiscal_quarter_date,
        sales_team_cro_level,
        sales_team_vp_level,
        sales_team_avp_rd_level,
        sales_team_asm_level,
        sales_qualified_source,
        deal_group,
        sales_team_rd_asm_level,
        report_user_segment_geo_region_area
  FROM totals_per_quarter
  
), consolidated_targets_totals AS (
  
  SELECT
     --------
     -- Keys
     base.close_fiscal_quarter_name,
     base.close_fiscal_quarter_date,
     base.sales_team_cro_level,
     base.sales_team_vp_level,
     base.sales_team_avp_rd_level,
     base.sales_team_asm_level,
     base.sales_qualified_source,
     base.deal_group,

     base.sales_team_rd_asm_level,
     base.report_user_segment_geo_region_area,
     -----
     
     report_date.fiscal_year    AS close_fiscal_year,
     
     COALESCE(target.target_net_arr,0)                      AS target_net_arr,
     COALESCE(target.target_deal_count,0)                   AS target_deal_count,
     COALESCE(target.target_pipe_generation_net_arr,0)      AS target_pipe_generation_net_arr, 
  
     COALESCE(total.total_booked_net_arr,0)                    AS total_booked_net_arr,
     COALESCE(total.total_churned_contraction_net_arr,0)       AS total_churned_contraction_net_arr,
     COALESCE(total.total_booked_deal_count,0)                 AS total_booked_deal_count,
     COALESCE(total.total_churned_contraction_deal_count,0)    AS total_churned_contraction_deal_count,     
     COALESCE(total.total_pipe_generation_net_arr,0)           AS total_pipe_generation_net_arr,
     COALESCE(total.total_pipe_generation_deal_count,0)        AS total_pipe_generation_deal_count,

     COALESCE(total.total_sao_generation_net_arr,0)             AS total_sao_generation_net_arr,
     COALESCE(total.total_sao_generation_deal_count,0)          AS total_sao_generation_deal_count,

     COALESCE(total.total_created_and_booked_same_quarter_net_arr,0)  AS total_created_and_booked_same_quarter_net_arr,
  

    -- check if we are in the current quarter or not. If not, use total, if we are use taret
     CASE
      WHEN today_date.current_fiscal_quarter_date <= base.close_fiscal_quarter_date
        THEN target.target_net_arr
      ELSE total.total_booked_net_arr
     END                  AS calculated_target_net_arr, 
     CASE
      WHEN today_date.current_fiscal_quarter_date <= base.close_fiscal_quarter_date
        THEN target.target_deal_count
      ELSE total.total_booked_deal_count
     END                  AS calculated_target_deal_count,  
     CASE
      WHEN today_date.current_fiscal_quarter_date <= base.close_fiscal_quarter_date
        THEN target.target_pipe_generation_net_arr
      ELSE total.total_pipe_generation_net_arr
     END                  AS calculated_target_pipe_generation
  FROM base_fields base
  CROSS JOIN today_date
  INNER JOIN date_details report_date
    ON report_date.date_actual = base.close_fiscal_quarter_date
  LEFT JOIN funnel_targets_per_quarter target
     ON target.target_fiscal_quarter_date = base.close_fiscal_quarter_date
      AND target.sales_team_cro_level = base.sales_team_cro_level
      AND target.sales_team_vp_level = base.sales_team_vp_level
      AND target.sales_team_avp_rd_level = base.sales_team_avp_rd_level
      AND target.sales_team_asm_level = base.sales_team_asm_level
      AND target.sales_qualified_source = base.sales_qualified_source
      AND target.deal_group = base.deal_group
      AND target.sales_team_rd_asm_level = base.sales_team_rd_asm_level
      AND target.report_user_segment_geo_region_area = base.report_user_segment_geo_region_area
  -- quarterly total
  LEFT JOIN totals_per_quarter total
     ON total.close_fiscal_quarter_date = base.close_fiscal_quarter_date
      AND total.sales_team_cro_level = base.sales_team_cro_level
      AND total.sales_team_vp_level = base.sales_team_vp_level
      AND total.sales_team_avp_rd_level = base.sales_team_avp_rd_level
      AND total.sales_team_asm_level = base.sales_team_asm_level
      AND total.sales_qualified_source = base.sales_qualified_source
      AND total.deal_group = base.deal_group
      AND total.sales_team_rd_asm_level = base.sales_team_rd_asm_level
      AND total.report_user_segment_geo_region_area = base.report_user_segment_geo_region_area
  
)
SELECT * 
FROM consolidated_targets_totals