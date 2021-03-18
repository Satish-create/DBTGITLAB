{{ config(alias='report_opportunity_pipeline_type') }}

WITH sfdc_opportunity_snapshot_history_xf AS (

  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot

), sfdc_opportunity_xf AS (
  
  SELECT opportunity_id,
    close_fiscal_quarter_date,
    stage_name,
    is_won,
    is_lost,
    is_open
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
  
), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy      AS current_fiscal_quarter_name,
                   90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS current_day_of_fiscal_quarter_normalised
   FROM {{ ref('date_details') }} 
   WHERE date_actual = CURRENT_DATE 
  
), pipeline_type_quarter_start AS (

    SELECT 
      opportunity_id,
      close_fiscal_quarter_date
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
    AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    -- not created within quarter
    AND snapshot_fiscal_quarter_date <> pipeline_created_fiscal_quarter_date
    -- set day 5 as start of the quarter for pipeline purposes
    AND snapshot_day_of_fiscal_quarter_normalised = 5
    GROUP BY 1, 2

), pipeline_type_quarter_created AS (

    SELECT 
      opportunity_id,
      close_fiscal_quarter_date
    FROM sfdc_opportunity_snapshot_history_xf
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
    AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    -- pipeline created same quarter
      AND snapshot_fiscal_quarter_date = pipeline_created_fiscal_quarter_date
    GROUP BY 1, 2
  
), pipeline_type AS (

  SELECT 
      opp_snap.opportunity_id,
      opp_snap.close_fiscal_quarter_date,
      -- pipeline type, identifies if the opty was there at the begging of the quarter or not
      CASE
        WHEN pipe_start.opportunity_id IS NOT NULL
          THEN '1. Starting Pipeline'
        WHEN pipe_created.opportunity_id IS NOT NULL
          THEN '2. Created Pipeline'
        WHEN opp_snap.close_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
          THEN '3. Pulled in Pipeline'
        ELSE '4. Not in Quarter'
      END                                                         AS pipeline_type,
      MIN(opp_snap.snapshot_day_of_fiscal_quarter_normalised)     AS min_snapshot_day_of_fiscal_quarter_normalised,
      MAX(opp_snap.snapshot_day_of_fiscal_quarter_normalised)     AS max_snapshot_day_of_fiscal_quarter_normalised,
      MIN(opp_snap.snapshot_date)                                 AS min_snapshot_date,
      MAX(opp_snap.snapshot_date)                                 AS max_snapshot_date,
      MIN(opp_snap.close_date)                                    AS min_close_date,
      MAX(opp_snap.close_date)                                    AS max_close_date,
      MIN(opp_snap.net_arr)                                       AS min_net_arr,
      MAX(opp_snap.net_arr)                                       AS max_net_arr,
      MIN(opp_snap.stage_name)                                    AS min_stage_name,
      MAX(opp_snap.stage_name)                                    AS max_stage_name
  FROM sfdc_opportunity_snapshot_history_xf opp_snap
  LEFT JOIN pipeline_type_quarter_start pipe_start
      ON pipe_start.opportunity_id = opp_snap.opportunity_id
      AND pipe_start.close_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date
  LEFT JOIN pipeline_type_quarter_created pipe_created
      ON pipe_created.opportunity_id = opp_snap.opportunity_id
      AND pipe_created.close_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date
  GROUP BY 1, 2, 3

), report_opportunity_pipeline_type AS (

  SELECT p.*,
      CASE 
          WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
              AND o.is_open = 0
                  THEN '1. Closed'
          WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
              AND o.is_open = 1
                  THEN '4. Current Quarter'
          WHEN p.close_fiscal_quarter_date <> o.close_fiscal_quarter_date
              AND p.max_snapshot_day_of_fiscal_quarter_normalised >= 75
                  THEN '2. Slipped'
          WHEN p.close_fiscal_quarter_date <> o.close_fiscal_quarter_date
              AND p.max_snapshot_day_of_fiscal_quarter_normalised < 75
                  THEN '3. Pushed Out' 
        ELSE NULL 
      END                                      AS deal_resolution 

  FROM pipeline_type p
  CROSS JOIN today_date 
  INNER JOIN sfdc_opportunity_xf o
      ON o.opportunity_id = p.opportunity_id
) 

SELECT *
FROM report_opportunity_pipeline_type