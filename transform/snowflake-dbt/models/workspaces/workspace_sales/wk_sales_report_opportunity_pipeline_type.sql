{{ config(alias='report_opportunity_pipeline_type') }}

WITH sfdc_opportunity_snapshot_history_xf AS (

  SELECT *
  FROM {{ref('wk_sales_sfdc_opportunity_snapshot_history_xf')}}  
  WHERE stage_name NOT IN ('9-Unqualified','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    AND is_deleted = 0
    AND is_edu_oss = 0

), sfdc_opportunity_xf AS (
  
  SELECT 
        opportunity_id,
        close_fiscal_quarter_date,
        stage_name,
        is_won,
        is_lost,
        is_open,
        order_type_stamped,
        sales_qualified_source,
        deal_category,
        deal_group,
        sales_team_cro_level,
        sales_team_rd_asm_level
  FROM {{ref('wk_sales_sfdc_opportunity_xf')}}  
  WHERE stage_name NOT IN ('9-Unqualified','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    AND is_deleted = 0
    AND is_edu_oss = 0

), today_date AS (
  
   SELECT DISTINCT first_day_of_fiscal_quarter AS current_fiscal_quarter_date,
                   fiscal_quarter_name_fy      AS current_fiscal_quarter_name,
                   90 - DATEDIFF(day, date_actual, last_day_of_fiscal_quarter)           AS current_day_of_fiscal_quarter_normalised
   FROM {{ ref('date_details') }} 
   WHERE date_actual = CURRENT_DATE 
  
  
), pipeline_type_quarter_start AS (

    SELECT 
        opportunity_id,
        snapshot_fiscal_quarter_date,
        close_fiscal_quarter_date       AS starting_close_fiscal_quarter_date,
        close_date                      AS starting_close_date,
        forecast_category_name          AS starting_forecast_category,
        net_arr                         AS starting_net_arr,
        stage_name                      AS starting_stage,
        snapshot_date                   AS starting_snapshot_date,
        is_won                          AS starting_is_won,
        is_open                         AS starting_is_open
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE snapshot_fiscal_quarter_date = close_fiscal_quarter_date -- closing in the same quarter of the snapshot
    AND stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    -- not created within quarter
    AND snapshot_fiscal_quarter_date <> pipeline_created_fiscal_quarter_date
    -- set day 5 as start of the quarter for pipeline purposes
    AND snapshot_day_of_fiscal_quarter_normalised = 5

), pipeline_type_quarter_created AS (

    SELECT 
        opportunity_id,
        pipeline_created_fiscal_quarter_date,
        min(snapshot_date)            AS created_snapshot_date
    FROM sfdc_opportunity_snapshot_history_xf
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
    -- pipeline created same quarter
      AND snapshot_fiscal_quarter_date = pipeline_created_fiscal_quarter_date
      AND pipeline_created_fiscal_quarter_date = close_fiscal_quarter_date  
      AND ((snapshot_fiscal_quarter_name = 'FY21-Q2' 
                    AND (snapshot_day_of_fiscal_quarter_normalised > 80
                          OR snapshot_day_of_fiscal_quarter_normalised < 45))
            OR snapshot_fiscal_quarter_name != 'FY21-Q2')
    GROUP BY 1, 2

), pipeline_type_pulled_in AS (

    SELECT 
        pull.opportunity_id,
        pull.snapshot_fiscal_quarter_date,
        min(pull.snapshot_date)            AS pulled_in_snapshot_date
    FROM sfdc_opportunity_snapshot_history_xf pull
    LEFT JOIN pipeline_type_quarter_start pipe_start
      ON pipe_start.opportunity_id = pull.opportunity_id
      AND pipe_start.snapshot_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
    LEFT JOIN pipeline_type_quarter_created pipe_created
      ON pipe_created.opportunity_id = pull.opportunity_id
      AND pipe_created.pipeline_created_fiscal_quarter_date = pull.snapshot_fiscal_quarter_date
    WHERE pull.stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified','00-Pre Opportunity','0-Pending Acceptance') 
      AND pull.snapshot_fiscal_quarter_date = close_fiscal_quarter_date
      AND pipe_start.opportunity_id IS NULL
      AND pipe_created.opportunity_id IS NULL  
    GROUP BY 1, 2

), pipeline_type_quarter_end AS (

    SELECT 
        opportunity_id,
        snapshot_fiscal_quarter_date,
        close_fiscal_quarter_date       AS end_close_fiscal_quarter_date,
        close_date                      AS end_close_date,
        forecast_category_name          AS end_forecast_category,
        net_arr                         AS end_net_arr,
        stage_name                      AS end_stage,
        is_won                          AS end_is_won,
        is_open                         AS end_is_open
    FROM sfdc_opportunity_snapshot_history_xf        
    WHERE (snapshot_day_of_fiscal_quarter_normalised = 90 
          OR snapshot_date = dateadd(day,-1,CURRENT_DATE))

), pipeline_type AS (

  SELECT 
        opp_snap.opportunity_id,
        opp_snap.close_fiscal_quarter_date,
        opp_snap.close_fiscal_quarter_name,

        pipe_start.starting_forecast_category,
        pipe_start.starting_net_arr,
        pipe_start.starting_stage,
        pipe_start.starting_close_date,

        pipe_end.end_forecast_category,
        pipe_end.end_net_arr,
        pipe_end.end_stage,
        pipe_end.end_is_open,
        pipe_end.end_is_won,
        pipe_end.end_close_date,

        -- pipeline type, identifies if the opty was there at the begging of the quarter or not
        CASE
          WHEN pipe_start.opportunity_id IS NOT NULL
            THEN '1. Starting'
          WHEN pipe_created.opportunity_id IS NOT NULL
            THEN '2. Created & Landed'
          WHEN pipe_pull.opportunity_id IS NOT NULL
            THEN '3. Pulled in'
          ELSE Null
        END                                                         AS pipeline_type,

        -- created pipe
        MAX(pipe_created.created_snapshot_date)                     AS pipeline_created_snapshot_date,

        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.net_arr
                  ELSE NULL
            END)                                                    AS pipeline_created_net_arr,
        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.stage_name
                  ELSE ''
            END)                                                    AS pipeline_created_stage,

        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.forecast_category_name
                  ELSE ''
            END)                                                    AS pipeline_created_forecast_category,
        
        MAX(CASE
              WHEN pipe_created.created_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.close_date
                  ELSE Null
            END)                                                    AS pipeline_created_close_date,

        -- pulled in pipe
        
        MAX(CASE
              WHEN pipe_pull.pulled_in_snapshot_date = opp_snap.snapshot_date
                  THEN opp_snap.net_arr
                  ELSE NULL
            END)                                                    AS pipeline_pull_net_arr,

        ----

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
  -- starting pipeline
  LEFT JOIN pipeline_type_quarter_start pipe_start
      ON pipe_start.opportunity_id = opp_snap.opportunity_id
      AND pipe_start.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- end pipeline
  LEFT JOIN pipeline_type_quarter_end pipe_end
      ON pipe_end.opportunity_id = opp_snap.opportunity_id
      AND pipe_end.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- created pipeline
  LEFT JOIN pipeline_type_quarter_created pipe_created
      ON pipe_created.opportunity_id = opp_snap.opportunity_id
      AND pipe_created.pipeline_created_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date
  -- pulled in pipeline
  LEFT JOIN pipeline_type_pulled_in pipe_pull
      ON pipe_pull.opportunity_id = opp_snap.opportunity_id
      AND pipe_pull.snapshot_fiscal_quarter_date = opp_snap.snapshot_fiscal_quarter_date
  -- closing in the same quarter of the snapshot
  WHERE opp_snap.snapshot_fiscal_quarter_date = opp_snap.close_fiscal_quarter_date 
    -- Exclude duplicate deals that were not created or started within the quarter
    AND (pipe_start.opportunity_id IS NOT NULL
          OR pipe_created.opportunity_id IS NOT NULL        
          OR pipe_pull.opportunity_id IS NOT NULL   
        )
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14

), report_opportunity_pipeline_type AS (

  SELECT 
  
        p.opportunity_id,
        -- descriptive cuts
        o.order_type_stamped,
        o.sales_qualified_source,
        o.deal_category,
        o.deal_group,
        o.sales_team_cro_level,
        o.sales_team_rd_asm_level,
        -- pipeline fields
        p.close_fiscal_quarter_date,
        p.close_fiscal_quarter_name,
        p.pipeline_type,

        CASE 
          WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
            THEN 1
          ELSE 0
        END                                                                     AS is_closed_in_quarter_flag,
        CASE 
            WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
                AND o.is_won = 1 
                    THEN '1. Closed Won'
            WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
                AND o.is_lost = 1
                    THEN '4. Closed Lost'
            WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
                AND o.is_open = 1
                    THEN '5. Open'
            WHEN p.close_fiscal_quarter_date = o.close_fiscal_quarter_date
                AND o.stage_name = '10-Duplicate'
                    THEN '6. Duplicate'
            WHEN p.close_fiscal_quarter_date <> o.close_fiscal_quarter_date
                AND p.max_snapshot_day_of_fiscal_quarter_normalised >= 75
                    THEN '2. Slipped'
            WHEN p.close_fiscal_quarter_date <> o.close_fiscal_quarter_date
                AND p.max_snapshot_day_of_fiscal_quarter_normalised < 75
                    THEN '3. Pushed Out' 
          ELSE NULL 
        END                                                                     AS pipe_resolution,

        -- basic net arr

        COALESCE(p.starting_net_arr,p.pipeline_created_net_arr,p.pipeline_pull_net_arr,0)   AS beg_net_arr,
        COALESCE(p.starting_stage,p.pipeline_created_stage)                                 AS beg_stage,
        COALESCE(p.starting_forecast_category,p.pipeline_created_forecast_category)         AS beg_forecast_category,
        COALESCE(p.starting_close_date,p.pipeline_created_close_date)                       AS beg_close_date,

        p.end_net_arr,                                                   
        p.end_stage,                                                
        p.end_forecast_category,
        p.end_close_date,
        p.end_is_won,
        p.end_is_open,

        p.end_net_arr - beg_net_arr                                             AS delta_net_arr,

        ----------
        -- extra fields for trouble shooting
        
        p.min_snapshot_day_of_fiscal_quarter_normalised,
        p.max_snapshot_day_of_fiscal_quarter_normalised,

        p.min_snapshot_date,
        p.max_snapshot_date,

        p.min_close_date,
        p.max_close_date,

        p.min_net_arr,
        p.max_net_arr,

        p.min_stage_name,
        p.max_stage_name,

        ----------
        current_date                                                    AS last_updated_at


  FROM pipeline_type p
  CROSS JOIN today_date 
  INNER JOIN sfdc_opportunity_xf o
      ON o.opportunity_id = p.opportunity_id
) 

SELECT *
FROM report_opportunity_pipeline_type