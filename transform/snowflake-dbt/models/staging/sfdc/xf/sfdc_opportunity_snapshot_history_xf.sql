/*

2020-09-15
It incorporates flags added to support the pipeline velocity report of @fkurniadi 
and the forecasting model for Commercial of Sales Strategy

*/ 
WITH RECURSIVE date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ ref('sfdc_accounts_xf') }}

), sfdc_opportunity_snapshot_history AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history') }}

), sfdc_opportunity_xf AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_xf') }}

), sfdc_users_xf AS (

    SELECT * 
    FROM {{ref('sfdc_users_xf')}}

), sales_admin_bookings_hierarchy AS (
    SELECT
        opportunity_id,
        owner_id,
        'CRO'                                                           AS level_1,
        CASE account_owner_team_stamped
            WHEN 'APAC'                 THEN 'VP Ent'
            WHEN 'Commercial'           THEN 'VP Comm SMB'
            WHEN 'Commercial - MM'      THEN 'VP Comm MM'
            WHEN 'Commercial - SMB'     THEN 'VP Comm SMB'
            WHEN 'EMEA'                 THEN 'VP Ent'
            WHEN 'MM - APAC'            THEN 'VP Comm MM'
            WHEN 'MM - East'            THEN 'VP Comm MM'
            WHEN 'MM - EMEA'            THEN 'VP Comm MM'
            WHEN 'MM - West'            THEN 'VP Comm MM'
            WHEN 'MM-EMEA'              THEN 'VP Comm MM'
            WHEN 'Public Sector'        THEN 'VP Ent'
            WHEN 'SMB'                  THEN 'VP Comm SMB'
            WHEN 'SMB - International'  THEN 'VP Comm SMB'
            WHEN 'SMB - US'             THEN 'VP Comm SMB'
            WHEN 'US East'              THEN 'VP Ent'
            WHEN 'US West'              THEN 'VP Ent'
            ELSE NULL END                                                  AS level_2,
        CASE account_owner_team_stamped
            WHEN 'APAC'                 THEN 'RD APAC'
            WHEN 'EMEA'                 THEN 'RD EMEA'
            WHEN 'MM - APAC'            THEN 'ASM - MM - APAC'
            WHEN 'MM - East'            THEN 'ASM - MM - East'
            WHEN 'MM - EMEA'            THEN 'ASM - MM - EMEA'
            WHEN 'MM - West'            THEN 'ASM - MM - West'
            WHEN 'MM-EMEA'              THEN 'ASM - MM - EMEA'
            WHEN 'Public Sector'        THEN 'RD PubSec'
            WHEN 'US East'              THEN 'RD US East'
            WHEN 'US West'              THEN 'RD US West'
            ELSE NULL END                                                   AS level_3
    FROM sfdc_opportunity_xf
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), final AS (

    SELECT h.date_actual                                                                                AS snapshot_date,

       --snapshot date helpers
        ds.first_day_of_month                                                                           AS snapshot_month,
        ds.fiscal_year                                                                                  AS snapshot_fiscal_year,
        ds.fiscal_quarter_name_fy                                                                       AS snapshot_fiscal_quarter,
        ds.first_day_of_fiscal_quarter                                                                  AS snapshot_fiscal_quarter_date,

       --close date helpers
        d.first_day_of_month                                                                            AS close_month,
        d.fiscal_year                                                                                   AS close_fiscal_year,
        d.fiscal_quarter_name_fy                                                                        AS close_fiscal_quarter,
        d.first_day_of_fiscal_quarter                                                                   AS close_fiscal_quarter_date,
        h.forecast_category_name                                                                        AS forecast_category_name,                  
        h.opportunity_id,
        h.owner_id                                                                                      AS owner_id,
        o.opportunity_owner_manager,                     
        h.stage_name,
        h.sales_type,
        h.is_deleted,
        a.tsp_region,
        a.tsp_sub_region,

        --********************************************************
        -- Deprecated field - 20201013
        -- Please use order_type_stamped instead
        
        CASE WHEN o.order_type IS NULL THEN '3. Growth'
            ELSE o.order_type END                                                                       AS order_type, 
        
        --********************************************************
        
        CASE WHEN o.order_type_stamped IS NULL THEN '3. Growth'
            ELSE o.order_type_stamped END                                                               AS order_type_stamped, 
        
        o.account_owner_team_stamped,

        CASE
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment = 'SMB' 
                    THEN 'SMB'
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment = 'Mid-Market' 
                    THEN 'Mid-Market'
            WHEN (a.sales_segment = 'Unknown' OR a.sales_segment IS NULL) 
                AND o.user_segment IN ('Large', 'US West', 'US East', 'Public Sector''EMEA', 'APAC') 
                    THEN 'Large'
            ELSE a.sales_segment END                                                                    AS sales_segment,
        CASE WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')  
                THEN 'Pipeline'
             WHEN h.stage_name IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                         
                THEN '3+ Pipeline'
             WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                       
                THEN 'Lost'
             WHEN h.stage_name IN ('Closed Won')                                                                                                         
                THEN 'Closed Won'
             ELSE 'Other' END                                                                           AS stage_name_3plus,
        CASE WHEN h.stage_name IN ('00-Pre Opportunity','0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')     
                THEN 'Pipeline'
            WHEN h.stage_name IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')                                                                               
                THEN '4+ Pipeline'
            WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost')                                                                                                                   
                THEN 'Lost'
            WHEN h.stage_name IN ('Closed Won')                                                                                                                                     
                THEN 'Closed Won'
            ELSE 'Other' END                                                                            AS stage_name_4plus,
        -- excluded accounts 
        CASE WHEN a.ultimate_parent_id IN ('001610000111bA3','0016100001F4xla','0016100001CXGCs','00161000015O9Yn','0016100001b9Jsc') 
                AND h.close_date < '2020-08-01' THEN 1 ELSE 0 END                                       AS is_excluded_flag,

        -- metrics
        h.forecasted_iacv,
        h.renewal_acv,
        h.total_contract_value,

        CASE WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') 
                AND h.sales_type = 'Renewal'      
                    THEN h.renewal_acv*-1
            WHEN h.stage_name IN ('Closed Won')                                                     
                THEN h.forecasted_iacv  
            ELSE 0 END                                                                                  AS net_iacv,
        CASE WHEN h.stage_name IN ('8-Closed Lost', 'Closed Lost') 
                AND h.sales_type = 'Renewal'      
                    THEN h.renewal_acv*-1
            WHEN h.stage_name IN ('Closed Won') AND h.forecasted_iacv < 0                           
                    THEN h.forecasted_iacv
            ELSE 0 END                                                                                  AS churn_only,

        -- created & closed in quarter
        CASE WHEN dc.fiscal_quarter_name_fy = d.fiscal_quarter_name_fy
            AND h.stage_name IN ('Closed Won')  
            THEN h.forecasted_iacv ELSE 0 END                                                           AS created_and_won_iacv,

        -- account owner hierarchies levels
        account_owner.sales_team_level_2                                                                AS account_owner_team_level_2,
        account_owner.sales_team_level_3                                                                AS account_owner_team_level_3,
        account_owner.sales_team_level_4                                                                AS account_owner_team_level_4,
        
        account_owner.sales_team_vp_level                                                               AS account_owner_team_vp_level,
        account_owner.sales_team_rd_level                                                               AS account_owner_team_rd_level,
        account_owner.sales_team_asm_level                                                              AS account_owner_team_asm_level,
        
        -- identify VP level managers
        account_owner.is_lvl_2_vp_flag                                                                  AS account_owner_is_lvl_2_vp_flag,

        -- opportunity owner hierarchies levels
        CASE WHEN sa.level_2 is not null 
            THEN sa.level_2 
            ELSE opportunity_owner.sales_team_level_2 END                                                   AS opportunity_owner_team_level_2,
        CASE WHEN sa.level_3 is not null 
            THEN sa.level_3 
            ELSE opportunity_owner.sales_team_level_3 END                                                   AS opportunity_owner_team_level_3,
        
        -- identify VP level managers
        CASE WHEN opportunity_owner.sales_team_level_2 LIKE 'VP%' 
            OR sa.level_2 LIKE 'VP%'
                THEN 1 ELSE 0 END                                                                           AS opportunity_owner_is_lvl_2_vp_flag

    FROM sfdc_opportunity_snapshot_history h
    -- close date
    INNER JOIN date_details d
        ON d.date_actual = cast(h.close_date as date) 
    -- snapshot date
    INNER JOIN date_details ds
        ON h.date_actual = ds.date_actual
    -- created date - INNER JOIN does not work.
    LEFT JOIN date_details dc
        ON dc.date_actual = CAST(h.created_date AS DATE)
    -- current opportunity
    LEFT JOIN sfdc_opportunity_xf o     
        ON o.opportunity_id = h.opportunity_id
    -- accounts
    LEFT JOIN sfdc_accounts_xf a
        ON h.account_id = a.account_id 
     -- account owner
    LEFT JOIN sfdc_users_xf account_owner
        ON account_owner.user_id = a.owner_id
    -- opportunity owner
    LEFT JOIN sfdc_users_xf opportunity_owner
        ON opportunity_owner.user_id = h.owner_id
    -- sales admin hierarchy
    LEFT JOIN sales_admin_bookings_hierarchy sa
        ON h.opportunity_id = sa.opportunity_id
) 
SELECT *
FROM final