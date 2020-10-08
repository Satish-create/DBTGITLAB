/*
Original issue: 

*/ 
WITH sfdc_opportunity_snapshot_history_xf AS (
    SELECT *
    FROM {{ ref('sfdc_opportunity_snapshot_history_xf') }}
    -- remove lost & deleted deals
    WHERE stage_name NOT IN ('9-Unqualified','10-Duplicate','Unqualified')
        AND is_deleted = 0
) 
SELECT  snapshot_date,
        close_fiscal_quarter,
        close_fiscal_quarter_date,
        close_fiscal_year,
        order_type_stamped,
        sales_segment,
        stage_name_3plus,
        stage_name_4plus,
        is_excluded_flag,
        stage_name,
        forecast_category_name,
        COUNT(DISTINCT opportunity_id)                          AS opps,
        SUM(net_iacv)                                           AS net_iacv,
        SUM(churn_only)                                         AS churn_only,
        SUM(forecasted_iacv)                                    AS forecasted_iacv,
        SUM(total_contract_value)                               AS tcv
FROM sfdc_opportunity_snapshot_history_xf 
WHERE 
    -- 2 quarters before start and full quarter, total rolling 9 months at end of quarter
    -- till end of quarter
    snapshot_date <= dateadd(month,3,close_fiscal_quarter_date)
    -- 2 quarters before start
    AND snapshot_date >= dateadd(month,-6,close_fiscal_quarter_date)
    -- remove forecast category Omitted
    AND forecast_category_name != 'Omitted'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11