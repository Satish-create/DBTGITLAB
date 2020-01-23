{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}


With source AS (
    
  SELECT *
  FROM "ANALYTICS"."PLUTHRA_SCRATCH_SENSITIVE"."Bamboohr_headcount_aggregation_xf"

), overall_headcount_pivoted AS (
   
  SELECT 
        pivot_table.month_date,
        'total'                                             AS diversity_field,                            
        "'headcount_start'"                                 AS headcount_start,
        "'headcount_end'"                                   AS headcount_end,
        ("'headcount_start'" + "'headcount_end'")/ 2        AS headcount_average,
        "'hires'"                                           AS hires,
        "'total_separated'"                                 AS total_separated,
        "'voluntary_separations'"                           AS voluntary_separations,
        "'involuntary_separations'"                         AS involuntary_separations,
        'total'                                             AS aggregation_type
    FROM  
      
      (
        SELECT 
          DATE_TRUNC('month',source.month_date)             AS month_date, 
          metric, 
          total_count
        FROM source  
      ) AS source_table
      
      PIVOT  
        ( 
          SUM(total_count)  
          FOR metric IN ('headcount_start'
                         ,'headcount_end'
                         ,'hires','total_separated'
                         ,'voluntary_separations'
                         ,'involuntary_separations')  
        ) AS pivot_table
     
), gender_headcount_pivoted AS (

  SELECT 
        pivot_table.month_date,
        pivot_table.gender,
        "'headcount_start'"                                 AS headcount_start,
        "'headcount_end'"                                   AS headcount_end,
        ("'headcount_start'" + "'headcount_end'")/ 2        AS headcount_average,
        "'hires'"                                           AS hires,
        "'total_separated'"                                 AS total_separated,
        "'voluntary_separations'"                           AS voluntary_separations,
        "'involuntary_separations'"                         AS involuntary_separations,
        'gender'                                            AS aggregation_tye
    FROM 
  
      (
        SELECT 
          DATE_TRUNC('month',source.month_date)             AS month_date, 
          gender,
          metric, 
          total_count
        FROM source
      ) AS source_table
  
      PIVOT  
        (  
          SUM(total_count)  
          FOR metric IN ('headcount_start'
                         ,'headcount_end'
                         ,'hires'
                         ,'total_separated'
                         ,'voluntary_separations'
                         ,'involuntary_separations')  
        ) AS pivot_table

), aggregated AS (

  SELECT 
      overall_headcount_pivoted.*,
      SUM(h2.total_separated)                                                   AS rolling_12_month_separations,
      SUM(h2.voluntary_separations)                                             AS rolling_12_month_voluntary_separations,
      SUM(h2.involuntary_separations)                                           AS rolling_12_month_involuntary_separations,  
      ROUND(AVG(h2.headcount_average),0)                                        AS rolling_12_month_headcount      
  FROM overall_headcount_pivoted
  LEFT JOIN overall_headcount_pivoted h2 
    ON h2.month_date BETWEEN DATE_TRUNC('month',DATEADD('month', -11, overall_headcount_pivoted.month_date)) AND DATE_TRUNC('month',overall_headcount_pivoted.month_date)
  GROUP BY 1,2,3,4,5,6,7,8,9,10

  UNION ALL

  SELECT 
      gender_headcount_pivoted.*,
      SUM(a2.total_separated)                                                   as rolling_12_month_separations,
      SUM(a2.voluntary_separations)                                             as rolling_12_month_voluntary_separations,
      SUM(a2.involuntary_separations)                                           as rolling_12_month_involuntary_separations,  
      ROUND(AVG(a2.headcount_average),0)                                        as rolling_12_month_headcount      
  FROM gender_headcount_pivoted
  LEFT JOIN gender_headcount_pivoted a2 
    ON a2.month_date BETWEEN DATE_TRUNC('month',DATEADD('month', -11, gender_headcount_pivoted.month_date)) AND DATE_TRUNC('month',gender_headcount_pivoted.month_date)
        AND a2.gender = gender_headcount_pivoted.gender
  GROUP BY 1,2,3,4,5,6,7,8,9,10

), final AS (

    SELECT
      month_date,
      diversity_field, 
      aggregation_type,
      headcount_start,
      headcount_end,    
      hires,
      rolling_12_month_headcount,
      rolling_12_month_separations,
      rolling_12_month_voluntary_separations,
      rolling_12_month_involuntary_separations,
      1 - (rolling_12_month_separations/rolling_12_month_headcount) AS retention    

    FROM aggregated

)

SELECT * 
FROM final
