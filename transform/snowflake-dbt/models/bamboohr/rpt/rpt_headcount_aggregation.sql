{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

With overall_headcount_pivoted AS (
   
    SELECT 
        DATE_TRUNC('month',month_date)                  AS month_date,
        {{ dbt_utils.pivot(
            'metric',
            dbt_utils.get_column_values(ref('bamboohr_headcount_aggregation_intermediate'), 'total_count')
        ) }}

    FROM {{ ref('bamboohr_headcount_aggregation_intermediate') }}
    GROUP BY DATE_TRUNC('month',month_date)


)

SELECT * 
FROM overall_headcount_pivoted


      
      

