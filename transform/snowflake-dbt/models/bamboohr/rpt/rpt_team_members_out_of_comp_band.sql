{{ config({
    "materialized":"table",
    "schema": "analytics"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

WITH employee_directory_intermediate AS (

   SELECT *
   FROM {{ref('employee_directory_intermediate')}}

 ), comp_band AS (

   SELECT *
   FROM {{ ref('comp_band_deviation_snapshots') }}

), joined as (

  SELECT employee_directory_intermediate.*,
          comp_band.deviation_from_comp_calc,
          CASE WHEN comp_band.deviation_from_comp_calc <= 0 THEN 0
               WHEN comp_band.deviation_from_comp_calc < 0.01 THEN 0.25
               WHEN comp_band.deviation_from_comp_calc < 0.05 THEN 0.5
               WHEN comp_band.deviation_from_comp_calc < 0.1 THEN 0.75
               ELSE 1 END as weighted_deviated_from_comp_calc
  FROM employee_directory_intermediate
  LEFT JOIN comp_band
    ON employee_directory_intermediate.employee_number::varchar = comp_band.bamboo_employee_number::varchar
    AND valid_from <= date_actual
    AND COALESCE(valid_to::date, {{max_date_in_analysis}}) > date_actual

), bucketed as (

  SELECT
    date_actual,
    SUM(weighted_deviated_from_comp_calc)/
      COUNT(distinct employee_number) as percent_of_employees_outside_of_band
  FROM joined
  WHERE date_actual < CURRENT_DATE
  GROUP BY 1
)

SELECT *
FROM aggregated
ORDER BY 1
