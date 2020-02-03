{{ config({
    "materialized":"table",
    "schema": "sensitive"
    })
}}

WITH recursive employee_directory AS (

    SELECT
      employee_id,
      employee_number,
      first_name,
      last_name,
      work_email,
      hire_date,
      termination_date,
      hire_location_factor,
      cost_center
    FROM {{ ref('employee_directory') }}

), date_details AS (

    SELECT *
    FROM {{ ref('date_details') }}

), department_info AS (

    SELECT employee_id,
          job_title,
          effective_date,
          department,
          division,
          reports_to,
          effective_end_date
    FROM {{ ref('bamboohr_job_info') }}


), location_factor AS (

    SELECT *
    FROM {{ ref('employee_location_factor_snapshots') }}

), enriched AS (

    SELECT
      date_actual,
      employee_directory.*,
      (first_name ||' '|| last_name) AS full_name,
      department_info.job_title,
      department_info.department,
      department_info.division,
      department_info.reports_to,
      location_factor.location_factor,
      IFF(hire_date = date_actual, True, False) AS is_hire_date,
      IFF(termination_date = DATEADD('day', 1, date_actual), True, False) AS is_termination_date
    FROM date_details
    LEFT JOIN employee_directory
      ON hire_date::date <= date_actual
      AND COALESCE(termination_date::date, {{max_date_in_bamboo_analyses()}}) > date_actual
    LEFT JOIN department_info
      ON employee_directory.employee_id = department_info.employee_id
      AND effective_date <= date_actual
      AND COALESCE(effective_end_date::date, {{max_date_in_bamboo_analyses()}}) > date_actual
    LEFT JOIN location_factor
      ON employee_directory.employee_number::varchar = location_factor.bamboo_employee_number::varchar
      AND valid_from <= date_actual
      AND COALESCE(valid_to::date, {{max_date_in_bamboo_analyses()}}) > date_actual
    WHERE employee_directory.employee_id IS NOT NULL

), base_layers as (

    SELECT
      date_actual,
      reports_to,
      full_name,
      array_construct(reports_to, full_name) AS lineage
    FROM enriched
    WHERE NULLIF(reports_to, '') IS NOT NULL

), layers (date_actual, employee, manager, lineage, layers_count) as (

    SELECT
      date_actual,
      full_name as employee,
      reports_to as manager,
      lineage as lineage,
      1 as layers_count
    FROM base_layers
    WHERE manager IS NOT NULL

    UNION ALL

    SELECT anchor.date_actual,
          iter.full_name as employee,
          iter.reports_to as manager,
          array_prepend(anchor.lineage, iter.reports_to) as lineage,
          (layers_count+1) as layers_count
    FROM layers anchor
    JOIN base_layers iter
    ON anchor.date_actual = iter.date_actual
    AND iter.reports_to = anchor.employee


), calculated_layers AS (

    SELECT
      date_actual,
      employee,
      max(layers_count) as layers
    FROM layers

)

SELECT
  enriched.*,
  calculated_layers.layers
FROM enriched
LEFT JOIN calculated_layers
ON enriched.date_actual = calculated_layers.date_actual
AND full_name = employee
