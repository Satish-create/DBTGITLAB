WITH employees AS (

    SELECT *
    FROM {{ ref ('employee_directory_analysis') }}

), bamboohr_engineering_division_mapping AS (

    SELECT *
    FROM {{ ref ('bamboohr_engineering_division_mapping') }}

), engineering_employees AS (

    SELECT
      date_actual,
      employee_id,
      full_name,
      LOWER(job_title)                                                      AS job_title,
      LOWER(TRIM(VALUE::string))                                            AS jobtitle_speciality,
      reports_to,
      layers,
      department,
      work_email
    FROM employees,
    LATERAL FLATTEN(INPUT=>SPLIT(COALESCE(REPLACE(jobtitle_speciality,'&',','),''), ','))
    WHERE division = 'Engineering'
      AND date_actual >= '2020-01-01'

), engineering_employee_attributes AS (
    
    SELECT 
      engineering_employees.date_actual,
      engineering_employees.employee_id,
      engineering_employees.full_name,
      engineering_employees.job_title,
      bamboohr_engineering_division_mapping.sub_department,
      engineering_employees.jobtitle_speciality,
      CASE 
        WHEN engineering_employees.employee_id IN (41965,41996,41453,41482,41974,41487,42029,40914,41954,46) 
            OR engineering_employees.job_title LIKE '%backend%' 
          THEN 'backend'
        WHEN engineering_employees.job_title LIKE '%fullstack%'
          THEN 'fullstack'
        WHEN engineering_employees.job_title LIKE '%frontend%'
          THEN 'frontend'
        ELSE NULL END                                                           AS technology_group,
      engineering_employees.department,
      engineering_employees.work_email,
      engineering_employees.reports_to
    FROM engineering_employees
    LEFT JOIN bamboohr_engineering_division_mapping
      ON bamboohr_engineering_division_mapping.jobtitle_speciality = engineering_employees.jobtitle_speciality 

), reporting_structure AS (

    SELECT 
      {{ dbt_utils.surrogate_key(['date_actual', 'employee_id', 'team_name']) }} AS unique_key,
      engineering_employee_attributes.*,
      bamboohr_engineering_division_mapping.team_name
    FROM engineering_employee_attributes
    LEFT JOIN bamboohr_engineering_division_mapping
      ON bamboohr_engineering_division_mapping.jobtitle_speciality = engineering_employee_attributes.jobtitle_speciality
      AND bamboohr_engineering_division_mapping.sub_department = engineering_employee_attributes.sub_department
      AND bamboohr_engineering_division_mapping.technology_group = engineering_employee_attributes.technology_group
 
)

SELECT *
FROM reporting_structure
