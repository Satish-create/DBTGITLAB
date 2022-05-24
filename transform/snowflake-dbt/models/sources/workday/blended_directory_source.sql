WITH bamboohr AS (
  
  SELECT *
  FROM {{ ref('bamboohr_directory_source') }}

),

workday AS (

  SELECT *
  FROM {{ ref('workday_directory_source') }} -- need a daily snapshot
),

map AS (

  SELECT *
  FROM {{ ref('map_employee_id') }}
)

SELECT
  map.wk_employee_id AS employee_id,
  bamboohr.work_email,
  bamboohr.full_name,
  bamboohr.job_title,
  bamboohr.supervisor,
  bamboohr.uploaded_at,
  'bamboohr' AS source_system
FROM bamboohr
INNER JOIN map
  ON bamboohr.employee_id = map.bhr_employee_id

UNION 

SELECT
  employee_id,
  work_email,
  full_name,
  job_title,
  supervisor,
  uploaded_at,
  'workday' AS source_system
FROM workday