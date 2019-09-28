WITH base_departments AS (

    SELECT *
    FROM {{ ref('netsuite_departments') }}

), ultimate_department AS (

    SELECT a.*,
           CASE WHEN a.parent_department_id IS NOT NULL THEN a.parent_department_id
                ELSE a.department_id
           END                               AS ultimate_department_id,
           CASE WHEN a.parent_department_id IS NOT NULL THEN b.department_name
                ELSE a.department_name
           END                               AS ultimate_department_name
    FROM base_departments a
    LEFT JOIN base_departments b
      ON a.parent_department_id = b.department_id

)

SELECT *
FROM ultimate_department
