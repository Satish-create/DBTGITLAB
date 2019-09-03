{{ config({
    "schema": "analytics"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ ref('bamboohr_custom_bonus') }}

), filtered AS (

  SELECT employee_id, bonus_id, bonus_date
  FROM source
  WHERE bonus_type = 'Discretionary Bonus'

)

SELECT *
FROM filtered
