{{ config({
    "schema": "analytics",
    "post-hook": "grant select on {{this}} to role reporter"
    })
}}

WITH source AS (

	SELECT *
	FROM {{ source('sheetload', 'planned_values') }}

), renamed AS (


	SELECT
       unique_key::INT                   AS primary_key,
       plan_month::DATE                  AS plan_month,
	     planned_new_pipe::INT             AS planned_new_pipe,
	     planned_total_iacv::INT           AS planned_total_iacv,
	     planned_tcv_minus_gross_opex::INT AS planned_tcv_minus_gross_opex,
       planned_total_arr::INT            AS planned_total_arr

	FROM source

)

SELECT * FROM renamed
