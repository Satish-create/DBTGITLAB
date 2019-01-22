WITH source AS (

	SELECT *
	FROM raw.sheetload.iacv_monthly_goals

), renamed AS (


	SELECT iacv_goal::integer as iacv_goal,
			date as date_day
	FROM source

)

SELECT *
FROM renamed
