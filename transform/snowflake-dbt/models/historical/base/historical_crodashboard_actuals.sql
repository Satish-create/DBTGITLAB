WITH source AS (

	SELECT md5(month_of :: varchar)                  as pk,
				 month_of :: date,
				 nullif(ramped_reps_on_quota, '') :: float as ramped_reps_on_quota,
				 nullif(rep_productivity, '') :: float     as rep_productivity,
				 nullif(sales_efficiency, '') :: float     as sales_efficiency
	FROM raw.historical.crodashboard_actuals
)

SELECT *
FROM source
