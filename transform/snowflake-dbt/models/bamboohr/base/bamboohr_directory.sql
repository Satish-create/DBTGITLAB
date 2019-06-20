WITH source AS (

			SELECT *
	    FROM {{ source('bamboohr', 'directory') }}
	    ORDER BY uploaded_at DESC
	    LIMIT 1

), intermediate as (

      SELECT d.value as data_by_row
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
        data_by_row['id']::bigint 						AS employee_id,
				data_by_row['displayName']::varchar 	AS full_name,
        data_by_row['jobTitle']::varchar 		AS job_title
    FROM intermediate

)

SELECT *
FROM renamed
