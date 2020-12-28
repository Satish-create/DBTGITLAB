WITH source AS (

    SELECT *
	FROM {{ source('bamboohr', 'directory') }}
	
), intermediate AS (

    SELECT 
      d.value                                   AS data_by_row, 
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['id']::NUMBER 				AS employee_id,
	  data_by_row['displayName']::varchar 	    AS full_name,
      data_by_row['jobTitle']::varchar 			AS job_title,
	  data_by_row['supervisor']::varchar 		AS supervisor,
	  data_by_row['workEmail']::varchar			AS work_email,
      DATE_TRUNC(day, uploaded_at)              AS uploaded_at
    FROM intermediate

), final AS (

    SELECT *
    FROM renamed
    WHERE work_email != 't2test@gitlab.com'

)

SELECT *
FROM final