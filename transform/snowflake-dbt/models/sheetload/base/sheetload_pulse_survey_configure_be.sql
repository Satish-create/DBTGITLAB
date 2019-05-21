with source as (

    SELECT * FROM raw.sheetload.pulse_survey_configure_be

), renamed as (

SELECT row_number() OVER (ORDER BY "Timestamp")                                 AS pk_id,
    date_trunc('week',"Timestamp"::date)::date                                  AS survey_date,
    "I_am_enthusiastic_about_the_work_that_I_do_for_my_team"::integer           AS enthusiasm_about_work,
    "My_manager_supports_me_and_allows_me_to_perform_at_my_best"::integer       AS manager_support,
    "I_would_highly_recommend_GitLab_as_a_place_to_work_to_my_friends"::integer AS recommend_GitLab,
    'configure'                                                                 AS gitlab_group,
    'backend'                                                                   AS team
FROM source

)

SELECT *
FROM renamed
