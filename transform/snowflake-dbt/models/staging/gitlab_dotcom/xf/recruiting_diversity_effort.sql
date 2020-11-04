WITH issues AS (
  
    SELECT *
    FROM {{ ref ('gitlab_dotcom_issues_xf') }}
    WHERE project_id =16492321 --Recruiting for Open Positions

), users AS (

    SELECT *
    FROM {{ ref ('gitlab_dotcom_users') }}

), assignee AS (

    SELECT *
    FROM {{ ref ('gitlab_dotcom_issue_assignees') }} 

), agg_assignee AS (

    SELECT
     issue_id,
     ARRAY_AGG(LOWER(user_name)) WITHIN GROUP (ORDER BY user_name ASC) AS assignee
    FROM assignee
    LEFT JOIN users
      ON assignee.user_id = users.user_id 
    GROUP BY issue_id

), intermediate AS (

    SELECT 
      issues.issue_title,
      issues.issue_iid,
      issues.issue_created_at,
      DATE_TRUNC(week,issue_created_at)                         AS issue_created_week,
      issues.issue_closed_at,
      DATE_TRUNC(week,issues.issue_closed_at)                   AS issue_closed_week,
      IFF(issue_closed_at IS NOT NULL,1,0)                      AS is_issue_closed,
      issues.state                                              AS issue_state,
      agg_assignee.assignee,
      issues.issue_description,
      SPLIT_PART(issue_description, '#### <summary>',2)                 AS issue_description_split,
      CASE WHEN CONTAINS(issue_description, '[x] Yes, Diversity Sourcing methods were used'::VARCHAR) = True
            THEN 'Used Diversity Strings'
           WHEN CONTAINS(issue_description, '[x] No, I did not use Diversity Sourcing methods'::VARCHAR) = True
            THEN 'Did not use'
           WHEN CONTAINS(issue_description, '[x] Not Actively Sourcing'::VARCHAR) = True
            THEN 'Not Actively Sourcing'
            ELSE 'No Answer' END                                AS issue_answer
    FROM issues
    LEFT JOIN agg_assignee 
      ON agg_assignee.issue_id = issues.issue_id
    WHERE project_id = 16492321
      AND LOWER(issue_title) LIKE '%weekly check-in:%'
      AND LOWER(issue_title) NOT LIKE '%test%'
  
), split_issue AS (

    SELECT *
    FROM intermediate AS splittable, lateral split_to_table(splittable.issue_description_split, '| 20') 
    ---- splitting by year starting with 20

), cleaned AS (

    SELECT *,
      LEFT(TRIM(value),10) As week_of,
      CASE WHEN CONTAINS(value,'[x] Yes, Diversity sourcing was used')
             THEN 'Yes'
           WHEN CONTAINS(value, '[x] Not actively sourcing')
             THEN 'Not Actively Sourcing'
          WHEN CONTAINS(value,'[x] No, Did not use')
            THEN 'No'
         ELSE issue_answer END                              AS used_diversity_string
  FROM split_issue

), final AS (

    SELECT
      issue_title,
      issue_iid,
      issue_created_at,
      issue_created_week,
      issue_closed_at,
      issue_closed_week,
      week_of,
      ---moved to using 1 issue per req and tracking weeks in issue on 2020.11.01
      is_issue_closed,
      issue_state,
      issue_description,
      assignee,
      used_diversity_string
    FROM cleaned
  
)

SELECT *
FROM final
