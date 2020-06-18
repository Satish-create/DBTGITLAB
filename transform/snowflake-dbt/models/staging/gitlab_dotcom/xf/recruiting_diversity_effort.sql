WITH issues AS (
  
    SELECT *
    FROM {{ ref ('gitlab_dotcom_issues_xf') }}
    WHERE project_id =16492321 --Recruiting for Open Positions

), users AS (

    SELECT *
    FROM {{ ref ('gitlab_dotcom_users') }}

), assignee AS (

    SELECT 
      assignee.*, 
      user_name AS assignee
    FROM {{ ref ('gitlab_dotcom_issue_assignees') }} assignee
    LEFT JOIN users
      ON assignee.user_id = users.user_id 

), agg_assignee AS (

    SELECT
     issue_id,
     ARRAY_AGG(LOWER(assignee)) WITHIN GROUP (ORDER BY assignee ASC) AS assignee
    FROM assignee
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
      issues.state,
      agg_assignee.assignee,
      IFF(CONTAINS(issue_description, '2. **Sourcer/Recruiter:** I used [Diversity Boolean strings](https://docs.google.com/spreadsheets/d/1Hs3UVEpgYOJgvV8Nlyb0Cl5P6_8IlAlxeLQeXz64d8Y/edit#gid=0) or used other methods, including provided best practices, to ensure 90% of my outbound sourcing efforts were directed towards individuals from underrepresented groups.
    * [x] Yes
    * [ ] No'::varchar) = True ,'used diversity strings',null)  AS used_diversity_booleanstrings,
      IFF(CONTAINS(issue_description, '2. **Sourcer/Recruiter:** I used [Diversity Boolean strings](https://docs.google.com/spreadsheets/d/1Hs3UVEpgYOJgvV8Nlyb0Cl5P6_8IlAlxeLQeXz64d8Y/edit#gid=0) or used other methods, including provided best practices, to ensure 90% of my outbound sourcing efforts were directed towards individuals from underrepresented groups.
    * [ ] Yes
    * [ ] No'::varchar) = True ,'no answer',null)               AS no_answer,
        IFF(CONTAINS(trim(issue_description), '2. **Sourcer/Recruiter:** I used [Diversity Boolean strings](https://docs.google.com/spreadsheets/d/1Hs3UVEpgYOJgvV8Nlyb0Cl5P6_8IlAlxeLQeXz64d8Y/edit#gid=0) or used other methods, including provided best practices, to ensure 90% of my outbound sourcing efforts were directed towards individuals from underrepresented groups.
    * [ ] Yes
    * [x] No'::varchar) = True ,'did not use',null)             AS did_not_use
    FROM issues
    LEFT JOIN agg_assignee 
      ON agg_assignee.issue_id = issues.issue_id
    WHERE project_id = 16492321
      AND issue_title LIKE '%Weekly Check-in:%'
      AND issue_title NOT LIKE '%Test%'
  
)

SELECT *
FROM intermediate
