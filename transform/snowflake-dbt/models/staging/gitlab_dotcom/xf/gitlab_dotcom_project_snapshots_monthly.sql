WITH date_details AS (
  
    SELECT *
    FROM {{ref("date_details")}}
    WHERE date_day >= '2019-02-01'::DATE
     
), project_snapshots AS (
   SELECT
     *,
     IFNULL(valid_to, DATEADD('days', 1, CURRENT_DATE)) AS valid_to_
   FROM {{ ref('gitlab_dotcom_projects_snapshots_base') }}
  -- where namespace_id = 8239636
), project_snapshots_history AS (
  
    SELECT
      DATEADD('days', -1, date_details.date_actual)                      AS date_actual,
      DATE_TRUNC('month', DATEADD('days', -1, date_details.date_actual)) AS snapshot_month,
      project_snapshots.project_id,
      project_snapshots.namespace_id,
      project_snapshots.visibility_level,
      project_snapshots.shared_runners_enabled
    FROM project_snapshots
    INNER JOIN date_details
      ON date_details.date_actual BETWEEN project_snapshots.valid_from AND project_snapshots.valid_to_
  
), project_snapshots_monthly AS (
    
    SELECT
      snapshot_month,
      project_id,
      namespace_id,
      visibility_level,
      shared_runners_enabled
    FROM project_snapshots_history
    QUALIFY ROW_NUMBER() OVER(PARTITION BY snapshot_month, project_id ORDER BY date_actual DESC) = 1
  
)

SELECT *
FROM project_snapshots_monthly
