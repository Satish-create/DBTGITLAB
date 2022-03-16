{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(2)).strftime('%Y')) %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(2)).strftime('%m')) %}
   

{%- set event_ctes = [
  {
    "event_name": "action",
    "source_cte_name": "prep_action",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id"
  },
  {
    "event_name": "dast_build_run",
    "source_cte_name": "dast_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "dependency_scanning_build_run",
    "source_cte_name": "dependency_scanning_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "deployment_creation",
    "source_cte_name": "prep_deployment",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_deployment_id"
  },
  {
    "event_name": "epic_creation",
    "source_cte_name": "prep_epic",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "group_id",
    "project_column_name": "NULL",
    "primary_key": "dim_epic_id"
  },
  {
    "event_name": "issue_creation",
    "source_cte_name": "prep_issue",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_issue_id"
  },
  {
    "event_name": "issue_note_creation",
    "source_cte_name": "issue_note",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id"
  },
  {
    "event_name": "license_scanning_build_run",
    "source_cte_name": "license_scanning_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "merge_request_creation",
    "source_cte_name": "prep_merge_request",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_merge_request_id"
  },
  {
    "event_name": "merge_request_note_creation",
    "source_cte_name": "merge_request_note",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id"
  },
  {
    "event_name": "ci_pipeline_creation",
    "source_cte_name": "prep_ci_pipeline",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_pipeline_id"
  },
  {
    "event_name": "package_creation",
    "source_cte_name": "prep_package",
    "user_column_name": "creator_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_package_id"
  },
  {
    "event_name": "protect_ci_build_creation",
    "source_cte_name": "protect_ci_build",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "push_action",
    "source_cte_name": "push_actions",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id"
  },
  {
    "event_name": "release_creation",
    "source_cte_name": "prep_release",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_release_id"
  },
  {
    "event_name": "requirement_creation",
    "source_cte_name": "prep_requirement",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_requirement_id"
  },
  {
    "event_name": "sast_build_run",
    "source_cte_name": "sast_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "secret_detection_build_run",
    "source_cte_name": "secret_detection_jobs",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "secure_ci_build_creation",
    "source_cte_name": "secure_ci_build",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_build_id"
  },
  {
    "event_name": "successful_ci_pipeline_creation",
    "source_cte_name": "successful_ci_pipelines",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_pipeline_id"
  },
  {
    "event_name": "action_monthly_active_users_project_repo",
    "source_cte_name": "monthly_active_users_project_repo",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_action_id"
  },
  {
    "event_name": "ci_stage",
    "source_cte_name": "prep_ci_stage",
    "user_column_name": "NULL",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_ci_stage_id" 
  },
  {
    "event_name": "notes",
    "source_cte_name": "prep_note",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_note_id"
  },
  {
    "event_name": "todos",
    "source_cte_name": "prep_todo",
    "user_column_name": "author_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_todo_id"
  },
  {
    "event_name": "issue_resource_label_events",
    "source_cte_name": "prep_resource_label_events",
    "user_column_name": "dim_user_id",
    "ultimate_parent_namespace_column_name": "ultimate_parent_namespace_id",
    "project_column_name": "dim_project_id",
    "primary_key": "dim_issue_label_id"
  },
  
]

-%}

{{ simple_cte([
    ('prep_ci_pipeline', 'prep_ci_pipeline'),
    ('prep_action', 'prep_action'),
    ('prep_ci_build', 'prep_ci_build'),
    ('prep_deployment', 'prep_deployment'),
    ('prep_epic', 'prep_epic'),
    ('prep_issue', 'prep_issue'),
    ('prep_merge_request', 'prep_merge_request'),
    ('prep_note', 'prep_note'),
    ('prep_package', 'prep_package'),
    ('prep_release', 'prep_release'),
    ('prep_requirement', 'prep_requirement'),
    ('dim_project', 'dim_project'),
    ('prep_namespace', 'prep_namespace'),
    ('prep_user', 'prep_user'),
    ('prep_plan', 'prep_gitlab_dotcom_plan'),
    ('prep_namespace_plan_hist', 'prep_namespace_plan_hist'),
    ('prep_ci_stage', 'prep_ci_stage'),
    ('prep_note', 'prep_note'),
    ('prep_todo', 'prep_todo'),
    ('prep_resource_label_events', 'prep_resource_label_events'),
    ('map_saas_event_to_gmau','map_saas_event_to_gmau'),
    ('map_saas_event_to_smau','map_saas_event_to_smau')
]) }}

, dast_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'dast'

), dependency_scanning_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'dependency_scanning'

), push_actions AS (

    SELECT *
    FROM  prep_action
    WHERE event_action_type = 'pushed'

), issue_note AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type = 'Issue'

), license_scanning_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type IN (
                                  'license_scanning',
                                  'license_management'
                                )

), merge_request_note AS (

    SELECT *
    FROM prep_note
    WHERE noteable_type = 'MergeRequest'

), protect_ci_build AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type IN ('container_scanning')
    
), sast_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'sast'

), secret_detection_jobs AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type = 'secret_detection'

), secure_ci_build AS (

    SELECT *
    FROM prep_ci_build
    WHERE secure_ci_build_type IN ('api_fuzzing',
                                    'dast',
                                    'dependency_scanning',
                                    'license_management',
                                    'license_scanning',
                                    'sast',
                                    'secret_detection'
                                    )
    
), successful_ci_pipelines AS (

    SELECT *
    FROM prep_ci_pipeline
    WHERE failure_reason IS NULL

), monthly_active_users_project_repo AS (

    SELECT *
    FROM  prep_action
    WHERE target_type IS NULL
      AND event_action_type = 'pushed'

), issue_resource_label_events AS (

    SELECT *
    FROM prep_resource_label_events
    WHERE issue_id IS NOT NULL

), stage_mapping AS (

    SELECT DISTINCT *
    FROM (
        SELECT event_name, stage_name
        FROM map_saas_event_to_gmau
        UNION ALL
        SELECT event_name, stage_name
        FROM map_saas_event_to_smau
    )

), data AS (

{% for event_cte in event_ctes %}

    SELECT
      MD5({{ event_cte.source_cte_name}}.{{ event_cte.primary_key }} || '-' || '{{ event_cte.event_name }}')   AS event_id,
      '{{ event_cte.event_name }}'                                                                             AS event_name,
      stage_mapping.stage_name,
      {%- if event_cte.project_column_name != 'NULL' %}
      {{ event_cte.source_cte_name}}.{{ event_cte.project_column_name }}                                       AS dim_project_id,
      'project'                                                                                                AS parent_type,
      {{ event_cte.source_cte_name}}.{{ event_cte.project_column_name }}                                       AS parent_id, 
      {%- else %}
      NULL                                                                                                     AS dim_project_id,
      'group'                                                                                                  AS parent_type,
      {{ event_cte.source_cte_name}}.{{ event_cte.ultimate_parent_namespace_column_name }}                     AS parent_id, 
      {%- endif %}
      {{ event_cte.source_cte_name}}.ultimate_parent_namespace_id,
      {{ event_cte.source_cte_name}}.dim_plan_id                                                               AS plan_id_at_event_date,
      prep_plan.plan_name                                                                                      AS plan_name_at_event_date,
      IFNULL(prep_plan.plan_is_paid, FALSE)                                                                    AS plan_was_paid_at_event_date,
      {{ event_cte.source_cte_name}}.created_at                                                                AS event_created_at,
      {{ event_cte.source_cte_name}}.created_date_id,
      {%- if event_cte.user_column_name != 'NULL' %}
      {{ event_cte.source_cte_name}}.{{ event_cte.user_column_name }}                                          AS dim_user_id,
      prep_user.created_at                                                                                     AS user_created_at,
      TO_DATE(prep_user.created_at)                                                                            AS user_created_date,
      {%- else %}
      NULL                                                                                                     AS dim_user_id,
      NULL                                                                                                     AS user_created_at,
      NULL                                                                                                     AS user_created_date,
      {%- endif %}
      prep_namespace.created_at                                                                                AS namespace_created_at,
      TO_DATE(prep_namespace.created_at)                                                                       AS namespace_created_date,
      blocked_user.is_blocked_user                                                                             AS is_blocked_namespace_creator,
      prep_namespace.namespace_is_internal,
      FLOOR(
      DATEDIFF('hour',
              prep_namespace.created_at,
              {{ event_cte.source_cte_name}}.created_at)/24)                                                   AS days_since_namespace_creation_at_event_date,
      {%- if event_cte.user_column_name != 'NULL' %}
      FLOOR(
      DATEDIFF('hour',
              prep_user.created_at,
              {{ event_cte.source_cte_name}}.created_at)/24)                                                   AS days_since_user_creation_at_event_date,
      {%- else %}
      NULL                                                                                                     AS days_since_user_creation_at_event_date,
      {%- endif %}
      {%- if event_cte.project_column_name != 'NULL' %}
      FLOOR(
      DATEDIFF('hour',
              dim_project.created_at,
              {{ event_cte.source_cte_name}}.created_at)/24)                                                   AS days_since_project_creation_at_event_date, 
      IFNULL(dim_project.is_imported, FALSE)                                                                   AS project_is_imported,
      dim_project.is_learn_gitlab                                                                              AS project_is_learn_gitlab
      {%- else %}
      NULL,
      NULL,
      NULL
      {%- endif %}                                                                       
    FROM {{ event_cte.source_cte_name }}
    {%- if event_cte.project_column_name != 'NULL' %}
    INNER JOIN dim_project 
      ON {{event_cte.source_cte_name}}.{{event_cte.project_column_name}} = dim_project.dim_project_id
    {%- endif %}
    {%- if event_cte.ultimate_parent_namespace_column_name != 'NULL' %}
    INNER JOIN prep_namespace
      ON {{event_cte.source_cte_name}}.{{event_cte.ultimate_parent_namespace_column_name}} = prep_namespace.dim_namespace_id
      AND prep_namespace.is_currently_valid = TRUE
    LEFT JOIN prep_user AS blocked_user
      ON prep_namespace.creator_id = blocked_user.dim_user_id
    {%- endif %}
    {%- if event_cte.user_column_name != 'NULL' %}
    LEFT JOIN prep_user
      ON {{event_cte.source_cte_name}}.{{event_cte.user_column_name}} = prep_user.dim_user_id
    {%- endif %}
    LEFT JOIN prep_plan
      ON {{event_cte.source_cte_name}}.dim_plan_id = prep_plan.dim_plan_id
    LEFT JOIN stage_mapping
      ON '{{ event_cte.event_name }}' = stage_mapping.event_name
    WHERE DATE_PART('year', {{ event_cte.source_cte_name}}.created_at) = {{year_value}}
      AND DATE_PART('month', {{ event_cte.source_cte_name}}.created_at) = {{month_value}}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{%- endfor %}

)

SELECT *
FROM data
