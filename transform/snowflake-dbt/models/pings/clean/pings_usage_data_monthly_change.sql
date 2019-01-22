{{
  config(
    materialized='incremental',
    unique_key='unique_key'
  )
}}

WITH pings_mom_change AS (
  SELECT
    md5(uuid || created_at)             AS unique_key,
    uuid                                AS uuid,
    created_at                          AS created_at,
    edition                             AS edition,
    main_edition                        AS main_edition,
    edition_type                        AS edition_type,
    {{ monthly_change('active_user_count') }} ,
    {{ monthly_change('assignee_lists') }} ,
    {{ monthly_is_used('auto_devops_disabled') }} ,
    {{ monthly_is_used('auto_devops_enabled') }} ,
    {{ monthly_change('boards') }} ,
    {{ monthly_change('ci_builds') }} ,
    {{ monthly_change('ci_external_pipelines') }} ,
    {{ monthly_change('ci_internal_pipelines') }} ,
    {{ monthly_change('ci_pipeline_config_auto_devops') }} ,
    {{ monthly_change('ci_pipeline_config_repository') }} ,
    {{ monthly_change('ci_pipeline_schedules') }} ,
    {{ monthly_is_used('ci_runners') }} ,
    {{ monthly_change('ci_triggers') }} ,
    {{ monthly_change('clusters') }} ,
    {{ monthly_is_used('clusters_applications_helm') }} ,
    {{ monthly_is_used('clusters_applications_ingress') }} ,
    {{ monthly_is_used('clusters_applications_prometheus') }} ,
    {{ monthly_is_used('clusters_applications_runner') }} ,
    {{ monthly_is_used('clusters_disabled') }} ,
    {{ monthly_is_used('clusters_enabled') }} ,
    {{ monthly_change('clusters_platforms_gke') }} ,
    {{ monthly_change('clusters_platforms_user') }} ,
    {{ monthly_change('container_scanning_jobs') }} ,
    {{ monthly_change('dast_jobs') }} ,
    {{ monthly_change('dependency_scanning_jobs') }} ,
    {{ monthly_change('deploy_keys') }} ,
    {{ monthly_change('deployments') }} ,
    {{ monthly_is_used('environments') }} ,
    {{ monthly_change('epics') }} ,
    {{ monthly_change('gcp_clusters') }} ,
    {{ monthly_change('geo_nodes') }} ,
    {{ monthly_change('groups') }} ,
    {{ monthly_is_used('groups') }} ,
    {{ monthly_change('in_review_folder') }} ,
    {{ monthly_change('issues') }} ,
    {{ monthly_change('keys') }} ,
    {{ monthly_change('label_lists') }} ,
    {{ monthly_change('labels') }} ,
    {{ monthly_change('ldap_group_links') }} ,
    {{ monthly_change('ldap_keys') }} ,
    {{ monthly_change('ldap_users') }} ,
    {{ monthly_change('lfs_objects') }} ,
    {{ monthly_change('license_management_jobs') }} ,
    {{ monthly_change('merge_requests') }} ,
    {{ monthly_change('milestone_lists') }} ,
    {{ monthly_change('milestones') }} ,
    {{ monthly_change('notes') }} ,
    {{ monthly_change('pages_domains') }} ,
    {{ monthly_change('projects') }} ,
    {{ monthly_change('projects_imported_from_github') }} ,
    {{ monthly_is_used('projects_jira_active') }} ,
    {{ monthly_is_used('projects_mirrored_with_pipelines_enabled') }} ,
    {{ monthly_is_used('projects_prometheus_active') }} ,
    {{ monthly_is_used('projects_reporting_ci_cd_back_to_github') }} ,
    {{ monthly_is_used('projects_slack_notifications_active') }} ,
    {{ monthly_is_used('projects_slack_slash_active') }} ,
    {{ monthly_change('protected_branches') }} ,
    {{ monthly_change('releases') }} ,
    {{ monthly_change('remote_mirrors') }} ,
    {{ monthly_change('sast_jobs') }} ,
    {{ monthly_is_used('service_desk_enabled_projects') }} ,
    {{ monthly_change('service_desk_issues') }} ,
    {{ monthly_change('snippets') }} ,
    {{ monthly_change('todos') }} ,
    {{ monthly_change('uploads') }} ,
    {{ monthly_change('web_hooks') }}
  FROM {{ ref("pings_usage_data_month") }}
  {% if is_incremental() %}
      WHERE created_at >= DATE_TRUNC('month', CURRENT_DATE) - interval '1 month'
  {% endif %}
)

SELECT *
FROM pings_mom_change
