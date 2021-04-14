{{config({
    "schema": "common_mart_product"
  })
}}

{{ simple_cte([
    ('latest_metrics','fct_product_usage_wave_1_3_metrics_latest'),
    ('billing_accounts','dim_billing_account'),
    ('crm_accounts','dim_crm_account')
]) }}

, joined AS (

    SELECT
      latest_metrics.dim_subscription_id,
      latest_metrics.dim_subscription_id_original,
      {{ get_keyed_nulls('billing_accounts.dim_billing_account_id') }}      AS dim_billing_account_id,
      {{ get_keyed_nulls('crm_accounts.dim_crm_account_id') }}              AS dim_crm_account_id,
      latest_metrics.seat_link_report_date,
      latest_metrics.seat_link_report_date_id,
      latest_metrics.dim_usage_ping_id,
      latest_metrics.ping_created_at,
      latest_metrics.ping_created_date_id,
      latest_metrics.uuid,
      latest_metrics.hostname,
      latest_metrics.instance_type,
      latest_metrics.dim_license_id,
      latest_metrics.license_md5,
      latest_metrics.cleaned_version,
      -- Wave 1
      latest_metrics.license_utilization,
      latest_metrics.active_user_count,
      latest_metrics.max_historical_user_count,
      latest_metrics.license_user_count,
      -- Wave 2 & 3
      latest_metrics.umau_28_days_user,
      latest_metrics.action_monthly_active_users_project_repo_28_days_user,
      latest_metrics.merge_requests_28_days_user,
      latest_metrics.projects_with_repositories_enabled_28_days_user,
      latest_metrics.commit_comment_all_time_event,
      latest_metrics.source_code_pushes_all_time_event,
      latest_metrics.ci_pipelines_28_days_user,
      latest_metrics.ci_internal_pipelines_28_days_user,
      latest_metrics.ci_builds_28_days_user,
      latest_metrics.ci_builds_all_time_user,
      latest_metrics.ci_builds_all_time_event,
      latest_metrics.ci_runners_all_time_event,
      latest_metrics.auto_devops_enabled_all_time_event,
      latest_metrics.gitlab_shared_runners_enabled_instance_setting,
      latest_metrics.container_registry_enabled_instance_setting,
      latest_metrics.template_repositories_all_time_event,
      latest_metrics.ci_pipeline_config_repository_28_days_user,
      latest_metrics.user_unique_users_all_secure_scanners_28_days_user,
      latest_metrics.user_sast_jobs_28_days_user,
      latest_metrics.user_dast_jobs_28_days_user,
      latest_metrics.user_dependency_scanning_jobs_28_days_user,
      latest_metrics.user_license_management_jobs_28_days_user,
      latest_metrics.user_secret_detection_jobs_28_days_user,
      latest_metrics.user_container_scanning_jobs_28_days_user,
      latest_metrics.object_store_packages_enabled_instance_setting,
      latest_metrics.projects_with_packages_all_time_event,
      latest_metrics.projects_with_packages_28_days_user,
      latest_metrics.deployments_28_days_user,
      latest_metrics.releases_28_days_user,
      latest_metrics.epics_28_days_user,
      latest_metrics.issues_28_days_user,
      -- Wave 3.1
      latest_metrics.ci_internal_pipelines_all_time_event,
      latest_metrics.ci_external_pipelines_all_time_event,
      latest_metrics.merge_requests_all_time_event,
      latest_metrics.todos_all_time_event,
      latest_metrics.epics_all_time_event,
      latest_metrics.issues_all_time_event,
      latest_metrics.projects_all_time_event,
      latest_metrics.deployments_28_days_event,
      latest_metrics.packages_28_days_event,
      latest_metrics.sast_jobs_all_time_event,
      latest_metrics.dast_jobs_all_time_event,
      latest_metrics.dependency_scanning_jobs_all_time_event,
      latest_metrics.license_management_jobs_all_time_event,
      latest_metrics.secret_detection_jobs_all_time_event,
      latest_metrics.container_scanning_jobs_all_time_event,
      latest_metrics.projects_jenkins_active_all_time_event,
      latest_metrics.projects_bamboo_active_all_time_event,
      latest_metrics.projects_jira_active_all_time_event,
      latest_metrics.projects_drone_ci_active_all_time_event,
      latest_metrics.jira_imports_28_days_event,
      latest_metrics.projects_github_active_all_time_event,
      latest_metrics.projects_jira_server_active_all_time_event,
      latest_metrics.projects_jira_dvcs_cloud_active_all_time_event,
      latest_metrics.projects_with_repositories_enabled_all_time_event,
      latest_metrics.protected_branches_all_time_event,
      latest_metrics.remote_mirrors_all_time_event,
      latest_metrics.projects_enforcing_code_owner_approval_28_days_user,
      latest_metrics.project_clusters_enabled_28_days_user,
      latest_metrics.analytics_28_days_user,
      latest_metrics.issues_edit_28_days_user,
      latest_metrics.user_packages_28_days_user,
      latest_metrics.terraform_state_api_28_days_user,
      latest_metrics.incident_management_28_days_user,
      -- Wave 3.2
      latest_metrics.clusters_applications_cilium_all_time_event,
      latest_metrics.network_policy_forwards_all_time_event,
      latest_metrics.network_policy_drops_all_time_event,
      latest_metrics.gitaly_clusters_all_time_event,
      latest_metrics.merge_requests_with_required_code_owners_28_days_user,
      latest_metrics.code_review_user_approve_mr_28_days_user,
      latest_metrics.auto_devops_enabled_instance_setting,
      latest_metrics.ci_templates_usage_28_days_event,
      latest_metrics.ci_pipelines_all_time_user,
      latest_metrics.epics_usage_28_days_user,
      latest_metrics.requirement_test_reports_ci_all_time_event,
      latest_metrics.project_management_issue_milestone_changed_28_days_user,
      latest_metrics.project_management_issue_iteration_changed_28_days_user,
      latest_metrics.analytics_value_stream_28_days_event,
      latest_metrics.projects_imported_from_github_all_time_event,
      latest_metrics.projects_jira_cloud_active_all_time_event,
      latest_metrics.projects_jira_dvcs_server_active_all_time_event,
      latest_metrics.projects_jira_active_28_days_user,
      latest_metrics.projects_jira_dvcs_cloud_active_28_days_user,
      latest_metrics.projects_jira_dvcs_server_active_28_days_user,
      latest_metrics.service_desk_issues_28_days_user,
      latest_metrics.service_desk_issues_all_time_event,
      -- Data Quality Flags
      latest_metrics.instance_user_count_not_aligned,
      latest_metrics.historical_max_users_not_aligned,
      latest_metrics.is_seat_link_subscription_in_zuora,
      latest_metrics.is_seat_link_rate_plan_in_zuora,
      latest_metrics.is_seat_link_active_user_count_available,
      latest_metrics.is_usage_ping_license_mapped_to_subscription,
      latest_metrics.is_usage_ping_license_subscription_id_valid,
      latest_metrics.is_data_in_subscription_month
    FROM latest_metrics
    LEFT JOIN billing_accounts
      ON latest_metrics.dim_billing_account_id = billing_accounts.dim_billing_account_id
    LEFT JOIN crm_accounts
      ON billing_accounts.dim_crm_account_id = crm_accounts.dim_crm_account_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@mcooperDD",
    created_date="2021-02-11",
    updated_date="2021-04-13"
) }}