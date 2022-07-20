{%- macro wave_metrics(metrics_path, metric_value) -%}

--usage ping data - devops metrics ( wave 2 & 3.0)
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.events' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS umau_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.action_monthly_active_users_project_repo' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS action_monthly_active_users_project_repo_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.merge_requests' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS merge_requests_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.projects_with_repositories_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_with_repositories_enabled_28_days_user,
CASE WHEN metrics_path = 'counts.commit_comment' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS commit_comment_all_time_event,
CASE WHEN metrics_path = 'counts.source_code_pushes' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS source_code_pushes_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.verify.ci_pipelines' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_pipelines_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.verify.ci_internal_pipelines' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_internal_pipelines_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.verify.ci_builds' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_builds_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage.verify.ci_builds' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_builds_all_time_user,
CASE WHEN metrics_path = 'counts.ci_builds' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_builds_all_time_event,
CASE WHEN metrics_path = 'counts.ci_runners' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_runners_all_time_event,
CASE WHEN metrics_path = 'counts.auto_devops_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS auto_devops_enabled_all_time_event,
CASE WHEN metrics_path = 'gitlab_shared_runners_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS gitlab_shared_runners_enabled,
CASE WHEN metrics_path = 'container_registry_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS container_registry_enabled,
CASE WHEN metrics_path = 'counts.template_repositories' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS template_repositories_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.verify.ci_pipeline_config_repository' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_pipeline_config_repository_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_unique_users_all_secure_scanners' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_unique_users_all_secure_scanners_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_sast_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_sast_jobs_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_dast_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_dast_jobs_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_dependency_scanning_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_dependency_scanning_jobs_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_license_management_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_license_management_jobs_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_secret_detection_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_secret_detection_jobs_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_container_scanning_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS user_container_scanning_jobs_28_days_user,
CASE WHEN metrics_path = 'object_store.packages.enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS object_store_packages_enabled,
CASE WHEN metrics_path = 'counts.projects_with_packages' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_with_packages_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.package.projects_with_packages' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_with_packages_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.release.deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS deployments_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.release.releases' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS releases_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.epics' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS epics_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.issues' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS issues_28_days_user,

-- 3.1 metrics
CASE WHEN metrics_path = 'counts.ci_internal_pipelines' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_internal_pipelines_all_time_event,
CASE WHEN metrics_path = 'counts.ci_external_pipelines' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_external_pipelines_all_time_event,
CASE WHEN metrics_path = 'counts.merge_requests' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS merge_requests_all_time_event,
CASE WHEN metrics_path = 'counts.todos' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS todos_all_time_event,
CASE WHEN metrics_path = 'counts.epics' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS epics_all_time_event,
CASE WHEN metrics_path = 'counts.issues' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS issues_all_time_event,
CASE WHEN metrics_path = 'counts.projects' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_all_time_event,
CASE WHEN metrics_path = 'counts_monthly.deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS deployments_28_days_event,
CASE WHEN metrics_path = 'counts_monthly.packages' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS packages_28_days_event,
CASE WHEN metrics_path = 'counts.sast_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS sast_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.dast_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dast_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.dependency_scanning_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dependency_scanning_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.license_management_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS license_management_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.secret_detection_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS secret_detection_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.container_scanning_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS container_scanning_jobs_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jenkins_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jenkins_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_bamboo_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_bamboo_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jira_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_drone_ci_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_drone_ci_active_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.issues_imported.jira' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS jira_imports_28_days_event,
CASE WHEN metrics_path = 'counts.projects_github_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_github_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jira_server_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_server_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jira_dvcs_cloud_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_dvcs_cloud_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_with_repositories_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_with_repositories_enabled_all_time_event,
CASE WHEN metrics_path = 'counts.protected_branches' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS protected_branches_all_time_event,
CASE WHEN metrics_path = 'counts.remote_mirrors' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS remote_mirrors_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.create.projects_enforcing_code_owner_approval' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_enforcing_code_owner_approval_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.configure.project_clusters_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS project_clusters_enabled_28_days_user,


-- 3.2 metrics
CASE WHEN metrics_path = 'instance_auto_devops_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS auto_devops_enabled,
CASE WHEN metrics_path = 'gitaly.clusters' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS gitaly_clusters_instance,
CASE WHEN metrics_path = 'counts.epics_deepest_relationship_level' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS epics_deepest_relationship_level_instance,
CASE WHEN metrics_path = 'counts.clusters_applications_cilium' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS clusters_applications_cilium_all_time_event,
CASE WHEN metrics_path = 'counts.network_policy_forwards' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS network_policy_forwards_all_time_event,
CASE WHEN metrics_path = 'counts.network_policy_drops' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS network_policy_drops_all_time_event,
CASE WHEN metrics_path = 'counts.requirements_with_test_report' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS requirements_with_test_report_all_time_event,
CASE WHEN metrics_path = 'counts.requirement_test_reports_ci' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS requirement_test_reports_ci_all_time_event,
CASE WHEN metrics_path = 'counts.projects_imported_from_github' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_imported_from_github_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jira_cloud_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_cloud_active_all_time_event,
CASE WHEN metrics_path = 'counts.projects_jira_dvcs_server_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_dvcs_server_active_all_time_event,
CASE WHEN metrics_path = 'counts.service_desk_issues' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS service_desk_issues_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.verify.ci_pipelines' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_pipelines_all_time_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.service_desk_issues' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS service_desk_issues_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.projects_jira_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_active_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.projects_jira_dvcs_cloud_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_dvcs_cloud_active_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.plan.projects_jira_dvcs_server_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_jira_dvcs_server_active_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.merge_requests_with_required_codeowners' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS merge_requests_with_required_code_owners_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.analytics.g_analytics_valuestream_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS analytics_value_stream_28_days_event,
CASE WHEN metrics_path = 'redis_hll_counters.code_review.i_code_review_user_approve_mr_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS code_review_user_approve_mr_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.epics_usage.epics_usage_total_unique_counts_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS epics_usage_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.ci_templates.ci_templates_total_unique_counts_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_templates_usage_28_days_event,
CASE WHEN metrics_path = 'redis_hll_counters.issues_edit.g_project_management_issue_milestone_changed_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS project_management_issue_milestone_changed_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.issues_edit.g_project_management_issue_iteration_changed_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS project_management_issue_iteration_changed_28_days_user,

-- 5.1 metrics
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.protected_branches' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS protected_branches_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.analytics.p_analytics_ci_cd_lead_time_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_cd_lead_time_usage_28_days_event,
CASE WHEN metrics_path = 'redis_hll_counters.analytics.p_analytics_ci_cd_deployment_frequency_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS ci_cd_deployment_frequency_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.create.projects_with_repositories_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_with_repositories_enabled_all_time_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_api_fuzzing_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS api_fuzzing_jobs_usage_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.coverage_fuzzing_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS coverage_fuzzing_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.api_fuzzing_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS api_fuzzing_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.container_scanning_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS container_scanning_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.dependency_scanning_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dependency_scanning_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.sast_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS sast_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.secret_detection_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS secret_detection_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.dast_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dast_pipeline_usage_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_coverage_fuzzing_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS coverage_fuzzing_jobs_28_days_user,
CASE WHEN metrics_path = 'counts.environments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS environments_all_time_event,
CASE WHEN metrics_path = 'counts.feature_flags' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS feature_flags_all_time_event,
CASE WHEN metrics_path = 'counts_monthly.successful_deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS successful_deployments_28_days_event,
CASE WHEN metrics_path = 'counts_monthly.failed_deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS failed_deployments_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.projects_with_compliance_framework' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_compliance_framework_all_time_event,
CASE WHEN metrics_path = 'redis_hll_counters.pipeline_authoring.o_pipeline_authoring_unique_users_committing_ciconfigfile_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS commit_ci_config_file_28_days_user,
CASE WHEN metrics_path = 'compliance_unique_visits.g_compliance_audit_events' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS view_audit_all_time_user,

-- 5.2 metrics
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.user_dependency_scanning_jobs' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dependency_scanning_jobs_all_time_user,
CASE WHEN metrics_path = 'analytics_unique_visits.i_analytics_dev_ops_adoption' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS analytics_devops_adoption_all_time_user,
CASE WHEN metrics_path = 'usage_activity_by_stage.manage.project_imports.total' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_imported_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.user_preferences_group_overview_security_dashboard' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS preferences_security_dashboard_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.create.action_monthly_active_users_ide_edit' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS web_ide_edit_28_days_user,
CASE WHEN metrics_path = 'counts.ci_pipeline_config_auto_devops' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS auto_devops_pipelines_all_time_event,
CASE WHEN metrics_path = 'counts.projects_prometheus_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_prometheus_active_all_time_event,
CASE WHEN metrics_path = 'prometheus_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS prometheus_enabled,
CASE WHEN metrics_path = 'prometheus_metrics_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS prometheus_metrics_enabled,
CASE WHEN metrics_path = 'usage_activity_by_stage.manage.group_saml_enabled' THEN  {{ convert_variant_to_boolean_field("metric_value") }} ELSE 0 END  AS group_saml_enabled,
CASE WHEN metrics_path = 'usage_activity_by_stage.manage.issue_imports.jira' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS jira_issue_imports_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.plan.epics' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS author_epic_all_time_user,
CASE WHEN metrics_path = 'usage_activity_by_stage.plan.issues' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS author_issue_all_time_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.release.failed_deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS failed_deployments_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.release.successful_deployments' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS successful_deployments_28_days_user,

-- 5.3 metrics
CASE WHEN metrics_path = 'geo_enabled' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS geo_enabled,
CASE WHEN metrics_path = 'counts.geo_nodes' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS geo_nodes_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.verify.ci_pipeline_config_auto_devops' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS auto_devops_pipelines_28_days_user,
CASE WHEN metrics_path = 'counts.ci_runners_instance_type_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS active_instance_runners_all_time_event,
CASE WHEN metrics_path = 'counts.ci_runners_group_type_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS active_group_runners_all_time_event,
CASE WHEN metrics_path = 'counts.ci_runners_project_type_active' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS active_project_runners_all_time_event,
CASE WHEN metrics_path = 'gitaly.version' THEN metric_value::VARCHAR   ELSE 0 END  AS gitaly_version,
CASE WHEN metrics_path = 'gitaly.servers' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS gitaly_servers_all_time_event,

-- 6.0 metrics
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.api_fuzzing_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS api_fuzzing_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.api_fuzzing_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS api_fuzzing_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.coverage_fuzzing_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS coverage_fuzzing_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.coverage_fuzzing_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS coverage_fuzzing_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.secret_detection_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS secret_detection_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.secret_detection_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS secret_detection_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.dependency_scanning_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dependency_scanning_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.dependency_scanning_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dependency_scanning_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.container_scanning_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS container_scanning_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.container_scanning_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS container_scanning_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.dast_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dast_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.dast_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS dast_scans_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.secure.sast_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS sast_scans_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.secure.sast_scans' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS sast_scans_28_days_event,

-- 6.1 metrics
CASE WHEN metrics_path = 'counts.package_events_i_package_push_package_by_deploy_token' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS packages_pushed_registry_all_time_event,
CASE WHEN metrics_path = 'counts.package_events_i_package_pull_package_by_guest' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS packages_pulled_registry_all_time_event,
CASE WHEN metrics_path = 'redis_hll_counters.compliance.g_compliance_dashboard_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS compliance_dashboard_view_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.compliance.g_compliance_audit_events_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS audit_screen_view_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.compliance.i_compliance_audit_events_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS instance_audit_screen_view_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.compliance.i_compliance_credential_inventory_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS credential_inventory_view_28_days_user,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.compliance_frameworks_with_pipeline' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS compliance_frameworks_pipeline_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.manage.groups_with_event_streaming_destinations' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS groups_streaming_destinations_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.groups_with_event_streaming_destinations' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS groups_streaming_destinations_28_days_event,
CASE WHEN metrics_path = 'usage_activity_by_stage.manage.audit_event_destinations' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS audit_event_destinations_all_time_event,
CASE WHEN metrics_path = 'usage_activity_by_stage_monthly.manage.audit_event_destinations' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS audit_event_destinations_28_days_event,
CASE WHEN metrics_path = 'counts.projects_with_external_status_checks' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS projects_status_checks_all_time_event,
CASE WHEN metrics_path = 'counts.external_status_checks' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS external_status_checks_all_time_event,
CASE WHEN metrics_path = 'redis_hll_counters.search.i_search_paid_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS paid_license_search_28_days_user,
CASE WHEN metrics_path = 'redis_hll_counters.manage.unque_active_users_monthly' THEN {{ null_negative_numbers("metric_value") }} ELSE 0 END  AS last_activity_28_days_user

{%- endmacro -%}