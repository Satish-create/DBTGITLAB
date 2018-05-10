view: usage_data {
  sql_table_name: version.usage_data ;;
  label: "Usage Data"

  dimension: created_at_month {
    type: date_month
    sql: ${TABLE}.created_at ;;
  }

  # UUID

  dimension: uuid {
    type: string
    sql: ${TABLE}.uuid ;;
  }

  measure: distinct_uuid_count {
    type: count_distinct
    sql: ${uuid} ;;
  }

  # Active users

  dimension: active_user_count {
    type: number
    sql: ${TABLE}.active_user_count ;;
  }

  measure: average_users {
    group_label: "Average Group"
    type: average
    sql: ${active_user_count} ;;
  }

  measure: percentile80_users {
    group_label: "80th Percentile Group"
    type: percentile
    percentile: 80
    sql: ${active_user_count} ;;
  }

  measure: percentile90_users {
    group_label: "90th Percentile Group"
    type: percentile
    percentile: 90
    sql: ${active_user_count} ;;
  }

  measure: percentile99_users {
    group_label: "99th Percentile Group"
    type: percentile
    percentile: 99
    sql: ${active_user_count} ;;
  }

  # Projects

  dimension: projects_count {
    type: number
    sql: (${TABLE}.stats->'projects')::text::numeric ;;
  }

  dimension: clusters_count {
    type: number
    sql: (${TABLE}.stats->'clusters')::text::numeric ;;
  }

  dimension: clusters_enabled_count {
    type: number
    sql: (${TABLE}.stats->'clusters_enabled')::text::numeric ;;
  }

  dimension: clusters_platforms_gke {
    type: number
    sql: (${TABLE}.stats->'clusters_platforms_gke')::text::numeric ;;
  }

  dimension: clusters_platforms_existing {
    type: number
    sql: (${TABLE}.stats->'clusters_platforms_user')::text::numeric ;;
  }

  dimension: clusters_helm_deployed {
    type: number
    sql: (${TABLE}.stats->'clusters_applications_helm')::text::numeric ;;
  }

  dimension: clusters_ingress_deployed {
    type: number
    sql: (${TABLE}.stats->'clusters_applications_ingress')::text::numeric ;;
  }

  dimension: clusters_prometheus_deployed {
    type: number
    sql: (${TABLE}.stats->'clusters_applications_prometheus')::text::numeric ;;
  }

  dimension: clusters_runner_deployed {
    type: number
    sql: (${TABLE}.stats->'clusters_applications_runner')::text::numeric ;;
  }

  dimension: ci_builds {
    type: number
    sql: (${TABLE}.stats->'ci_builds')::text::numeric ;;
  }

  dimension: ci_deployments {
    type: number
    sql: (${TABLE}.stats->'deployments')::text::numeric ;;
  }

  dimension: ci_internal_pipelines {
    type: number
    sql: (${TABLE}.stats->'ci_internal_pipelines')::text::numeric ;;
  }

  dimension: ci_external_pipelines {
    type: number
    sql: (${TABLE}.stats->'ci_external_pipelines')::text::numeric ;;
  }

  measure: average_projects_per_user {
    group_label: "Average Group"
    type: average
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile80_projects_per_user {
    group_label: "80th Percentile Group"
    type: percentile
    percentile: 80
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile90_projects_per_user {
    group_label: "90th Percentile Group"
    type: percentile
    percentile: 90
    sql: ${projects_count} / ${active_user_count} ;;
  }

  measure: percentile99_projects_per_user {
    group_label: "99th Percentile Group"
    type: percentile
    percentile: 99
    sql: ${projects_count} / ${active_user_count} ;;
  }

  # Issues

  dimension: issues_count {
    type: number
    sql: (${TABLE}.stats->'issues')::text::numeric ;;
  }

  measure: average_issues_per_user {
    group_label: "Average Group"
    type: average
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile80_issues_per_user {
    group_label: "80th Percentile Group"
    type: percentile
    percentile: 80
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile90_issues_per_user {
    group_label: "90th Percentile Group"
    type: percentile
    percentile: 90
    sql: ${issues_count} / ${active_user_count} ;;
  }

  measure: percentile99_issues_per_user {
    group_label: "99th Percentile Group"
    type: percentile
    percentile: 99
    sql: ${issues_count} / ${active_user_count} ;;
  }

  # Merge requests

  dimension: merge_requests_count {
    type: number
    sql: (${TABLE}.stats->'merge_requests')::text::numeric ;;
  }

  measure: average_merge_requests_per_user {
    group_label: "Average Group"
    type: average
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile80_merge_requests_per_user {
    group_label: "80th Percentile Group"
    type: percentile
    percentile: 80
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile90_merge_requests_per_user {
    group_label: "90th Percentile Group"
    type: percentile
    percentile: 90
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

  measure: percentile99_merge_requests_per_user {
    group_label: "99th Percentile Group"
    type: percentile
    percentile: 99
    sql: ${merge_requests_count} / ${active_user_count} ;;
  }

}
