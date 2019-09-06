{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

	SELECT *,
				ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) as rank_in_key
  FROM {{ source('gitlab_dotcom', 'ci_builds') }}

), renamed AS (

  SELECT 
    id::INTEGER                      AS ci_build_id, 
    status                           AS ci_build_status, 
    finished_at::TIMESTAMP           AS ci_build_finished_at, 
    trace                            AS ci_build_trace, 
    created_at::TIMESTAMP            AS ci_build_created_at, 
    updated_at::TIMESTAMP            AS ci_build_updated_at, 
    started_at::TIMESTAMP            AS ci_build_started_at, 
    runner_id::INTEGER               AS ci_build_runner_id, 
    coverage                         AS ci_build_coverage, 
    commit_id::INTEGER               AS ci_build_commit_id, 
    commands                         AS ci_build_commands, 
    name                             AS ci_build_name, 
    options                          AS ci_build_options, 
    allow_failure                    AS ci_build_allow_failure, 
    stage                            AS ci_build_stage, 
    trigger_request_id::INTEGER      AS ci_build_trigger_request_id, 
    stage_idx                        AS ci_build_stage_idx, 
    tag                              AS ci_build_tag, 
    ref                              AS ci_build_ref, 
    user_id::INTEGER                 AS ci_build_user_id, 
    TYPE                             AS ci_build_type, 
    target_url                       AS ci_build_target_url, 
    description                      AS ci_build_description, 
    artifacts_file                   AS ci_build_artifacts_file, 
    project_id::INTEGER              AS ci_build_project_id, 
    artifacts_metadata               AS ci_build_artifacts_metadata, 
    erased_by_id::INTEGER            AS ci_build_erased_by_id, 
    erased_at::TIMESTAMP             AS ci_build_erased_at, 
    artifacts_expire_at::TIMESTAMP   AS ci_build_artifacts_expire_at, 
    environment                      AS ci_build_environment, 
    artifacts_size                   AS ci_build_artifacts_size, 
    yaml_variables                   AS ci_build_yaml_variables, 
    queued_at::TIMESTAMP             AS ci_build_queued_at, 
    token                            AS ci_build_token, 
    lock_version                     AS ci_build_lock_version, 
    coverage_regex                   AS ci_build_coverage_regex, 
    auto_canceled_by_id::INTEGER     AS ci_build_auto_canceled_by_id, 
    retried                          AS ci_build_retried, 
    stage_id::INTEGER                AS ci_build_stage_id, 
    artifacts_file_store             AS ci_build_artifacts_file_store, 
    artifacts_metadata_store         AS ci_build_artifacts_metadata_store, 
    protected                        AS ci_build_protected, 
    failure_reason                   AS ci_build_failure_reason, 
    scheduled_at::TIMESTAMP          AS ci_build_scheduled_at, 
    token_encrypted                  AS ci_build_token_encrypted, 
    upstream_pipeline_id::INTEGER    AS ci_build_upstream_pipeline_id 
  FROM source
  WHERE rank_in_key = 1

)


SELECT *
FROM renamed
