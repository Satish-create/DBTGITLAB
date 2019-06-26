import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "AVG_CYCLE_ANALYTICS_ID": "1",
    "CI_PROJECT_DIR": "/analytics",
    "DAYS": "1",
    "EXECUTION_DATE": "{{ execution_date }}",
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
}

# Extract DAG
extract_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}
extract_dag = DAG(
    "version_db_extract", default_args=extract_dag_args, schedule_interval="0 */8 * * *"
)
# Extract Task
extract_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/version_db_manifest.yaml
"""
version_db_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="version-db-extract",
    name="version-db-extract",
    secrets=[
        VERSION_DB_USER,
        VERSION_DB_PASS,
        VERSION_DB_HOST,
        VERSION_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[extract_cmd],
    dag=extract_dag,
)

# Sync DAG
sync_dag_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}
sync_dag = DAG(
    "version_db_sync", default_args=sync_dag_args, schedule_interval="0 4 */1 * *"
)
# Extract Task
sync_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/version_db_manifest.yaml --sync
"""
version_db_sync = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="version-db-sync",
    name="version-db-sync",
    secrets=[
        VERSION_DB_USER,
        VERSION_DB_PASS,
        VERSION_DB_HOST,
        VERSION_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[sync_cmd],
    dag=sync_dag,
)
# SCD Task
scd_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/extract/postgres/ &&
    python tap_postgres/tap_postgres.py tap manifests/version_db_manifest.yaml --scd_only
"""
version_db_scd = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="version-db-scd",
    name="version-db-scd",
    secrets=[
        VERSION_DB_USER,
        VERSION_DB_PASS,
        VERSION_DB_HOST,
        VERSION_DB_NAME,
        PG_PORT,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_ROLE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[scd_cmd],
    dag=sync_dag,
)
version_db_sync >> version_db_scd
