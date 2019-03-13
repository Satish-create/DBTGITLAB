import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator

from kube_secrets import *
from common_utils import slack_failed_task


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "SNOWFLAKE_TRANSFORM_DATABASE": RAW
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
}

# Set the command for the container
container_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    analytics/orchestration/generate_dbt_docs.sh
"""

# Create the DAG
dag = DAG("dbt_pages", default_args=default_args, schedule_interval=timedelta(days=1))

# Task 1
snowflake_load = KubernetesPodOperator(
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="dbt-pages",
    name="dbt-pages",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_USER,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[container_cmd],
    namespace=env["NAMESPACE"],
    get_logs=True,
    is_delete_operator_pod=True,
    in_cluster=False if env["IN_CLUSTER"] == "False" else True,
    dag=dag,
)
