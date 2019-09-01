import os
from datetime import datetime, timedelta

from airflow import DAG

from kube_secrets import *
from airflow_utils import slack_failed_task, gitlab_defaults, gitlab_pod_env_vars
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
pod_env_vars = {**gitlab_pod_env_vars, **{"CI_PROJECT_DIR": "/analytics"}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "sla": timedelta(hours=24),
    "sla_miss_callback": slack_failed_task,
    "start_date": datetime(2019, 1, 1),
}

# Create the DAG
dag = DAG(
    "engineering_extract", default_args=default_args, schedule_interval="0 */8 * * *"
)

engineering_extract_cmd = f"""
    git clone -b {env['GIT_BRANCH']} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1 &&
    export PYTHONPATH="$CI_PROJECT_DIR/orchestration/:$PYTHONPATH" &&
    cd analytics/ &&
    python extract/engineering/execute.py
"""
engineering_extract = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/data-image:latest",
    task_id="engineering-extract",
    name="engineering-extract",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
        SNOWFLAKE_LOAD_PASSWORD,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[engineering_extract_cmd],
    dag=dag,
)
