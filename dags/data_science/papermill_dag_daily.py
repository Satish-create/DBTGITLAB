import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    clone_datascience_repo_cmd,
)
from kube_secrets import (
    GCP_SERVICE_CREDS,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_DATABASE,
)
from kubernetes_helpers import get_affinity, get_toleration


# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2019, 1, 1),
    "dagrun_timeout": timedelta(hours=2),
}

# Create the DAG
dag = DAG("snowflake_cleanup", default_args=default_args, schedule_interval="0 5 * * 0")

# Task 1
drop_clones_cmd = f"""
    {clone_datascience_repo_cmd} &&
    analytics/orchestration/drop_snowflake_objects.py drop_databases
"""
purge_clones = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="papermill-daily",
    name="papermill-daily",
    secrets=[
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_LOAD_DATABASE,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[drop_clones_cmd],
    affinity=get_affinity(True),
    toleration=get_toleration(True),
    dag=dag,
)
