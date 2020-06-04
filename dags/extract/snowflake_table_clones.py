import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DATA_IMAGE,
    clone_repo_cmd,
    gitlab_defaults,
    slack_failed_task,
    gitlab_pod_env_vars,
    clone_and_setup_extraction_cmd,
)
from kube_secrets import (
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_ROLE,
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_PASSWORD,
)

# Load the env vars into a dict and set env vars
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
    "TASK_INSTANCE": "{{ task_instance_key_str }}",
}

logging.info(pod_env_vars)

secrets = [
    SNOWFLAKE_LOAD_USER,
    SNOWFLAKE_LOAD_PASSWORD,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_LOAD_WAREHOUSE,
    SNOWFLAKE_LOAD_ROLE,
]


# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": datetime(2020, 6, 1),
}

timestamp_format = "yyyy-mm-dd hh24:mi:ss"

# Create the DAG
#  DAG will be triggered at 0am UTC which is 5 PM PST
dag = DAG(
    "snowflake_table_clones", default_args=default_args, schedule_interval="0 0 * * *"
)
CLONE_DATE = "{{ execution_date.strftime('%Y-%m-%d %H:%M:%SS') }}"
arguments=[clone_and_setup_extraction_cmd + " && " + \
    "python snowflake/snowflake_create_clones.py create_table_clone --source_schema analytics --source_table arr_data_mart --target_schema analytics_clones  --timestamp " + CLONE_DATE + " --target_table arr_data_mart_{{ yesterday_ds_nodash }} --timestamp_format ""yyyy-mm-dd hh:mi:ss""",
               ]

logging.info(arguments)

# Task 1
# Macros reference:
# {{ yesterday_ds_nodash }} - yesterday's execution date as YYYYMMDD
# {{ ts }} - same as execution_date.isoformat(). Example: 2018-01-01T00:00:00+00:00

make_clone = KubernetesPodOperator(
    **gitlab_defaults,
    image=DATA_IMAGE,
    task_id="snowflake-clone-arr-data-mart",
    name="snowflake-clone-arr-data-mart",
    secrets=secrets,
    env_vars=pod_env_vars,
    arguments=arguments,
    dag=dag,
)
