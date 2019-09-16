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
    "SNOWFLAKE_LOAD_DATABASE": "RAW" if GIT_BRANCH == "master" else f"{GIT_BRANCH}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH}_ANALYTICS",
}

# Default arguments for the DAG
default_args = {
    "catchup": False,
    "depends_on_past": False,
    "on_failure_callback": slack_failed_task,
    "params": {"slack_channel_override": "#dbt-runs"},
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1, 0, 0, 0),
}

# Create the DAG
dag = DAG(
    "dbt_snowplow_backfill", default_args=default_args, schedule_interval=None
)


# Set the git command for the containers
git_cmd = f"git clone -b {GIT_BRANCH} --single-branch https://gitlab.com/gitlab-data/analytics.git --depth 1"


# Warehouse variable declaration
xs_warehouse = f"""'{{warehouse_name: transforming_xs}}'"""

# dbt-backfill
dbt_backfill_cmd = f"""
    {git_cmd} &&
    cd analytics/transform/snowflake-dbt/ &&
    export snowflake_load_database="RAW" &&
    dbt deps --profiles-dir profile &&
    dbt seed --profiles-dir profile --target prod --vars {xs_warehouse} # seed data from csv &&
    python scripts/dbt_backfill.py --models snowplow --target prod --vars --from-date 2018-09-01 --to-date {{ ds }} --partition month --profiles-dir profile
"""
dbt_backfill_cmd = KubernetesPodOperator(
    **gitlab_defaults,
    image="registry.gitlab.com/gitlab-data/data-image/dbt-image:latest",
    task_id="dbt-snowplow-backfill",
    name="dbt-snowplow-backfill",
    secrets=[
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
    ],
    env_vars=pod_env_vars,
    cmds=["/bin/bash", "-c"],
    arguments=[dbt_backfill_cmd],
    dag=dag,
)
