import os
from datetime import datetime

from airflow import DAG
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow_utils import (
    DBT_IMAGE,
    dbt_install_deps_and_seed_nosha_cmd,
    gitlab_defaults,
    gitlab_pod_env_vars,
    slack_failed_task,
    xs_warehouse,
)
from kube_secrets import (
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
)

# Load the env vars into a dict and set Secrets
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
pod_env_vars = {**gitlab_pod_env_vars, **{}}

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
    "zuora_source_validation", default_args=default_args, schedule_interval="0 6 * * 0"
)

# Test Zuora source
dbt_source_test_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models source:zuora; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_zuora_source_test = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="zuora-source-test",
    name="zuora-source-test",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_source_test_cmd],
    dag=dag,
)

# Run zuora models
dbt_run_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt run --profiles-dir profile --target prod --models +sources.zuora; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_zuora_source_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="zuora-source-run",
    name="zuora-source-run",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_run_cmd],
    dag=dag,
)


# Test all zuora models
dbt_model_test_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt test --profiles-dir profile --target prod --models +sources.zuora; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_model_test_run = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="zuora-model-test",
    name="zuora-model-test",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_model_test_cmd],
    dag=dag,
)

# Snapshot zuora data
dbt_snapshot_cmd = f"""
    {dbt_install_deps_and_seed_nosha_cmd} &&
    dbt snapshot --profiles-dir profile --target prod --select +sources.zuora; ret=$?;
    python ../../orchestration/upload_dbt_file_to_snowflake.py results; exit $ret
"""
dbt_snapshot = KubernetesPodOperator(
    **gitlab_defaults,
    image=DBT_IMAGE,
    task_id="zuora-snapshot",
    name="zuora-snapshot",
    secrets=[
        SALT,
        SALT_EMAIL,
        SALT_IP,
        SALT_NAME,
        SALT_PASSWORD,
        SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_PASSWORD,
        SNOWFLAKE_TRANSFORM_ROLE,
        SNOWFLAKE_TRANSFORM_SCHEMA,
        SNOWFLAKE_TRANSFORM_WAREHOUSE,
        SNOWFLAKE_USER,
        SNOWFLAKE_LOAD_PASSWORD,
        SNOWFLAKE_LOAD_ROLE,
        SNOWFLAKE_LOAD_USER,
        SNOWFLAKE_LOAD_WAREHOUSE,
    ],
    env_vars=pod_env_vars,
    arguments=[dbt_snapshot_cmd],
    dag=dag,
)
