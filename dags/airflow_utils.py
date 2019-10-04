""" This file contains common operators/functions to be used across multiple DAGs """
import os
import json
from typing import List
from datetime import datetime, timedelta, date

from airflow.contrib.kubernetes.pod import Resources
from airflow.operators.slack_operator import SlackAPIPostOperator


def split_date_parts(day: date, partition: str) -> List[dict]:

    if partition == "month":
        split_dict = {
            "year": day.strftime("%Y"),
            "month": day.strftime("%m"),
            "part": day.strftime("%Y_%m"),
        }

    return split_dict


def partitions(from_date: date, to_date: date, partition: str) -> List[dict]:
    """
    A list of partitions to build.
    """

    delta = to_date - from_date
    all_parts = [
        split_date_parts((from_date + timedelta(days=i)), partition)
        for i in range(delta.days + 1)
    ]

    seen = set()
    parts = []
    # loops through every day and pulls out unique set of date parts
    for p in all_parts:
        if p["part"] not in seen:
            seen.add(p["part"])
            parts.append({k: v for k, v in p.items()})
    return parts


def slack_failed_task(context):
    """
    Function to be used as a callable for on_failure_callback.
    Send a Slack alert.
    """

    # Set all of the contextual vars
    base_url = "http://35.190.127.73"
    execution_date = context["ts"]
    dag_context = context["dag"]
    dag_name = dag_context.dag_id
    dag_id = context["dag"].dag_id
    task_name = context["task"].task_id
    task_id = context["task_instance"].task_id
    execution_date_str = str(context["execution_date"])
    task_instance = str(context["task_instance_key_str"])

    # Generate the link to the logs
    link = f"{base_url}/log?dag_id={dag_id}&task_id={task_id}&execution_date={execution_date}"

    if task_name == "dbt-source-freshness":
        slack_channel = "#analytics-pipelines"
    else:
        slack_channel = dag_context.params.get(
            "slack_channel_override", "#analytics-pipelines"
        )

    attachment = [
        {
            "color": "#FF0000",
            "fallback": "An Airflow DAG has failed!",
            "title": "Link to debug",
            "title_link": link,
            "fields": [
                {"title": "Timestamp", "value": execution_date_str, "short": True},
                {"title": "Task ID", "value": task_instance, "short": False},
            ],
        }
    ]

    failed_alert = SlackAPIPostOperator(
        attachments=attachment,
        channel=slack_channel,
        task_id="slack_failed",
        text=f"DAG: *{dag_name}* failed on task: *{task_name}*!",
        token=os.environ["SLACK_API_TOKEN"],
        username="Airflow",
    )
    return failed_alert.execute()


# Set the resources for the task pods
pod_resources = Resources(
    request_memory="1Gi", limit_memory="10Gi", request_cpu="200m", limit_cpu="1"
)

# GitLab default settings for all DAGs
gitlab_defaults = dict(
    get_logs=True,
    image_pull_policy="Always",
    in_cluster=not os.environ["IN_CLUSTER"] == "False",
    is_delete_operator_pod=True,
    namespace=os.environ["NAMESPACE"],
    resources=pod_resources,
)

# GitLab default environment variables for worker pods
env = os.environ.copy()
GIT_BRANCH = env["GIT_BRANCH"]
gitlab_pod_env_vars = {
    "CI_PROJECT_DIR": "/analytics",
    "EXECUTION_DATE": "{{ next_execution_date }}",
    "SNOWFLAKE_LOAD_DATABASE": "RAW"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_RAW",
    "SNOWFLAKE_TRANSFORM_DATABASE": "ANALYTICS"
    if GIT_BRANCH == "master"
    else f"{GIT_BRANCH.upper()}_ANALYTICS",
}
