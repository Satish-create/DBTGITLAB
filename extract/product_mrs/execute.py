import io
from os import environ as env
import pandas as pd
import requests

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


def get_project_ids():
    url = "https://gitlab.com/gitlab-data/analytics/raw/master/transform/snowflake-dbt/data/projects_part_of_product.csv"
    csv_bytes = requests.get(url).content
    csv = pd.read_csv(io.StringIO(csv_bytes.decode("utf-8")))
    return csv["project_id"].unique()


def get_urls_for_mrs_for_project(project_id, api_token):
    url = f"https://gitlab.com/api/v4/projects/{project_id}/merge_requests"
    response = requests.get(url, headers={"Private-Token": api_token})
    mr_json_list = response.json()
    return [mr["web_url"] for mr in mr_json_list]


def get_mr_json(mr_url, api_token):
    url = f"{mr_url}/diffs.json"
    response = requests.get(url, headers={"Private-Token": api_token})
    return response.json()


if __name__ == "__main__":

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    file_name = "part_of_product_mrs.json"

    with open(file_name, "w") as out_file:
        project_ids = get_project_ids()
        for project_id in project_ids:
            mr_urls = get_urls_for_mrs_for_project(
                project_id, env["GITLAB_COM_API_TOKEN"]
            )
            for mr_url in mr_urls:
                mr_information = get_mr_json(mr_url, env["GITLAB_COM_API_TOKEN"])

    snowflake_stage_load_copy_remove(
        file_name,
        f"raw.engineering_extracts.engineering_extracts",
        f"raw.engineering_extracts.part_of_product_merge_requests",
        snowflake_engine,
    )
