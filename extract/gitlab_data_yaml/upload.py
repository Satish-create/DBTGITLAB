import logging
import subprocess
import sys
from os import environ as env

from gitlabdata.orchestration_utils import (
    snowflake_engine_factory,
    snowflake_stage_load_copy_remove,
)


if __name__ == "__main__":

    file_dict = dict(
        categories="categories",
        location_factors="location_factors",
        roles="job_families"
        team="team",
    )

    logging.basicConfig(stream=sys.stdout, level=20)

    config_dict = env.copy()
    snowflake_engine = snowflake_engine_factory(config_dict, "LOADER")

    base_url = "https://gitlab.com/gitlab-com/www-gitlab-com/raw/master/data/"

    job_failed = False

    for key, value in file_dict.items():
        logging.info(f"Downloading {value}.yml to {value}.json file.")
        try:
            command = f"curl {base_url}{value}.yml | yaml2json -o {value}.json"
            p = subprocess.run(command, shell=True)
            p.check_returncode()
        except:
            job_failed = True

        logging.info(f"Uploading to {value}.json to Snowflake stage.")

        snowflake_stage_load_copy_remove(
            f"{value}.json",
            "raw.gitlab_data_yaml.gitlab_data_yaml_load",
            f"raw.gitlab_data_yaml.{key}",
            snowflake_engine,
        )

    if job_failed:
        sys.exit(1)
