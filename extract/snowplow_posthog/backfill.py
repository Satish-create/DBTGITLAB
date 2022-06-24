"""
Main routine for SNOWPLOW -> POSTHOG historical back-filling
For this task will use PostHog Python API library.
Library URL: https://posthog.com/docs/integrate/server/python
"""

import datetime
import yaml
import sys
from os import environ as env
from itertools import zip_longest

from fire import Fire
from logging import info, basicConfig

import boto3
import gzip
from dateutil.relativedelta import *
from decouple import config

ENCODING = "utf-8"


"""
Extract routines
"""


def s3_get_credentials() -> tuple:
    """
    This function returns the set of aws_access_key_id,aws_secret_access_key
    based on the the schema name provided.
    """

    posthog_access_key_id = env["POSTHOG_ACCESS_KEY_ID"]
    posthog_secret_access_key = env["POSTHOG_SECRET_ACCESS_KEY"]
    snowplow_s3_bucket = env["SNOWPLOW_S3_BUCKET"]

    return posthog_access_key_id, posthog_secret_access_key, snowplow_s3_bucket


def s3_get_client(
    aws_access_key_id: str, aws_secret_access_key: str
) -> boto3.resources.base.ServiceResource:
    """
    Get and return s3 client object
    """

    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key
    )
    return session.client("s3")


def s3_list_files(client, bucket, prefix="") -> str:
    """
    List files in specific S3 bucket using yield for in a cost-optimized fashion
    and return the file name
    """

    results = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents")

    for result in results:
        yield result["Key"]


def source_file_get_row(row: str) -> list:
    """
    Convert line from the file to a list of strings
    """
    separator = "\t"

    if row is None:
        return []

    result = row.split(separator)

    # exclude newline character ('\n') at the end of the line
    if result[-1] == "\n":
        return result[:-1]
    return result


def s3_load_source_file(client, bucket: str, file_name: str) -> list:
    """
    Load file content from object storage (for now, it is a S3 bucket)
    and return list
    """
    csv_obj = client.get_object(Bucket=bucket, Key=file_name)

    body = csv_obj["Body"]

    # Handy trick to convert binary to text file
    with gzip.open(body, "rt") as gz_file:
        for line in gz_file.readlines():
            yield source_file_get_row(line)


def get_date_range(yyyymm: str) -> list:
    """
    Return the date range as a list (hourly level)
    """
    full_date = f"{yyyymm}01"
    time_start = datetime.datetime.strptime(full_date, "%Y%m%d")
    time_end = datetime.datetime.strptime(full_date, "%Y%m%d") + relativedelta(months=1)

    ret_list = []

    while time_start < time_end:
        ret_list.append(time_start)

        time_start += relativedelta(hours=1)

    return ret_list


def get_file_prefix(yyyymm: str) -> list:
    """
    Get the list of file prefix

    Usage: "output/YYYY/MM/DD/HH24"
    """

    folder_name = "output/"

    return [
        f"""{folder_name}{day.strftime("%Y/%m/%d/%H")}"""
        for day in get_date_range(yyyymm=yyyymm)
    ]


def s3_extraction(file_prefix: str) -> None:

    """
    Load data from .tsv files (even the extension is .gz) stored in an S3 Bucket and push it to PostHog.
    Loader will iterate through all files in the provided bucket that have the `.tsv`/`.gz` extension.

    ----------------------------------------------------------------------------------------------------
    The bucket structure for events in S3 is:

    BUCKET_NAME
        /output
            /YYYY
                /MM
                    /DD
                        /HH24
                            /SnowPlowEnrichedGood-2-YYYY-MM-DD-HH24-MI-SS-UUID.gz
    ----------------------------------------------------------------------------------------------------

    File example: output/2022/06/06/04/SnowPlowEnrichedGood-2-2022-06-06-04-29-38-a3034baf-2167-42a5-9633-76318f7b5b8c.gz
    """
    (
        posthog_access_key_id,
        posthog_secret_access_key,
        snowplow_s3_bucket,
    ) = s3_get_credentials

    s3_client = s3_get_client(posthog_access_key_id, posthog_secret_access_key)

    snowplow_files = s3_list_files(s3_client, snowplow_s3_bucket, prefix=file_prefix)
    for file in snowplow_files:
        pass


"""
Load routines
"""


def posthog_get_credentials() -> tuple:
    """
    This function returns the set of PostHog secrets
    based on the the schema name provided.
    """

    posthog_project_api_key = env["POSTHOG_ACCESS_KEY_ID"]
    posthog_personal_api_key = env["POSTHOG_SECRET_ACCESS_KEY"]
    posthog_host = env["SNOWPLOW_S3_BUCKET"]

    return posthog_project_api_key, posthog_personal_api_key, posthog_host


def load_manifest_file(file_name: str) -> dict:
    """
    Load manifest file with schema definition
    """
    with open(file_name, "r", encoding=ENCODING) as yaml_file:
        return yaml.load(yaml_file, Loader=yaml.FullLoader)


def get_property_keys(schema_file: str, table_name: str) -> list:
    """
    Get list of property keys from the file
    """
    return load_manifest_file(file_name=schema_file).get(table_name, [])


def get_properties(property_list: str, values: list) -> dict:
    """
    Get key-value pairs for properties for uploading
    """
    return dict(zip_longest(property_list, values))


def get_upload_structure(schema_file: str, table_name: str, values: list) -> dict:
    """
    Return the prepared structure for the upload.
    Example of payload JSON to push to PostHog:
    {
        "event": "[event name]",
        "distinct_id": "[your users' distinct id]",
        "properties": {
            "key1": "value1",
            "key2": "value2"
        },
        "timestamp": "[optional timestamp in ISO 8601 format]"
    }

    """

    properties = get_properties(
        schema_file=schema_file, table_name=table_name, values=values
    )

    api_skeleton = {
        "event": "[event name]",
        "distinct_id": "[your users' distinct id]",
        "properties": {"key1": "value1", "key2": "value2"},
        "timestamp": "[optional timestamp in ISO 8601 format]",
    }

    api_skeleton["properties"] = properties

    return api_skeleton


def posthog_push_json(data: dict) -> None:
    """
    Use PostHog lib to push
    historical record to PostHog as a part of BackFill process
    """

    pass


def snowplow_posthog_backfill(month: str) -> None:
    """
    Entry point to trigger the back filling for Snowplow S3 -> PostHog
    """

    # get the data from S3 bucket
    # s3_extraction(file_prefix=curr_month)

    # transform data from .tsv -> .json

    # push data to PostHog
    # json_data = None

    # posthog_push_json(data=json_data)
    pass


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)

    Fire(
        {
            "snowplow_posthog_backfill": snowplow_posthog_backfill,
        }
    )
    info("Upload complete.")
