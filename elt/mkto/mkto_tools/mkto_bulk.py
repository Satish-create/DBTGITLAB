#!/usr/bin/python3

import time
import json
import csv
import re
import os
import datetime
import requests

from .mkto_token import get_token, mk_endpoint
from .mkto_leads import get_leads_fieldnames_mkto, describe_leads, write_to_db_from_csv, upsert_to_db_from_csv, PRIMARY_KEY as LEADS_PRIMARY_KEY
from .mkto_activities import PRIMARY_KEY as ACTIVITIES_PRIMARY_KEY
from .mkto_utils import db_open, bulk_filter_builder, get_mkto_config
from config import MarketoSource, ExportType, ExportOutput


def auth_headers(token, content_type="application/json"):
    return {
        "Authorization": "Bearer {}".format(token),
        "Content-Type": content_type,
    }


FIELD_CREATED_AT = "createdAt"
FIELD_UPDATED_AT = "updatedAt"


def bulk_create_job(filter, data_type, fields=None, format="CSV", column_header_names=None):
    """
    Create a bulk job
    :param filter: dictionary of filtering parameters (createdAt, fields, activityIds, etc)
    :param data_type: "leads" or "activities"
    :param fields: Optional list of fields to filter by
    :param format: returns CSV file by default, other options are TSV and SSV
    :param column_header_names: optional dictionary of preferred column header names i.e. => {
          "firstName": "First Name",
          "email": "Email Address"
       }
    :return: json data
    """
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    create_url = "{}bulk/v1/{}/export/create.json".format(mk_endpoint, data_type)

    payload = {
        "format": format,
        "filter": filter,
    }

    if fields is not None:
        payload["fields"] = fields

    if column_header_names is not None:
        payload["columnHeaderNames"] = column_header_names

    response = requests.post(create_url,
                             json=payload,
                             headers=auth_headers(token))

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success"):
            return r_json
    else:
        return "Error"


def bulk_get_export_jobs(data_type, status=None, batch_size=10):
    """
    Get a list of previous jobs
    :param data_type: "leads" or "activities"
    :param status: Optional filter by status
    :param batch_size: returns 10 jobs by default
    :return: json data
    """

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    export_url = "{}bulk/v1/{}/export.json".format(mk_endpoint, data_type)

    payload = {
        "access_token": token,
        "batchSize": batch_size,
    }

    if status is not None:
        payload["status"] = ','.join(status)

    response = requests.get(export_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        return r_json
    else:
        return "Error"


def bulk_enqueue_job(data_type, export_id):
    """
    Enqueue a created job
    :param data_type: "leads" or "activites"
    :param export_id: guid
    :return: json data
    """
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    enqueue_url = "{}bulk/v1/{}/export/{}/enqueue.json".format(mk_endpoint, data_type, export_id)

    response = requests.post(enqueue_url, headers=auth_headers(token))

    if response.status_code == 200:
        return response
    else:
        return "Error"


def bulk_job_status(data_type, export_id):
    """
    Query for the status of a bulk job
    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return: json data
    """

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    status_url = "{}bulk/v1/{}/export/{}/status.json".format(mk_endpoint, data_type, export_id)

    payload = {
        "access_token": token,
    }

    response = requests.get(status_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        return r_json
    else:
        return "Error"


def bulk_get_file(data_type, export_id):
    """
    Download the CSV of a completed job. Can be called while job is still processsing.
    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return:
    """
    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    file_url = "{}bulk/v1/{}/export/{}/file.json".format(mk_endpoint, data_type, export_id)
    output_file = "{}.csv".format(data_type)

    payload = {
        "access_token": token
    }

    while True:
        status_result = bulk_job_status(data_type, export_id)
        job_status = status_result.get("result", [])[0].get("status")
        if job_status == "Completed":
            break
        elif job_status == "Failed":
            print("Job Failed")
            return
        else:
            print("Job Status is " + job_status)
            print("Waiting for 60 seconds.")
            time.sleep(60)
            continue

    with requests.Session() as s:
        # TODO It's possible for the token to expire between start of function and here!
        updated_token = get_token()
        download = s.get(file_url, params=payload)

        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')

    with open(file=output_file, mode='w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter=',',
                               quotechar='"', quoting=csv.QUOTE_MINIMAL)
        for row in cr:
            csvwriter.writerow(row)

        print("Writing File")


def bulk_cancel_job(data_type, export_id):
    """
    Cancel a currently running job.

    :param data_type: "leads" or "activities"
    :param export_id: guid
    :return:
    """

    token = get_token()
    if token == "Error":
        print("No job created. Token Error.")
        return

    cancel_url = "{}bulk/v1/{}/export/{}/cancel.json".format(mk_endpoint, data_type, export_id)

    response = requests.post(cancel_url, headers=auth_headers(token))

    if response.status_code == 200:
        return
    else:
        return "Error"


def bulk_export(args):
    fields = None
    activity_ids = None
    output_file = "{}.csv".format(args.source)

    # Validates that the date is of the format "YYYY-MM-DD".
    iso_check = re.compile(r'^\d{4}-\d{2}-\d{2}')
    if args.start is not None:
        try:
            iso_check.match(args.start)
            date_start = args.start + 'T00:00:00Z'
        except TypeError:
            print("Start date is not in the proper format.")
            return

    if args.end is not None:
        try:
            iso_check.match(args.end)
            date_end = args.end + 'T00:00:00Z'
        except TypeError:
            print("Start date is not in the proper format.")
            return

    if args.days is not None:
        date_now = datetime.datetime.now()
        next_day = date_now + datetime.timedelta(days=1)
        offset = date_now - datetime.timedelta(days=args.days)
        date_end = next_day.strftime("%Y-%m-%d") + 'T00:00:00Z'
        date_start = offset.strftime("%Y-%m-%d") + 'T00:00:00Z'

    if args.type == ExportType.CREATED:
        pull_type = FIELD_CREATED_AT

    if args.type == ExportType.UPDATED:
        pull_type = FIELD_UPDATED_AT

    if args.source == MarketoSource.ACTIVITIES:
        # If Activities, default is to get all activity types. All fields are returned by Marketo API by default
        activity_objects = get_mkto_config('Activities', 'objects')
        activity_ids = [int(get_mkto_config(ob, 'id')) for ob in activity_objects.split(',')]
        primary_key = ACTIVITIES_PRIMARY_KEY

    if args.source == MarketoSource.LEADS:
        # This is an API call to Marketo. Should probably pull from static config and periodically check for differences
        fields = get_leads_fieldnames_mkto(describe_leads())
        primary_key = LEADS_PRIMARY_KEY

    filter = bulk_filter_builder(start_date=date_start,
                                 end_date=date_end,
                                 pull_type=pull_type,
                                 activity_ids=activity_ids)
    new_job = bulk_create_job(
        filter=filter, data_type=args.source, fields=fields)
    print(json.dumps(new_job, indent=2))

    export_id = new_job.get("result", ["None"])[0].get("exportId")
    print("Enqueuing Job")
    bulk_enqueue_job(args.source, export_id)

    print("Get Results File")
    bulk_get_file(args.source, export_id)

    if args.output == ExportOutput.DB:
        print("Upserting to Database")
        with db_open(**vars(args)) as db:
            upsert_to_db_from_csv(db, output_file, primary_key)

    if args.nodelete or args.output == ExportOutput.FILE:
        return
    else:
        os.remove(output_file)
