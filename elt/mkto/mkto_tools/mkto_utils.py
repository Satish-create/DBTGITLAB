#!/usr/bin/python3
import os
import requests
import psycopg2

from configparser import SafeConfigParser
from .mkto_token import get_token, mk_endpoint


def get_mkto_config(section, field):
    """
    Generic function for getting marketo config info
    :param section: The section in the INI config file
    :param field: The key of the key/value pairs in a section
    :return:
    """
    myDir = os.path.dirname(os.path.abspath(__file__))
    myPath = os.path.join(myDir, "../../config", "mktoFields.conf")
    parser = SafeConfigParser()
    parser.read(myPath)
    values = parser.get(section, field)
    return values


def bulk_filter_builder(start_date, end_date, pull_type, activity_ids=None):
    """
    Helper function to build the filter payload.
    :param start_date: Time stamp of the form 2018-01-01T00:00:00Z
    :param end_date: Time stamp of the form 2018-01-01T00:00:00Z
    :param pull_type: Either "createdAt" or "updatedAt"
    :param activity_ids: Optional list of activity ids
    :return: Dictionary of filter object
    """
    filter = {
        pull_type: {
            "startAt": start_date,
            "endAt": end_date
        }
    }

    if activity_ids is not None:
        filter["activityTypeIds"] = activity_ids

    return filter


def get_from_lead_db(item, item_id=None):
    # Designed for getting campaigns and lists, with an optional Id for each.
    token = get_token()
    if token == "Error":
        print("Token Error")
        return

    lead_db_url = "{}rest/v1/{}".format(mk_endpoint, item)
    if item_id is not None:
        lead_db_url += "/{}".format(item_id)

    lead_db_url += ".json"

    payload = {
        "access_token": token
    }

    response = requests.get(lead_db_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"


def get_asset(asset):
    # For getting programs, primarily
    token = get_token()
    if token == "Error":
        print("Token Error")
        return

    asset_url = "{}rest/asset/v1/{}.json".format(mk_endpoint, asset)

    payload = {
        "access_token": token
    }

    response = requests.get(asset_url, params=payload)

    if response.status_code == 200:
        r_json = response.json()
        if r_json.get("success") is True:
            return r_json
    else:
        return "Error"
