#!/usr/bin/python3

import os
import json
import logging
from datetime import datetime

import psycopg2
from simple_salesforce import Salesforce
from toolz import dicttoolz
from hosts_to_sfdc.dw_setup import host, username, password, database


sf_username= os.environ.get('SFDC_SBOX_USERNAME')
sf_password= os.environ.get('SFDC_SBOX_PASSWORD')
sf_security_token= os.environ.get('SFDC_SBOX_SECURITY_TOKEN')

sf = Salesforce(username=sf_username, password=sf_password, security_token=sf_security_token, sandbox=True)

mydb = psycopg2.connect(host=host, user=username,
                            password=password, dbname=database)
cursor = mydb.cursor()

def get_sfdc_fieldnames(object_name):
    sfdc_object = getattr(sf, object_name).describe()

    sf_fields = [field.get("name", "Error") for field in sfdc_object.get("fields", [])]

    return sf_fields


# Match Postgres fields with SFDC fields
def generate_column_mapping(postgres_table, sfdc_object):
    """
    Returns a dictionary mapping the postgres column name to the sfdc object name
    :param postgres_table:
    :param sfdc_object:
    :return:
    """
    db_query = "SELECT column_name FROM information_schema.columns WHERE table_name = %s"
    logger.debug("Executing query %s", cursor.mogrify(db_query, (postgres_table,)))
    col_cursor=mydb.cursor()
    col_cursor.execute(db_query, (postgres_table,))

    db_fields = [result[0] for result in col_cursor]
    sf_fields = get_sfdc_fieldnames(sfdc_object)

    mapping = dict()

    for db_col in db_fields:
        for sf_col in sf_fields:
            if db_col == sf_col.lower():
                mapping[db_col] = sf_col

    return mapping


# Get Hosts to Upload
def upload_hosts():
    """
    Funciton that gets all host records, checks them against what exists in
    SFDC, upserts if they exist and inserts if they don't.
    :return:
    """
    host_query = "SELECT * FROM version.libre_sfdc_accounts"
    host_cursor = mydb.cursor()
    host_cursor.execute(host_query)

    column_mapping = generate_column_mapping('libre_sfdc_accounts', 'Host__c')

    #Generate an ordered list of the correct column mappings
    correct_column_names = [column_mapping.get(desc[0]) for desc in host_cursor.description]

    # Match on the ID of the host record so we upsert instead of insert
    all_sfdc_hosts = sf.query_all("SELECT Id, Name FROM Host__c")

    # Create dictionary of {"SFDC Name": "SFDC Id"}
    id_mapping=dict()
    if all_sfdc_hosts.get("done") is True:
        for result in all_sfdc_hosts.get("records"):
            id_mapping[result.get("Name", "None")] = result.get("Id", "None")


    # Generate objects to write to SFDC via bulk query
    insert_obj = []
    upsert_obj = []

    # Iterate through each host record from Postgres
    for result in host_cursor:
        tmp_dict = dict(zip(correct_column_names, list(result)))
        possible_id = id_mapping.get(tmp_dict.get('Name'), None)
        if possible_id is not None:
            tmp_dict["Id"] = possible_id
        for key in tmp_dict:
            if isinstance(tmp_dict[key], datetime):
                tmp_dict[key] = str(tmp_dict[key].strftime("%Y-%m-%d"))
            if tmp_dict[key] is None:
                tmp_dict = dicttoolz.dissoc(tmp_dict, key)

        if 'Id' in tmp_dict:
            upsert_obj.append(tmp_dict)
        else:
            insert_obj.append(tmp_dict)


    upsert_count = len(upsert_obj)
    if upsert_count != 0:
        logger.debug("%s hosts to upsert.", upsert_count)
        upsert_results = sf.bulk.Host__c.upsert(upsert_obj, "Id")

        for result in upsert_results:
            if result.get("success", True) is False:
                logger.debug("Error on SFDC id: %s", result.get("id", None))
                for error in result.get("errors", []):
                    new_error=dicttoolz.dissoc(error, "message")
                    logger.debug(json.dumps(new_error, indent=2))
    else:
        logger.debug("No hosts to upsert.")

    insert_count = len(insert_obj)
    if insert_count != 0:
        logger.debug("%s hosts to insert.", insert_count)
        insert_results = sf.bulk.Host__c.insert(insert_obj)

        for result in insert_results:
            if result.get("success", True) is False:
                logger.debug("Error on SFDC id: %s", result.get("id", None))
                for error in result.get("errors", []):
                    new_error=dicttoolz.dissoc(error, "message")
                    logger.debug(json.dumps(new_error, indent=2))
    else:
        logger.debug("No hosts to insert.")


def generate_accounts():
    account_query = "SELECT * FROM version.sfdc_accounts_gen"
    account_cursor = mydb.cursor()
    account_cursor.execute(account_query)
    logger.debug("Found %s accounts to generate.", account_cursor.rowcount)

    column_mapping = generate_column_mapping('sfdc_accounts_gen', 'Account')
    correct_column_names = [column_mapping.get(desc[0]) for desc in account_cursor.description]

    #TODO Need to get the account that will create this in the future.
    # Checks for Accounts that were created by the API user
    sfdc_account_query = sf.bulk.Account.query("SELECT Id, Name, Website FROM Account WHERE CreatedById='00561000002rsNT'")

    # Generates a unique string to compare against
    existing_accounts = {}
    for account in sfdc_account_query:
        account_string = account.get("Name") + account.get("Website")
        existing_accounts[account_string]=account.get("Id")


    write_obj = []
    for result in account_cursor:

        # Skips host if host is already an Account
        result_string = result[0] + result[1]
        if existing_accounts.get(result_string, None) is not None:
            logger.debug("Skipping host record. Already present as account %s", existing_accounts.get(result_string))
            continue

        tmp_dict = dict(zip(correct_column_names, list(result)))
        write_obj.append(tmp_dict)

    logger.debug("Generating %s accounts.", len(write_obj))

    # Generate SFDC Accounts
    account_results = sf.bulk.Account.insert(write_obj)

    for result in account_results:
        if result.get("success", True) is False:
            logger.debug("Error on SFDC id: %s", result.get("id", None))
            for error in result.get("errors", []):
                new_error = dicttoolz.dissoc(error, "message")
                logger.debug(json.dumps(new_error, indent=2))
        else:
            logger.debug("Generedated Account - %s", result.get("id", None))

def delete_all_hosts(sf_conn):
    """
    Delete all hosts files in SFDC. Use with caution!
    :param sf_conn:
    :return:
    """
    query = sf_conn.query("SELECT Id from Host__c")

    host_count = query.get("totalSize")

    if host_count == 0:
        logger.debug("No hosts to delete.")
        return

    logger.debug("Found %s hosts to delete.", query.get("totalSize"))

    items_to_delete = []
    for record in query.get("records"):
        items_to_delete.append({"Id": record.get("Id")})

    results = sf_conn.bulk.Host__c.delete(items_to_delete)

    print(results)



logger = logging.getLogger(__name__)

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%Y-%m-%d %I:%M:%S %p')
    logging.getLogger(__name__).setLevel(logging.DEBUG)
    upload_hosts()
    generate_accounts()
    # delete_all_hosts(sf)

# TODO will need to keep track of errors so I can associate them with the host file
