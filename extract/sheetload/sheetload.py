from logging import exception, info, basicConfig
from os import environ as env
import sys
from time import time
from typing import Dict, List
from yaml import load

from fire import Fire
import gspread
from gspread.exceptions import SpreadsheetNotFound
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from snowflake.sqlalchemy import URL as snowflake_URL
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

SHEETLOAD_SCHEMA = 'sheetload'

def postgres_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    db_address = args['PG_ADDRESS']
    db_database = args['PG_DATABASE']
    db_port = args['PG_PORT']
    db_username = args['PG_USERNAME']
    db_password = args['PG_PASSWORD']

    conn_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_username,
                                                       db_password,
                                                       db_address,
                                                       db_port,
                                                       db_database)

    return create_engine(conn_string)


def snowflake_engine_factory(args: Dict[str, str]) -> Engine:
    """
    Create a database engine from a dictionary of database info.
    """

    conn_string = snowflake_URL(
            user = args['SNOWFLAKE_LOAD_USER'],
            password = args['SNOWFLAKE_PASSWORD'],
            account = args['SNOWFLAKE_ACCOUNT'],
            database = args['SNOWFLAKE_LOAD_DATABASE'],
            warehouse = args['SNOWFLAKE_LOAD_WAREHOUSE'],
            role = args["SNOWFLAKE_LOAD_ROLE"]
    )

    return create_engine(conn_string)


def dw_uploader(engine: Engine, table: str, schema: str,
                data: pd.DataFrame) -> bool:
    """
    Use a DB engine to upload a dataframe.
    """

    # Clean the column names
    data.columns = [column_name.replace(' ','_').replace('/','_')
                    for column_name in data.columns]
    try:
        if engine.has_table(table, schema):
            existing_table = (pd.read_sql_table(table, engine, schema)
                                .drop('updated_at', axis=1))
            if existing_table.equals(data):
                info('Table "{}" has not changed. Aborting upload.'.format(table))
                return False
            engine.connect().execute('drop table {}.{} cascade'.format(schema, table))
        data['updated_at'] = time()
        data.to_sql(name=table,con=engine,schema=schema,index=False)
        info('Successfully loaded {} rows into {}.{}'.format(data.shape[0],
                                                          schema, table))
        return True
    except Exception as e:
        info(repr(e))
        info('Failed to load {}.{}'.format(schema, table))
        raise


def csv_loader(*paths: List[str], conn_dict: Dict[str,str] = None)  -> None:
    """
    Load data from a csv file into a DataFrame and pass it to dw_uploader.

    Loader expects the naming convention of the file to be:
    <schema>.<table>.csv

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Paths is a list that is separated spaces. i.e.:
    python spreadsheet_loader.py csv <path_1> <path_2> <path_3> ...
    """

    engine = engine_factory(conn_dict or env)
    # Extract the schema and the table name from the file name
    for path in paths:
        schema, table = path.split('/')[-1].split('.')[0:2]
        try:
            sheet_df =  pd.read_csv(path)
        except FileNotFoundError:
            info('File {} not found.'.format(path))
            continue
        dw_uploader(engine, table, schema, sheet_df)


def sheet_loader(sheet_file: str, destination: str, gapi_keyfile: str = None,
                 conn_dict: Dict[str, str] = None) -> None:
    """
    Load data from a google sheet into a DataFrame and pass it to dw_uploader.
    The sheet must have been shared with the google service account of the runner.

    Loader expects the name of the sheet to be:
    <sheet_name>.<tab>
    The tab name will become the table name.

    Column names can not contain parentheses. Spaces and slashes will be
    replaced with underscores.

    Sheets is a newline delimited txt fileseparated spaces.

    python spreadsheet_loader.py sheets <file_name> <snowflake|postgres>
    """

    with open(sheet_file, 'r') as file:
        sheets = file.read().splitlines()

    # This dictionary determines what engine gets created
    engine_dict = {'postgres': postgres_engine_factory,
                   'snowflake': snowflake_engine_factory}

    engine = engine_dict[destination](conn_dict or env)
    # Get the credentials for sheets and the database engine
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    keyfile = load(gapi_keyfile or env['GCP_SERVICE_CREDS'])
    credentials = (ServiceAccountCredentials.from_json_keyfile_dict(keyfile, scope))
    gc = gspread.authorize(credentials)

    for sheet_info in sheets:
        # Sheet here refers to the name of the sheet file, table is the actual sheet name
        sheet_file, table = sheet_info.split('.')
        try:
            sheet = gc.open(SHEETLOAD_SCHEMA + '.' + sheet_file).worksheet(table).get_all_values()
        except SpreadsheetNotFound:
            info('Sheet {} not found.'.format(sheet_info))
            raise
        sheet_df = pd.DataFrame(sheet[1:], columns=sheet[0])
        dw_uploader(engine, table, SHEETLOAD_SCHEMA, sheet_df)

    if destination == 'snowflake':
        try:
            query = 'grant select on all tables in schema raw.{} to role transformer'.format(SHEETLOAD_SCHEMA)
            connection = engine.connect()
            connection.execute(query)
        finally:
            connection.close()
        info('Permissions granted.')


if __name__ == "__main__":
    basicConfig(stream=sys.stdout, level=20)
    Fire({
        'csv': csv_loader,
        'sheets': sheet_loader,
    })
    info('Done.')

