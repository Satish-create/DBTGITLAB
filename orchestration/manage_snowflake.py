#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class SnowflakeManager(object):
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
                        URL(user=config_vars['SNOWFLAKE_USER'],
                            password=config_vars['SNOWFLAKE_PASSWORD'],
                            account=config_vars['SNOWFLAKE_ACCOUNT'],
                            role=config_vars['SNOWFLAKE_SYSADMIN_ROLE'],
                            warehouse=config_vars['SNOWFLAKE_LOAD_WAREHOUSE']))

        # Snowflake database name should be in CAPS
        # see https://gitlab.com/meltano/analytics/issues/491
        self.analytics_database = "{}_ANALYTICS".format(config_vars['SNOWFLAKE_DATABASE'].upper())
        self.raw_database = "{}_RAW".format(config_vars['SNOWFLAKE_DATABASE'].upper())

    def manage_clones(self, force: bool=False) -> None:
        """
        Manage zero copy clones in Snowflake.
        """

        # Queries for database cloning
        create_analytics_query = """create or replace database "{0}" clone ANALYTICS;"""
        grant_analytics_query = """grant ownership on database "{0}" to TRANSFORMER;"""

        create_raw_query = """create or replace database "{0}" clone RAW;"""
        grant_raw_query = """grant ownership on database "{0}" to LOADER;"""

        grant_roles_loader = """grant create schema, usage on database "{0}" to LOADER"""
        grant_roles_transformer = """grant create schema, usage on database "{0}" to TRANSFORMER"""

        # Put all of the queries in a list and format them
        queries = [
                create_analytics_query.format(self.analytics_database),
                grant_analytics_query.format(self.analytics_database),
                grant_roles_transformer.format(self.analytics_database),
                grant_roles_loader.format(self.analytics_database),
                create_raw_query.format(self.raw_database),
                grant_raw_query.format(self.raw_database),
                grant_roles_transformer.format(self.raw_database),
                grant_roles_loader.format(self.raw_database),
        ]

        # if force is false, check if the database exists
        if force:
            logging.info('Forcing a create or replace...')
            db_exists = False
        else:
            try:
                logging.info('Checking if DB exists...')
                analytics_query = 'use database "{}";'.format(self.analytics_database)
                raw_query = 'use database "{}";'.format(self.raw_database)
                connection = self.engine.connect()
                connection.execute(analytics_query)
                connection.execute(raw_query)
                logging.info('DBs exist...')
                db_exists = True
            except:
                logging.info('At least one DB does not exist...')
                db_exists = False
            finally:
                connection.close()
                self.engine.dispose()

        # If the DB doesn't exist or --force is true, create or replace the db
        if not db_exists:
            logging.info('Creating or replacing DBs')
            try:
                for query in queries:
                    logging.info('Executing Query: {}'.format(query))
                    connection = self.engine.connect()
                    [result] = connection.execute(query).fetchone()
                    logging.info('Query Result: {}'.format(result))
            finally:
                connection.close()
                self.engine.dispose()

    def delete_clone(self):
        """
        Delete a clone.
        """
        db_list = [self.analytics_database, self.raw_database]

        for db in db_list:
            query = 'drop database "{}";'.format(db)
            try:
                logging.info('Executing Query: {}'.format(query))
                connection = self.engine.connect()
                [result] = connection.execute(query).fetchone()
                logging.info('Query Result: {}'.format(result))
            finally:
                connection.close()
                self.engine.dispose()


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)

