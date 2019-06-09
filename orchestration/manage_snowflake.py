#!/usr/bin/env python3
import logging
import sys
from os import environ as env
from typing import Dict, List

from fire import Fire
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


# Set logging defaults
logging.basicConfig(stream=sys.stdout, level=20)


class SnowflakeManager(object):
    def __init__(self, config_vars: Dict):
        self.engine = create_engine(
            URL(
                user=config_vars["SNOWFLAKE_USER"],
                password=config_vars["SNOWFLAKE_PASSWORD"],
                account=config_vars["SNOWFLAKE_ACCOUNT"],
                role=config_vars["SNOWFLAKE_SYSADMIN_ROLE"],
                warehouse=config_vars["SNOWFLAKE_LOAD_WAREHOUSE"],
            )
        )

        # Snowflake database name should be in CAPS
        # see https://gitlab.com/meltano/analytics/issues/491
        self.analytics_database = "{}_ANALYTICS".format(
            config_vars["SNOWFLAKE_DATABASE"].upper()
        )
        self.raw_database = "{}_RAW".format(config_vars["SNOWFLAKE_DATABASE"].upper())

    def generate_db_queries(
        self, database_name: str, cloned_database: str
    ) -> List[str]:
        """
        Generate the queries to clone and provide permissions for databases.
        """

        # Queries for database cloning and permissions
        check_db_exists_query = """use database "{0}";"""
        create_query = """create or replace database "{0}" clone {1};"""
        grant_query = """grant ownership on database "{0}" to TRANSFORMER;"""
        grant_roles_loader = (
            """grant create schema, usage on database "{0}" to LOADER"""
        )
        grant_roles_transformer = (
            """grant create schema, usage on database "{0}" to TRANSFORMER"""
        )

        # The order of the queries matters!
        queries = [
            check_db_exists_query.format(database_name),
            create_query.format(database_name, cloned_database),
            grant_query.format(database_name),
            grant_roles_transformer.format(database_name),
            grant_roles_loader.format(database_name),
        ]

        return queries

    def manage_clones(self, database: str, force: bool = False) -> None:
        """
        Manage zero copy clones in Snowflake.
        """

        clone_db = {"analytics": self.analytics_database, "raw": self.raw_database}
        queries = self.generate_db_queries(clone_db[database], database)

        # if force is false, check if the database exists
        if force:
            logging.info("Forcing a create or replace...")
            db_exists = False
        else:
            try:
                logging.info("Checking if DB exists...")
                connection = self.engine.connect()
                connection.execute(queries[0])
                logging.info("DBs exist...")
                db_exists = True
            except:
                logging.info("DB does not exist...")
                db_exists = False
            finally:
                connection.close()
                self.engine.dispose()

        # If the DB doesn't exist or --force is true, create or replace the db
        if not db_exists:
            logging.info("Creating or replacing DBs")
            for query in queries[1:]:
                try:
                    logging.info("Executing Query: {}".format(query))
                    connection = self.engine.connect()
                    [result] = connection.execute(query).fetchone()
                    logging.info("Query Result: {}".format(result))
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
                logging.info("Executing Query: {}".format(query))
                connection = self.engine.connect()
                [result] = connection.execute(query).fetchone()
                logging.info("Query Result: {}".format(result))
            finally:
                connection.close()
                self.engine.dispose()


if __name__ == "__main__":
    snowflake_manager = SnowflakeManager(env.copy())
    Fire(snowflake_manager)
