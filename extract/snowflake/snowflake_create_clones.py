#!/usr/bin/env python3
import argparse
import logging
import sys

from os import environ as env
from gitlabdata.orchestration_utils import snowflake_engine_factory


def create_table_clone(
    source_schema: str,
    source_table: str,
    target_table: str,
    target_schema: str = None,
    timestamp: str = None,
):
    """
    timestamp: timestamp indicating time to
    """
    timestamp_format = """yyyy-mm-dd hh:mi:ss"""
    if not target_schema:
        target_schema = source_schema

    print(env)
    engine = snowflake_engine_factory(env, "CI_USER")
    print(engine)
    database = env["SNOWFLAKE_TRANSFORM_DATABASE"]
    queries = [
        f"""USE "{database}"; """,
    ]
    # Tries to create the schema its about to write to
    # If it does exists, {schema} already exists, statement succeeded.
    # is returned.
    schema_check = f"""CREATE SCHEMA IF NOT EXISTS "{database}".{target_schema};"""
    queries.append(schema_check)

    clone_sql = f"""create table if not exists {target_schema}.{target_table} clone "{database}".{source_schema}.{source_table}"""
    if timestamp and timestamp_format:
        clone_sql += f""" at (timestamp => to_timestamp_tz('{timestamp}', '{timestamp_format}'))"""
    clone_sql += " COPY GRANTS;"
    queries.append(f"drop table if exists {target_schema}.{target_table};")
    queries.append(clone_sql)

    connection = engine.connect()
    try:
        for q in queries:
            logging.info(q)
            results = connection.execute(q).fetchall()
            logging.info(results)
    finally:
        connection.close()
        engine.dispose()


if __name__ == "__main__":
    print(sys.argv[1:])

    parser = argparse.ArgumentParser()
    parser.add_argument("--source_schema")
    parser.add_argument("--source_table")
    parser.add_argument("--target_schema")
    parser.add_argument("--target_table")
    parser.add_argument("--timestamp")

    args = parser.parse_args()
    create_table_clone(**vars(args))
