import logging
import os
from typing import Dict, Any

from fire import Fire
from gitlabdata.orchestration_utils import snowflake_engine_factory
from sqlalchemy.engine.base import Engine

import utils


SCHEMA = "tap_postgres"


def swap_temp_table(engine: Engine, real_table: str, temp_table: str) -> None:
    """
    Drop the real table and rename the temp table to take the place of the
    real table.
    """

    try:
        connection = engine.connect()
        connection.execute(
            f"ALTER TABLE tap_postgres.{temp_table} SWAP WITH tap_postgres.{real_table}"
        )
        logging.info(f"Table altered from {temp_table} to {real_table}")
        connection.execute(f"DROP TABLE tap_postgres.{temp_table}")
        logging.info(f"Dropped table: {temp_table}")
    finally:
        connection.close()
        engine.dispose()


def load_incremental(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> None:
    """
    Load tables incrementally based off of the execution date.
    """

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")
    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return
    env = os.environ.copy()
    query = f"{raw_query.format(**env)} {additional_filter}"
    logging.info(query)
    utils.chunk_and_upload(query, source_engine, target_engine, table_name)
    return query


def sync_incremental_ids(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> None:
    """
    Sync incrementally-loaded tables based on their IDs.
    """

    raw_query = table_dict["import_query"]
    additional_filtering = table_dict.get("additional_filtering", "")
    if "{EXECUTION_DATE}" not in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return

    id_queries = utils.id_query_generator(
        table, table_name, raw_query, target_engine, source_engine
    )
    # Iterate through the generated queries
    for query in id_queries:
        filtered_query = f"{query} {additional_filtering} ORDER BY id"
        logging.info(filtered_query)
        utils.chunk_and_upload(filtered_query, source_engine, target_engine, table_name)


def load_scd(
    source_engine: Engine,
    target_engine: Engine,
    table: str,
    table_dict: Dict[Any, Any],
    table_name: str,
) -> None:
    """
    Load tables that are slow-changing dimensions.
    """

    raw_query = table_dict["import_query"]
    additional_filter = table_dict.get("additional_filtering", "")
    if "{EXECUTION_DATE}" in raw_query:
        logging.info(f"Table {table} does not need processing.")
        return
    logging.info(f"Processing table: {table}")

    query = f"{raw_query} {additional_filter}"
    logging.info(query)
    utils.chunk_and_upload(query, source_engine, target_engine, table_name)


def main(file_path: str, load_type: str = None) -> None:
    """
    Read data from a postgres DB and upload it directly to Snowflake.
    """

    # Process the manifest
    logging.info(f"Reading manifest at location: {file_path}")
    manifest_dict = utils.manifest_reader(file_path)

    postgres_engine, snowflake_engine = utils.get_engines(
        manifest_dict["connection_info"]
    )
    logging.info(snowflake_engine)

    # Link the load_types to their respective functions
    load_types = {
        "incremental": load_incremental,
        "scd": load_scd,
        "sync": sync_incremental_ids,
    }

    for table in manifest_dict["tables"]:
        logging.info(f"Processing Table: {table}")
        table_dict = manifest_dict["tables"][table]
        table_name = "{import_db}_{export_table}".format(**table_dict).upper()
        raw_query = table_dict["import_query"]
        is_incremental = "{EXECUTION_DATE}" in raw_query

        # Check if the schema has changed, and if so then do a full load
        schema_changed = utils.check_if_schema_changed(
            raw_query,
            postgres_engine,
            table_dict["export_table"],
            table_dict["export_table_primary_key"],
            snowflake_engine,
            table_name,
        )
        if schema_changed:
            real_table_name = table_name
            table_name = f"{table_name}_TEMP"
            load_type = "sync" if is_incremental else load_type
            logging.info(f"Schema has changed, backfilling table into: {table_name}")

        # Call the correct function based on the load_type
        load_types[load_type](
            postgres_engine, snowflake_engine, table, table_dict, table_name
        )
        logging.info(f"Finished upload for table: {table}")

        # Drop the original table and rename the temp table
        if schema_changed:
            swap_temp_table(snowflake_engine, real_table_name, table_name)

        postgres_engine.dispose()
        snowflake_engine.dispose()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("snowflake.connector.cursor").disabled = True
    Fire({"tap": main})
