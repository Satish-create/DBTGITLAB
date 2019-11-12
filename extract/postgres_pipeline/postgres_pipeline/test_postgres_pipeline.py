import os
import sys

# Path magic so the tests can run
sys.path.insert(
    0,
    os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        "postgres_pipeline",
    ),
)

from gitlabdata.orchestration_utils import snowflake_engine_factory, query_executor
import pandas as pd

from main import load_incremental, load_scd, check_new_tables, sync_incremental_ids
from utils import (
    dataframe_uploader,
    manifest_reader,
    postgres_engine_factory,
    query_results_generator,
)

# Set up some fixtures
CONFIG_DICT = os.environ.copy()
CONNECTION_DICT = {
    "user": "GITLAB_COM_DB_USER",
    "pass": "GITLAB_COM_DB_PASS",
    "host": "GITLAB_COM_DB_HOST",
    "database": "GITLAB_COM_DB_NAME",
    "port": "PG_PORT",
}
POSTGRES_ENGINE = postgres_engine_factory(CONNECTION_DICT, CONFIG_DICT)
SNOWFLAKE_ENGINE = snowflake_engine_factory(CONFIG_DICT, "SYSADMIN", "TEST")
print(SNOWFLAKE_ENGINE)
TEST_TABLE = "TAP_POSTGRES_TEST_TABLE"
TEST_TABLE_TEMP = TEST_TABLE + "_TEMP"


def table_cleanup(table, cleanup_mode: str = "DROP"):
    """
    Cleanup the test table that is reused during testing.
    """
    try:
        connection = SNOWFLAKE_ENGINE.connect()
        connection.execute(f"{cleanup_mode} TABLE IF EXISTS {table};")
    finally:
        connection.close()
        SNOWFLAKE_ENGINE.dispose()


table_cleanup(TEST_TABLE)
table_cleanup(TEST_TABLE_TEMP)


class TestPostgresPipeline:
    def test_query_results_generator(self):
        """
        Test loading a dataframe by querying a known table.
        Confirm this doesn't throw errors.
        """
        query = "SELECT * FROM approvals WHERE id = (SELECT MAX(id) FROM approvals)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        assert query_results

    def test_dataframe_uploader1(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup(TEST_TABLE)
        query = "SELECT * FROM approver_groups WHERE id = (SELECT MAX(id) FROM approver_groups)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_dataframe_uploader2(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup(TEST_TABLE)
        query = "SELECT * FROM merge_requests WHERE id = (SELECT MAX(id) FROM merge_requests)"
        query_results = query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_incremental_users(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2019-01-01T00:00:00+00:00"
        hours = "10"
        source_table = "users"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_users"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        load_incremental(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_incremental_merge_requests(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2019-06-29T10:00:00+00:00"
        hours = "2"
        source_table = "merge_requests"
        source_db = "gitlab_com_db"
        table_query = (
            f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_merge_requests"
        )
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        load_incremental(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_incremental_merge_request_metrics(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE)
        # Set some env_vars for this run
        execution_date = "2019-07-20T10:00:00+00:00"
        hours = "2"
        source_table = "merge_request_metrics"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_merge_request_metrics"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        load_incremental(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_scd_ci_variables_resync(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "ci_variables"
        source_db = "gitlab_com_db"

        # Get the count from the source DB
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        load_scd(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE_TEMP
        )
        target_count_query = (
            f"SELECT COUNT(*) - 10000 AS row_count FROM {TEST_TABLE_TEMP}"
        )
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_scd_project_import_data_normal(self):
        """
        Test to make sure that the SCD loader is working as intended.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "project_import_data"
        source_db = "gitlab_com_db"
        table_query = f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_project_import_data"
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the count from the source DB
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        load_scd(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_load_test_board_labels(self):
        """
        Test to make sure that the new table checker is fucntioning.
        """
        table_cleanup(TEST_TABLE)
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "board_labels"
        source_db = "gitlab_com_db"
        table_query = (
            f"create table {TEST_TABLE} like raw.tap_postgres.gitlab_db_board_labels"
        )
        query_executor(SNOWFLAKE_ENGINE, table_query)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        check_succeeded = check_new_tables(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE_TEMP
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE_TEMP}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert target_count_results["row_count"][0] > 0

    def test_sync_incremental_ids_empty(self):
        """
        Test to make sure that the sync_incremental_ids function doesn't break
        when the temp table exists but contains no rows.
        """
        table_cleanup(TEST_TABLE_TEMP)
        # Set some env_vars for this run
        source_table = "approver_groups"
        source_db = "gitlab_com_db"
        temp_table_query = f"create table {TEST_TABLE_TEMP} like raw.tap_postgres.gitlab_db_approver_groups"
        query_executor(SNOWFLAKE_ENGINE, temp_table_query)

        # Get the manifest for a specific table
        file_path = f"extract/postgres_pipeline/manifests/{source_db}_manifest.yaml"
        manifest_dict = manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        return_status = sync_incremental_ids(
            POSTGRES_ENGINE, SNOWFLAKE_ENGINE, source_table, table_dict, TEST_TABLE_TEMP
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE_TEMP}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert return_status
        assert target_count_results["row_count"][0] > 0
