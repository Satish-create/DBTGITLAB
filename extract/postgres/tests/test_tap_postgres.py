import os

from gitlabdata.orchestration_utils import snowflake_engine_factory
import pandas as pd

from ..tap_postgres import tap_postgres


# Set up some fixtures
CONFIG_DICT = os.environ.copy()
CONNECTION_DICT = {
    "user": "GITLAB_COM_DB_USER",
    "pass": "GITLAB_COM_DB_PASS",
    "host": "GITLAB_COM_DB_HOST",
    "database": "GITLAB_COM_DB_NAME",
    "port": "PG_PORT",
}
POSTGRES_ENGINE = tap_postgres.postgres_engine_factory(CONNECTION_DICT, CONFIG_DICT)
SNOWFLAKE_ENGINE = snowflake_engine_factory(CONFIG_DICT, "SYSADMIN", "TEST")
TEST_TABLE = "tap_postgres_test_table"


def table_cleanup():
    """
    Cleanup the test table that is reused during testing.
    """
    try:
        connection = SNOWFLAKE_ENGINE.connect()
        connection.execute(f"DROP TABLE IF EXISTS {TEST_TABLE}")
    finally:
        connection.close()
        SNOWFLAKE_ENGINE.dispose()


class TestTapPostgres:
    def test_query_results_generator(self):
        """
        Test loading a dataframe by querying a known table.
        Confirm this doesn't throw errors.
        """
        query = "SELECT * FROM approvals WHERE id = (SELECT MAX(id) FROM approvals)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        assert query_results

    def test_dataframe_uploader1(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup()
        query = "SELECT * FROM approver_groups WHERE id = (SELECT MAX(id) FROM approver_groups)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            tap_postgres.dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_dataframe_uploader2(self):
        """
        Attempt to upload a dataframe that includes a datetime index from postgres.
        """
        table_cleanup()
        query = "SELECT * FROM merge_requests WHERE id = (SELECT MAX(id) FROM merge_requests)"
        query_results = tap_postgres.query_results_generator(query, POSTGRES_ENGINE)
        for result in query_results:
            tap_postgres.dataframe_uploader(result, SNOWFLAKE_ENGINE, TEST_TABLE)
        uploaded_rows = pd.read_sql(f"select * from {TEST_TABLE}", SNOWFLAKE_ENGINE)
        assert uploaded_rows.shape[0] == 1

    def test_load_incremental1(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup()
        # Set some env_vars for this run
        execution_date = "2019-06-29T10:00:00+00:00"
        hours = "2"
        source_table = "merge_requests"
        source_db = "gitlab_com_db"

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"analytics/extract/postgres/manifests/{source_db}_manifest.yaml"
        manifest_dict = tap_postgres.manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        tap_postgres.load_incremental(
            table_dict["import_query"],
            POSTGRES_ENGINE,
            SNOWFLAKE_ENGINE,
            source_table,
            TEST_TABLE,
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)

    def test_load_incremental2(self):
        """
        Test to make sure that the incremental loader is catching all of the rows.
        """
        table_cleanup()
        # Set some env_vars for this run
        execution_date = "2019-06-29T10:00:00+00:00"
        hours = "2"
        source_table = "merge_request_metrics"
        source_db = "gitlab_com_db"

        # Get the count from the source DB
        source_between_clause = f"updated_at BETWEEN '{execution_date}'::timestamp - interval '{hours} hours' AND '{execution_date}'::timestamp"
        source_count_query = f"SELECT COUNT(*) AS row_count FROM {source_table} WHERE {source_between_clause}"
        source_count_results = pd.read_sql(source_count_query, POSTGRES_ENGINE)

        # Get the manifest for a specific table
        file_path = f"analytics/extract/postgres/manifests/{source_db}_manifest.yaml"
        manifest_dict = tap_postgres.manifest_reader(file_path)
        table_dict = manifest_dict["tables"][source_table]

        # Run the query and count the results
        new_env_vars = {"EXECUTION_DATE": execution_date, "HOURS": hours}
        os.environ.update(new_env_vars)
        tap_postgres.load_incremental(
            table_dict["import_query"],
            POSTGRES_ENGINE,
            SNOWFLAKE_ENGINE,
            source_table,
            TEST_TABLE,
        )
        target_count_query = f"SELECT COUNT(*) AS row_count FROM {TEST_TABLE}"
        target_count_results = pd.read_sql(target_count_query, SNOWFLAKE_ENGINE)

        assert source_count_results.equals(target_count_results)
