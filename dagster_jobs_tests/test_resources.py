import tempfile
import pytest
import duckdb
from dagster import build_op_context
from dagster_duckdb import DuckDBResource
from dagster import resource

@resource
def test_duckdb_resource(_):

    # Initialize in-memory DuckDB database
    conn = duckdb.connect(database=':memory:', read_only=False)
    conn.execute("CREATE TABLE IF NOT EXISTS dummy (id INTEGER);")
    return conn

# Define a fixture for the mock context and database resource
@pytest.fixture
def mock_context():
    mock_partition_key = "2024-03-21"
    mock_database = test_duckdb_resource(None)
    context = build_op_context(
        partition_key=mock_partition_key,
        resources={"database": mock_database}
    )
    return context, mock_database


