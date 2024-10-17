import warnings

import dagster
from dagster import (
    op, 
    MetadataValue, 
    Output, 
)
from dagster_duckdb import DuckDBResource
from duckdb import InvalidInputException
import numpy
import pandas

warnings.filterwarnings("ignore", category=dagster.ExperimentalWarning)

def create_drop_table_op(table_name: str):
    """
    factory to build ops
    """
    @op(name=f"drop_{table_name}_table")
    def drop_table_op(database: DuckDBResource) -> Output:
        """
        Drop a staging table and return metadata with the number of rows dropped.
        """
        with database.get_connection() as conn:
            try:
                # Check if the table exists and count rows
                result = conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchone()
                row_count = result[0] if result else 0
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            except InvalidInputException:
                row_count = 0

        return Output(
            value=row_count,
            metadata={
                "rows_dropped": MetadataValue.int(row_count)
            }
        )

    return drop_table_op
