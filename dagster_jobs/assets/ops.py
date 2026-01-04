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

# Dagster 1.12 removed ExperimentalWarning; ignore if present for older versions
try:
    _exp_warn = getattr(dagster, "ExperimentalWarning", None)
    if _exp_warn:
        warnings.filterwarnings("ignore", category=_exp_warn)
except Exception:
    pass

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


def create_backup_table_op(table_name: str):
    """
    factory to build backup ops that copy a table to {table_name}_backup
    """
    @op(name=f"backup_{table_name}_table")
    def backup_table_op(database: DuckDBResource) -> Output:
        """
        Backup a staging table by copying it to a _backup table.
        """
        backup_name = f"{table_name}_backup"
        with database.get_connection() as conn:
            try:
                # Check if the source table exists and count rows
                result = conn.execute(f"SELECT COUNT(*) as row_count FROM {table_name}").fetchone()
                row_count = result[0] if result else 0
                # Drop existing backup and create new one
                conn.execute(f"DROP TABLE IF EXISTS {backup_name}")
                conn.execute(f"CREATE TABLE {backup_name} AS SELECT * FROM {table_name}")
            except InvalidInputException:
                row_count = 0

        return Output(
            value=row_count,
            metadata={
                "rows_backed_up": MetadataValue.int(row_count),
                "backup_table": MetadataValue.text(backup_name)
            }
        )

    return backup_table_op


def create_restore_table_op(table_name: str):
    """
    factory to build restore ops that restore a table from {table_name}_backup
    """
    @op(name=f"restore_{table_name}_table")
    def restore_table_op(database: DuckDBResource) -> Output:
        """
        Restore a staging table from its _backup table.
        """
        backup_name = f"{table_name}_backup"
        with database.get_connection() as conn:
            try:
                # Check if the backup table exists and count rows
                result = conn.execute(f"SELECT COUNT(*) as row_count FROM {backup_name}").fetchone()
                row_count = result[0] if result else 0
                # Drop existing table and restore from backup
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM {backup_name}")
            except InvalidInputException:
                row_count = 0

        return Output(
            value=row_count,
            metadata={
                "rows_restored": MetadataValue.int(row_count),
                "restored_from": MetadataValue.text(backup_name)
            }
        )

    return restore_table_op
