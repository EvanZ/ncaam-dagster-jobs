from dagster_duckdb import DuckDBResource
from dagster import EnvVar, ConfigurableResource
from pydantic import Field

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE")      # replaced with environment variable
)

class LocalFileStorage(ConfigurableResource):
    filepath: str = Field(description="path to local file storage")

download_base_path = LocalFileStorage(
    filepath=EnvVar("LOCAL_STORAGE_PATH")
)