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

class JinjaTemplates(ConfigurableResource):
    searchpath: str = Field(description="path to search for jinja templates")

jinja_templates_path= JinjaTemplates(
    searchpath=EnvVar("JINJA_TEMPLATES_PATH")
)