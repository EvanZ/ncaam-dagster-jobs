from dagster import Definitions, load_assets_from_modules, with_source_code_references

from .assets import espn
from .resources import database_resource, download_base_path
from .jobs import daily_update_job
from .schedules import daily_update_schedule

espn_assets = load_assets_from_modules([espn])

all_jobs = [daily_update_job]
all_schedules = [daily_update_schedule]

defs = Definitions(
    assets=with_source_code_references([*espn_assets]),
    resources={
        "database": database_resource,
        "storage": download_base_path
    },
    jobs=all_jobs,
    schedules=all_schedules
)
