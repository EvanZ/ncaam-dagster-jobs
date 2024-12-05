from dagster import Definitions, load_assets_from_modules, with_source_code_references

from .assets import espn, women, models
from .resources import database_resource, download_base_path, jinja_templates_path
from .jobs import daily_update_job, daily_update_job_women, cleanup_job, models_update_job, models_update_job_women, \
    top_lines_job, top_lines_job_women, season_report_job, season_report_job_women
from .schedules import daily_update_schedule

espn_assets = load_assets_from_modules([espn, women])
model_assets = load_assets_from_modules([models])

all_jobs = [daily_update_job, daily_update_job_women, models_update_job, models_update_job_women, 
            top_lines_job, cleanup_job, season_report_job, top_lines_job_women, season_report_job_women]
all_schedules = [daily_update_schedule]

defs = Definitions(
    assets=with_source_code_references([*espn_assets, *model_assets]),
    resources={
        "database": database_resource,
        "storage": download_base_path,
        "templates": jinja_templates_path
    },
    jobs=all_jobs,
    schedules=all_schedules
)
