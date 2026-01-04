import warnings

from dagster import Definitions, load_assets_from_modules, with_source_code_references
from dagster._utils.warnings import BetaWarning

# Silence noisy beta warnings from Dagster helpers (e.g., with_source_code_references).
warnings.filterwarnings("ignore", category=BetaWarning)

from .assets import espn, women, models, web
from .resources import database_resource, download_base_path, jinja_templates_path
from .jobs import daily_update_job, daily_update_job_women, cleanup_job, models_update_job, models_update_job_women, \
    top_lines_job, top_lines_job_women, season_report_job, season_report_job_women, seasonal_update_job, \
    cleanup_models_job, backup_models_job, restore_models_job, daily_backfill_job, daily_backfill_job_women, \
    web_export_job, web_export_job_women
from .schedules import daily_update_schedule

espn_assets = load_assets_from_modules([espn, women])
model_assets = load_assets_from_modules([models])
web_assets = load_assets_from_modules([web])

all_jobs = [daily_update_job, daily_update_job_women, models_update_job, models_update_job_women, 
            top_lines_job, cleanup_job, season_report_job, top_lines_job_women, season_report_job_women, seasonal_update_job,
            cleanup_models_job, backup_models_job, restore_models_job, daily_backfill_job, daily_backfill_job_women,
            web_export_job, web_export_job_women]
all_schedules = [daily_update_schedule]

defs = Definitions(
    assets=with_source_code_references([*espn_assets, *model_assets, *web_assets]),
    resources={
        "database": database_resource,
        "storage": download_base_path,
        "templates": jinja_templates_path
    },
    jobs=all_jobs,
    schedules=all_schedules
)
