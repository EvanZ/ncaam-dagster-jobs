from dagster import ScheduleDefinition
from ..jobs import daily_update_job

daily_update_schedule = ScheduleDefinition(
    job=daily_update_job,
    cron_schedule="0 0 * * *",  # This runs daily at midnight
)
