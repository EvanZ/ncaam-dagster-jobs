from dagster import build_schedule_from_partitioned_job
from ..jobs import daily_update_job

# Use the partitioned job's partition definition so each tick has a partition key.
daily_update_schedule = build_schedule_from_partitioned_job(
    daily_update_job,
    minute_of_hour=0,
    hour_of_day=0,  # daily at midnight
)
