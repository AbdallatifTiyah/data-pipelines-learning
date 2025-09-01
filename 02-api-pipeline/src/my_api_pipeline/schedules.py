import os
from dagster import ScheduleDefinition
from .jobs import daily_api_files_job

tz = os.getenv("TIMEZONE", "Asia/Hebron")

# Run every day at 02:00 local time
daily_api_schedule = ScheduleDefinition(
    job=daily_api_files_job,
    cron_schedule="0 2 * * *",
    execution_timezone=tz,
)
