import os
from dagster import ScheduleDefinition
from .jobs import csv_to_pg_job

tz = os.getenv("TIMEZONE", "Asia/Hebron")

# Example: run daily at 01:30 local time
daily_csv_to_pg = ScheduleDefinition(
    job=csv_to_pg_job,
    cron_schedule="30 1 * * *",
    execution_timezone=tz,
)
