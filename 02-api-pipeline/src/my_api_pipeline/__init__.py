from dagster import Definitions
from .assets_api import api_raw, api_normalized, api_to_files
from .jobs import daily_api_files_job
from .schedules import daily_api_schedule

defs = Definitions(
    assets=[api_raw, api_normalized, api_to_files],
    jobs=[daily_api_files_job],
    schedules=[daily_api_schedule],
)
