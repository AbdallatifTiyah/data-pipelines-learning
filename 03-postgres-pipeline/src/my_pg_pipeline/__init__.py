from dagster import Definitions
from .assets_csv_to_pg import csv_raw, csv_to_postgres
from .jobs import csv_to_pg_job
from .schedules import daily_csv_to_pg

defs = Definitions(
    assets=[csv_raw, csv_to_postgres],
    jobs=[csv_to_pg_job],
    schedules=[daily_csv_to_pg],
)
