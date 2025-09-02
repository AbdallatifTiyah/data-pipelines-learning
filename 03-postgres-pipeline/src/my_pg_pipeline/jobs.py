from dagster import define_asset_job

csv_to_pg_job = define_asset_job(
    name="csv_to_pg_job",
    selection="csv_to_postgres"
)
