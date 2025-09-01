from dagster import define_asset_job

# Sink-first selection: running this will auto-run upstream assets
daily_api_files_job = define_asset_job(
    name="daily_api_files_job",
    selection="api_to_files"
)
