from __future__ import annotations

import os
import pandas as pd
from dagster import asset, MaterializeResult, get_dagster_logger, MetadataValue
from dotenv import load_dotenv

from .resources import build_pg_engine, ensure_schema

load_dotenv()
logger = get_dagster_logger()

CSV_PATH       = os.getenv("CSV_PATH", "data/input/people.csv")
CSV_ENCODING   = os.getenv("CSV_ENCODING", "utf-8")
CSV_DELIMITER  = os.getenv("CSV_DELIMITER", ",")
PG_SCHEMA_RAW  = os.getenv("PG_SCHEMA_RAW", "raw")
PG_TABLE_CSV   = os.getenv("PG_TABLE_CSV", "people")

@asset(description="Read a local CSV into a DataFrame (source for raw load).")
def csv_raw() -> pd.DataFrame:
    df = pd.read_csv(CSV_PATH, encoding=CSV_ENCODING, sep=CSV_DELIMITER)
    # Add ingestion metadata (good habit for warehousing)
    df["_ingested_at_utc"] = pd.Timestamp.utcnow()
    df["_source"] = f"csv:{os.path.basename(CSV_PATH)}"
    return df

@asset(description="Create raw schema if needed and append CSV rows to raw.people.")
def csv_to_postgres(csv_raw: pd.DataFrame) -> MaterializeResult:
    engine = build_pg_engine()
    ensure_schema(engine, PG_SCHEMA_RAW)

    # Append pattern (safe for raw zone). Dedup/upsert is handled later by dbt.
    with engine.begin() as conn:
        csv_raw.to_sql(
            name=PG_TABLE_CSV,
            con=conn.connection,
            schema=PG_SCHEMA_RAW,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=10_000,
        )

    return MaterializeResult(
        metadata={
            "rows_appended": len(csv_raw),
            "schema": PG_SCHEMA_RAW,
            "table": PG_TABLE_CSV,
            "columns": list(csv_raw.columns),
            "csv_path": MetadataValue.path(CSV_PATH),
        }
    )
