from __future__ import annotations

import os
import json
import pandas as pd

from dagster import (
    asset, MaterializeResult, get_dagster_logger,
    DailyPartitionsDefinition, MetadataValue
)
from dotenv import load_dotenv

from .resources import HttpClient

load_dotenv()
logger = get_dagster_logger()

API_URL            = os.getenv("API_URL", "https://jsonplaceholder.typicode.com/posts")
RAW_SNAPSHOT_DIR   = os.getenv("RAW_SNAPSHOT_DIR", "data/raw_snapshots")
PROCESSED_DIR      = os.getenv("PROCESSED_DIR", "data/processed")
PARTITIONS_START   = os.getenv("PARTITIONS_START", "2025-01-01")

daily_partitions = DailyPartitionsDefinition(start_date=PARTITIONS_START)

@asset(
    partitions_def=daily_partitions,
    description="Fetch raw JSON from the API and snapshot to disk per partition date."
)
def api_raw(context) -> list[dict]:
    client = HttpClient()
    data = client.get_json(API_URL)

    part = context.partition_key
    os.makedirs(RAW_SNAPSHOT_DIR, exist_ok=True)
    raw_path = os.path.join(RAW_SNAPSHOT_DIR, f"{part}.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    items = len(data) if isinstance(data, list) else 1
    logger.info(f"Saved raw snapshot: {raw_path} (items={items})")

    return MaterializeResult(
        value=data,
        metadata={
            "api_url": API_URL,
            "items": items,
            "snapshot_path": MetadataValue.path(raw_path),
            "partition": part,
        },
    )

@asset(
    partitions_def=daily_partitions,
    description="Normalize raw JSON to a tabular DataFrame (adds load metadata)."
)
def api_normalized(api_raw: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(api_raw if isinstance(api_raw, list) else [api_raw])
    # Add load metadata helpful for warehousing later
    df["_ingested_at_utc"] = pd.Timestamp.utcnow()
    df["_source"] = "api:jsonplaceholder/posts"
    return df

@asset(
    partitions_def=daily_partitions,
    description="Write normalized data to CSV and Parquet (per partition)."
)
def api_to_files(context, api_normalized: pd.DataFrame) -> MaterializeResult:
    part = context.partition_key
    out_dir = os.path.join(PROCESSED_DIR, part)
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, "posts.csv")
    pq_path  = os.path.join(out_dir, "posts.parquet")

    api_normalized.to_csv(csv_path, index=False)
    api_normalized.to_parquet(pq_path, index=False)

    return MaterializeResult(
        metadata={
            "rows": len(api_normalized),
            "columns": list(api_normalized.columns),
            "csv_path": MetadataValue.path(csv_path),
            "parquet_path": MetadataValue.path(pq_path),
            "partition": part,
        }
    )
