from __future__ import annotations
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

def build_pg_engine() -> Engine:
    host = os.getenv("PG_HOST", "localhost")
    port = os.getenv("PG_PORT", "5432")
    db   = os.getenv("PG_DB", "analytics")
    user = os.getenv("PG_USER", "analytics")
    pwd  = os.getenv("PG_PASSWORD", "analytics")
    url  = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)

def ensure_schema(engine: Engine, schema: str):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
