"""Shared helpers: config from .env and a SQLAlchemy engine for PostgreSQL."""
import csv
import io
import logging
import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)

RAW_SCHEMA = "raw_data"
PROD_SCHEMA = "production"


def get_engine():
    user = os.getenv("POSTGRES_USER", "nba")
    password = os.getenv("POSTGRES_PASSWORD", "nba_password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nba_dw")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_schema(engine, schema):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def copy_insert(table, conn, keys, data_iter):
    """Fast `to_sql` method that streams rows through PostgreSQL COPY.

    Much faster than INSERTs for big tables like games_details (~670k rows).
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        buf = io.StringIO()
        csv.writer(buf).writerows(data_iter)
        buf.seek(0)
        columns = ", ".join(f'"{k}"' for k in keys)
        name = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        cur.copy_expert(f"COPY {name} ({columns}) FROM STDIN WITH CSV", buf)


def write_table(df, name, schema, engine, chunksize=50_000):
    """Replace `schema.name` with the contents of `df`."""
    df.to_sql(
        name,
        engine,
        schema=schema,
        if_exists="replace",
        index=False,
        method=copy_insert,
        chunksize=chunksize,
    )
