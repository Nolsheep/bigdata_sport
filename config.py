"""Shared settings and helpers for every pipeline step.

All settings come from environment variables (loaded from .env), so the same
code runs on any machine without edits.
"""
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")

DATA_DIR = PROJECT_DIR / os.getenv("DATA_DIR", "data")
KAGGLE_DATASET = os.getenv("KAGGLE_DATASET", "nathanlauga/nba-games")

RAW_SCHEMA = "raw_data"
PROD_SCHEMA = "production"
AUDIT_SCHEMA = "audit"

# The five CSV files in the Kaggle dataset -> raw_data table names.
RAW_FILES = {
    "games.csv": "games",
    "games_details.csv": "games_details",
    "players.csv": "players",
    "ranking.csv": "ranking",
    "teams.csv": "teams",
}

GOOGLE_CREDENTIALS_FILE = PROJECT_DIR / os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "")


def get_engine() -> Engine:
    user = os.getenv("POSTGRES_USER", "nba")
    password = os.getenv("POSTGRES_PASSWORD", "nba_password")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nba")
    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url, pool_pre_ping=True)


def ensure_schemas(engine: Engine) -> None:
    """Create schemas if docker's init.sql did not run (e.g. an existing volume)."""
    with engine.begin() as conn:
        for schema in (RAW_SCHEMA, PROD_SCHEMA, AUDIT_SCHEMA):
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))


def get_logger(name: str) -> logging.Logger:
    log_dir = PROJECT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fmt = logging.Formatter("%(asctime)s | %(name)-9s | %(levelname)-7s | %(message)s", "%H:%M:%S")
        console = logging.StreamHandler()
        console.setFormatter(fmt)
        logfile = logging.FileHandler(log_dir / "pipeline.log", encoding="utf-8")
        logfile.setFormatter(fmt)
        logger.addHandler(console)
        logger.addHandler(logfile)
        logger.propagate = False
    return logger
