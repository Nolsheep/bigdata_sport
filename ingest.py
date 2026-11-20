"""Step 1 - Extract & Load.

Downloads the Kaggle dataset `nathanlauga/nba-games` with kagglehub and loads
every CSV, untouched, into the `raw_data` schema of PostgreSQL. Each table is
named after its file: games.csv -> raw_data.games, and so on.
"""
import logging
import os
import time
from pathlib import Path

import kagglehub
import pandas as pd
from sqlalchemy import text

from db import RAW_SCHEMA, ensure_schema, get_engine, write_table

log = logging.getLogger("ingest")

DATASET = os.getenv("KAGGLE_DATASET", "nathanlauga/nba-games")


def download_dataset() -> Path:
    local = os.getenv("LOCAL_DATA_DIR")
    if local:
        log.info("Using local CSV folder %s (LOCAL_DATA_DIR)", local)
        return Path(local)
    log.info("Downloading Kaggle dataset %s ...", DATASET)
    path = kagglehub.dataset_download(DATASET)
    log.info("Path to dataset files: %s", path)
    return Path(path)


def table_name_for(csv_path: Path) -> str:
    return csv_path.stem.lower().replace("-", "_").replace(" ", "_")


def main():
    data_dir = download_dataset()
    csv_files = sorted(data_dir.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")

    engine = get_engine()
    ensure_schema(engine, RAW_SCHEMA)

    loaded_at = pd.Timestamp.now(tz="UTC")
    for csv_path in csv_files:
        t0 = time.time()
        table = table_name_for(csv_path)
        # Raw layer: keep the data as-is (only lower-case column names so they
        # are easy to query in SQL / DBeaver without quoting).
        df = pd.read_csv(csv_path, low_memory=False)
        df.columns = [c.strip().lower() for c in df.columns]
        df["_source_file"] = csv_path.name
        df["_loaded_at"] = loaded_at
        write_table(df, table, RAW_SCHEMA, engine)
        log.info(
            "Loaded %-28s -> %s.%-16s %9s rows %3s cols (%.1fs)",
            csv_path.name, RAW_SCHEMA, table, f"{len(df):,}", df.shape[1], time.time() - t0,
        )

    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = :s ORDER BY table_name"), {"s": RAW_SCHEMA}).scalars().all()
    log.info("raw_data tables: %s", ", ".join(rows))


if __name__ == "__main__":
    main()
