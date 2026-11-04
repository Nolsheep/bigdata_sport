"""Step 1 - Extract & Load.

1. Extract: download the Kaggle dataset (nathanlauga/nba-games) into ./data
   with the Kaggle API. If the CSVs are already there, the download is skipped
   unless --force is given.
2. Validate: check every file has the columns we expect (schema check) and
   report nulls (completeness check).
3. Load: full-load each CSV into the raw_data schema of PostgreSQL, one table
   per file (games, games_details, players, ranking, teams).
4. Reconcile: compare the CSV with the loaded table (row count + column SUM
   checks) and store the result in audit.reconciliation.

Run on its own:  python ingest.py [--force]
"""
import argparse
import csv
import io
import sys
from datetime import datetime, timezone

import pandas as pd
from sqlalchemy import text

import config

log = config.get_logger("ingest")

# Columns each file must have (from the Kaggle data card). Extra columns are allowed.
EXPECTED_COLUMNS = {
    "games.csv": [
        "GAME_DATE_EST", "GAME_ID", "GAME_STATUS_TEXT", "HOME_TEAM_ID", "VISITOR_TEAM_ID",
        "SEASON", "PTS_home", "FG_PCT_home", "FT_PCT_home", "FG3_PCT_home", "AST_home",
        "REB_home", "PTS_away", "FG_PCT_away", "FT_PCT_away", "FG3_PCT_away", "AST_away",
        "REB_away", "HOME_TEAM_WINS",
    ],
    "games_details.csv": [
        "GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "PLAYER_ID", "PLAYER_NAME",
        "START_POSITION", "COMMENT", "MIN", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
        "OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS", "PLUS_MINUS",
    ],
    "players.csv": ["PLAYER_NAME", "TEAM_ID", "PLAYER_ID", "SEASON"],
    "ranking.csv": ["TEAM_ID", "SEASON_ID", "STANDINGSDATE", "CONFERENCE", "TEAM", "G", "W", "L", "W_PCT"],
    "teams.csv": ["TEAM_ID", "ABBREVIATION", "NICKNAME", "CITY", "ARENA", "YEARFOUNDED"],
}

# Numeric columns whose SUM must match between CSV and database (reconciliation check 3).
SUM_CHECK_COLUMNS = {
    "games.csv": ["PTS_home", "PTS_away"],
    "games_details.csv": ["PTS", "REB", "AST"],
    "players.csv": ["PLAYER_ID"],
    "ranking.csv": ["W", "L"],
    "teams.csv": ["TEAM_ID"],
}


# --------------------------------------------------------------------------- extract
def download_dataset(force: bool = False) -> None:
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    missing = [f for f in config.RAW_FILES if not (config.DATA_DIR / f).exists()]
    if not missing and not force:
        log.info("All %d CSV files already in %s, skipping download (use --force to re-download)",
                 len(config.RAW_FILES), config.DATA_DIR)
        return

    log.info("Downloading Kaggle dataset %s ...", config.KAGGLE_DATASET)
    try:
        # Imported here because the kaggle package authenticates on import and
        # fails if no kaggle.json / KAGGLE_KEY exists.
        from kaggle.api.kaggle_api_extended import KaggleApi
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            "Kaggle API is not configured. Put kaggle.json in ~/.kaggle/ (or set KAGGLE_USERNAME "
            "and KAGGLE_KEY in .env), or download the dataset by hand and unzip the CSVs into "
            f"{config.DATA_DIR}."
        ) from exc

    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(config.KAGGLE_DATASET, path=str(config.DATA_DIR), unzip=True, quiet=False)
    still_missing = [f for f in config.RAW_FILES if not (config.DATA_DIR / f).exists()]
    if still_missing:
        raise FileNotFoundError(f"Download finished but these files are missing: {still_missing}")
    log.info("Download complete")


# --------------------------------------------------------------------------- validate
def validate(file_name: str, df: pd.DataFrame) -> None:
    """Schema + completeness checks (W2: Data Validation)."""
    expected = EXPECTED_COLUMNS[file_name]
    missing = [c for c in expected if c not in df.columns]
    if missing:
        raise ValueError(f"{file_name}: schema check failed, missing columns {missing}")
    if df.empty:
        raise ValueError(f"{file_name}: file has no rows")

    null_pct = (df.isna().mean() * 100).round(1)
    noisy = null_pct[null_pct > 0].sort_values(ascending=False)
    log.info("%s: schema OK (%d columns, %d rows)", file_name, df.shape[1], len(df))
    if not noisy.empty:
        log.info("%s: columns with nulls -> %s", file_name,
                 ", ".join(f"{c} {p}%" for c, p in noisy.head(8).items()))


# --------------------------------------------------------------------------- load
def _copy_insert(table, conn, keys, data_iter):
    """pandas.to_sql method that streams rows with PostgreSQL COPY (much faster than INSERT)."""
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        buf = io.StringIO()
        csv.writer(buf).writerows(data_iter)
        buf.seek(0)
        columns = ", ".join(f'"{k}"' for k in keys)
        name = f'"{table.schema}"."{table.name}"' if table.schema else f'"{table.name}"'
        cur.copy_expert(f"COPY {name} ({columns}) FROM STDIN WITH CSV", buf)


def load_table(engine, df: pd.DataFrame, table: str, source_file: str) -> None:
    df = df.copy()
    df["_source_file"] = source_file
    df["_loaded_at"] = datetime.now(timezone.utc)
    # Full load: drop and recreate so a re-run never duplicates rows.
    df.to_sql(table, engine, schema=config.RAW_SCHEMA, if_exists="replace",
              index=False, chunksize=50_000, method=_copy_insert)


# --------------------------------------------------------------------------- reconcile
def reconcile(engine, file_name: str, table: str, df: pd.DataFrame, run_ts: datetime) -> list[dict]:
    """W4 reconciliation: row count check + aggregation (SUM) check, source vs target."""
    results = []
    with engine.connect() as conn:
        db_rows = conn.execute(text(f'SELECT COUNT(*) FROM {config.RAW_SCHEMA}."{table}"')).scalar()
        results.append(dict(check_name="row_count", source_value=float(len(df)), target_value=float(db_rows)))
        for col in SUM_CHECK_COLUMNS[file_name]:
            db_sum = conn.execute(text(f'SELECT COALESCE(SUM("{col}"), 0) FROM {config.RAW_SCHEMA}."{table}"')).scalar()
            results.append(dict(check_name=f"sum_{col}", source_value=float(df[col].sum()), target_value=float(db_sum)))

    for r in results:
        r.update(run_at=run_ts, layer="raw_data", table_name=table,
                 passed=abs(r["source_value"] - r["target_value"]) < 1e-6)
        status = "PASS" if r["passed"] else "FAIL"
        log.info("  reconcile %-22s %-16s source=%-14s target=%-14s %s", table, r["check_name"],
                 f"{r['source_value']:,.0f}", f"{r['target_value']:,.0f}", status)
    return results


def save_reconciliation(engine, results: list[dict]) -> None:
    pd.DataFrame(results).to_sql("reconciliation", engine, schema=config.AUDIT_SCHEMA,
                                 if_exists="append", index=False)


# --------------------------------------------------------------------------- main
def run(force_download: bool = False) -> None:
    run_ts = datetime.now(timezone.utc)
    download_dataset(force=force_download)

    engine = config.get_engine()
    config.ensure_schemas(engine)

    all_results = []
    for file_name, table in config.RAW_FILES.items():
        path = config.DATA_DIR / file_name
        log.info("Reading %s", path.name)
        df = pd.read_csv(path, low_memory=False)
        validate(file_name, df)
        load_table(engine, df, table, file_name)
        log.info("Loaded %s rows into %s.%s", f"{len(df):,}", config.RAW_SCHEMA, table)
        all_results += reconcile(engine, file_name, table, df, run_ts)

    save_reconciliation(engine, all_results)
    failed = [r for r in all_results if not r["passed"]]
    if failed:
        raise RuntimeError(f"Reconciliation failed for {len(failed)} check(s): "
                           + ", ".join(f"{r['table_name']}.{r['check_name']}" for r in failed))
    log.info("Ingest finished: %d tables loaded, %d reconciliation checks passed",
             len(config.RAW_FILES), len(all_results))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download the Kaggle NBA dataset and load it into raw_data")
    parser.add_argument("--force", action="store_true", help="re-download even if the CSVs exist")
    args = parser.parse_args()
    try:
        run(force_download=args.force)
    except Exception as exc:  # noqa: BLE001
        log.error("Ingest failed: %s", exc)
        sys.exit(1)
