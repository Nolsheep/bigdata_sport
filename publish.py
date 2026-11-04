"""Step 3 - Publish production tables to Google Sheets (cloud staging for Data Studio).

Reads the dashboard tables from the production schema and writes each one to its
own worksheet (tab) in a Google Sheet, using a Google service account.

Google Sheets allows at most 10,000,000 cells per spreadsheet, so the big
player-by-game fact table (~600k rows) stays in PostgreSQL and only the
aggregated tables are published. The script checks the cell budget before it
writes anything.

Run on its own:  python publish.py [--dry-run]
"""
import argparse
import sys
import time
from datetime import datetime, timezone

import pandas as pd

import config

log = config.get_logger("publish")

# production table -> worksheet name. Order = tab order in the sheet.
PUBLISH_TABLES = {
    "agg_league_season": "league_season",
    "agg_team_season": "team_season",
    "agg_player_season": "player_season",
    "agg_win_factors": "win_factors",
    "agg_starter_bench": "starter_bench",
    "fact_games": "games",
    "dim_team": "teams",
}
SHEETS_CELL_LIMIT = 10_000_000
CELL_BUDGET = 5_000_000          # stay well under the limit so the sheet stays fast
ROWS_PER_REQUEST = 5_000         # write big tables in chunks to avoid API payload limits


def load_tables(engine) -> dict[str, pd.DataFrame]:
    frames = {}
    for table, sheet in PUBLISH_TABLES.items():
        frames[sheet] = pd.read_sql_table(table, engine, schema=config.PROD_SCHEMA)
    return frames


def check_cell_budget(frames: dict[str, pd.DataFrame]) -> int:
    total = 0
    for name, df in frames.items():
        cells = (len(df) + 1) * df.shape[1]
        total += cells
        log.info("  %-15s %8s rows x %2d cols = %10s cells", name, f"{len(df):,}", df.shape[1], f"{cells:,}")
    log.info("Total %s cells (budget %s, Google limit %s)", f"{total:,}", f"{CELL_BUDGET:,}", f"{SHEETS_CELL_LIMIT:,}")
    if total > CELL_BUDGET:
        raise ValueError("Too many cells for Google Sheets: aggregate further or drop a table from PUBLISH_TABLES")
    return total


def prepare(df: pd.DataFrame) -> pd.DataFrame:
    """Make values JSON/Sheets friendly: dates -> ISO text, NaN -> blank."""
    out = df.copy()
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = pd.to_datetime(out[col]).dt.strftime("%Y-%m-%d")
        elif out[col].dtype == object:
            out[col] = out[col].map(lambda v: v.isoformat() if hasattr(v, "isoformat") else v)
        elif pd.api.types.is_bool_dtype(out[col]):
            out[col] = out[col].astype(int)
    return out.astype(object).where(out.notna(), "")


def open_sheet():
    import gspread

    if not config.GOOGLE_CREDENTIALS_FILE.exists():
        raise FileNotFoundError(
            f"{config.GOOGLE_CREDENTIALS_FILE.name} not found. Create a service account key in Google Cloud "
            "and save it in the project folder (see README, 'Google API credentials').")
    if not config.GOOGLE_SHEET_ID:
        raise ValueError("GOOGLE_SHEET_ID is empty in .env")
    client = gspread.service_account(filename=str(config.GOOGLE_CREDENTIALS_FILE))
    return client.open_by_key(config.GOOGLE_SHEET_ID)


def write_worksheet(spreadsheet, name: str, df: pd.DataFrame) -> None:
    import gspread
    from gspread_dataframe import set_with_dataframe

    rows, cols = len(df) + 1, max(df.shape[1], 1)
    try:
        ws = spreadsheet.worksheet(name)
        ws.clear()
        ws.resize(rows=rows, cols=cols)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=name, rows=rows, cols=cols)

    for start in range(0, max(len(df), 1), ROWS_PER_REQUEST):
        chunk = df.iloc[start:start + ROWS_PER_REQUEST]
        first = start == 0
        set_with_dataframe(ws, chunk, row=1 if first else start + 2, col=1,
                           include_index=False, include_column_header=first, resize=False)
        time.sleep(1)  # be gentle with the Sheets API write quota (60 requests/min/user)
    ws.freeze(rows=1)
    log.info("Published %-15s %8s rows", name, f"{len(df):,}")


def run(dry_run: bool = False) -> None:
    engine = config.get_engine()
    frames = {name: prepare(df) for name, df in load_tables(engine).items()}
    check_cell_budget(frames)

    # A small tab the dashboard can show as "data last refreshed".
    frames["pipeline_info"] = pd.DataFrame([{
        "last_refreshed_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "source": f"kaggle.com/datasets/{config.KAGGLE_DATASET}",
        "seasons": f"{frames['league_season']['season_label'].iloc[0]} to {frames['league_season']['season_label'].iloc[-1]}",
        "games": len(frames["games"]),
    }])

    if dry_run:
        log.info("Dry run: nothing written to Google Sheets")
        return

    spreadsheet = open_sheet()
    log.info("Writing to Google Sheet '%s'", spreadsheet.title)
    for name, df in frames.items():
        write_worksheet(spreadsheet, name, df)

    # Remove the default empty "Sheet1" if it is still there.
    for ws in spreadsheet.worksheets():
        if ws.title not in frames and ws.title.lower() in {"sheet1", "แผ่น1", "sheet 1"}:
            spreadsheet.del_worksheet(ws)
    log.info("Publish finished: %s", spreadsheet.url)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish production tables to Google Sheets")
    parser.add_argument("--dry-run", action="store_true", help="check sizes only, do not write")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception as exc:  # noqa: BLE001
        log.error("Publish failed: %s", exc)
        sys.exit(1)
