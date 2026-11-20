"""Step 3 - Publish.

Reads the final tables from the `production` schema and writes each one to its
own worksheet in a Google Sheets spreadsheet (the Data Studio data source).

Needs a Google service-account key file (credentials.json, never committed) and
the spreadsheet ID in GOOGLE_SHEET_ID. Share the spreadsheet with the service
account's e-mail as Editor. See README.md for the step-by-step setup.
"""
import logging
import os
import time

import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe

from db import PROD_SCHEMA, get_engine

log = logging.getLogger("publish")

# Google Sheets allows 10,000,000 cells per spreadsheet. Stay well under it.
CELL_BUDGET = 9_000_000

# worksheet name -> (production table, optional SQL filter / order)
PUBLISH = {
    "season_summary": ("season_summary", "ORDER BY season"),
    "team_season_stats": ("team_season_stats", "ORDER BY season, league_rank"),
    "player_season_stats": ("player_season_stats", "WHERE games >= 10 ORDER BY season, ppg DESC"),
    "win_factors": ("win_factors", "ORDER BY season, stat"),
    "fact_games": ("fact_games", "WHERE game_type IN ('Regular Season', 'Playoffs', 'Play-In') "
                                 "ORDER BY game_date"),
}


def get_client():
    creds_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    if not os.path.exists(creds_file):
        raise FileNotFoundError(
            f"Google credentials file '{creds_file}' not found. "
            "Create a service account key (see README.md) and save it there.")
    return gspread.service_account(filename=creds_file)


def to_sheet_friendly(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = out[c].dt.strftime("%Y-%m-%d")
        elif pd.api.types.is_bool_dtype(out[c]):
            out[c] = out[c].astype(int)
    return out.astype(object).where(out.notna(), "")


def upload(spreadsheet, title, df):
    rows, cols = df.shape[0] + 1, max(df.shape[1], 1)
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
        ws.resize(rows=rows, cols=cols)
    except gspread.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=title, rows=rows, cols=cols)
    # string_escaping='full' stores text as text: otherwise Sheets turns season
    # labels such as '2003-04' into dates (2003-04-01).
    set_with_dataframe(ws, df, include_index=False, resize=True, string_escaping="full")
    return ws


def main():
    sheet_id = os.getenv("GOOGLE_SHEET_ID")
    if not sheet_id:
        raise ValueError("Set GOOGLE_SHEET_ID in .env (the ID from the spreadsheet URL).")
    engine = get_engine()

    frames = {}
    for title, (table, clause) in PUBLISH.items():
        df = pd.read_sql(f'SELECT * FROM "{PROD_SCHEMA}"."{table}" {clause}', engine)
        frames[title] = to_sheet_friendly(df)
    total_cells = sum((len(df) + 1) * df.shape[1] for df in frames.values())
    log.info("About to publish %s cells (budget %s)", f"{total_cells:,}", f"{CELL_BUDGET:,}")
    if total_cells > CELL_BUDGET:
        raise RuntimeError("Too many cells for Google Sheets - tighten the filters in PUBLISH.")

    client = get_client()
    spreadsheet = client.open_by_key(sheet_id)
    for title, df in frames.items():
        t0 = time.time()
        upload(spreadsheet, title, df)
        log.info("Published %-20s %8s rows x %2s cols (%.1fs)",
                 title, f"{len(df):,}", df.shape[1], time.time() - t0)
        time.sleep(1)  # be gentle with the Sheets API write quota

    # Drop the default empty sheet if it is still there.
    for ws in spreadsheet.worksheets():
        if ws.title in ("Sheet1", "ชีต1") and ws.title not in frames and len(spreadsheet.worksheets()) > 1:
            spreadsheet.del_worksheet(ws)
    log.info("Done: %s", spreadsheet.url)


if __name__ == "__main__":
    main()
