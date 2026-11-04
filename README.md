# NBA Games Data Pipeline (Kaggle → PostgreSQL → Google Sheets → Data Studio)

An automated, local data pipeline built on the Kaggle dataset
[**NBA games data** (nathanlauga/nba-games)](https://www.kaggle.com/datasets/nathanlauga/nba-games).
One command downloads the data, loads it into PostgreSQL (Docker), cleans and models it with
pandas, and publishes dashboard-ready tables to Google Sheets for a Looker Studio (Data Studio) report.

```
python run_pipeline.py
```

## Architecture

```
 Kaggle API                    PostgreSQL 16 (Docker)                         Google Cloud
┌──────────────┐  ingest.py  ┌──────────────────────┐  transform.py  ┌────────────────────┐  publish.py  ┌───────────────┐   ┌─────────────┐
│ 5 CSV files  │ ──────────▶ │ raw_data schema      │ ─────────────▶ │ production schema  │ ───────────▶ │ Google Sheets │ ─▶│ Data Studio │
│ (games,      │  validate   │ games, games_details,│  clean, join,  │ dim_team,          │  gspread +   │ 1 tab / table │   │ dashboard   │
│  details...) │  reconcile  │ players, ranking,    │  feature eng., │ fact_games,        │  gspread-    └───────────────┘   └─────────────┘
└──────────────┘             │ teams                │  aggregate,    │ fact_player_games, │  dataframe
                             └──────────────────────┘  reconcile     │ agg_* (5 tables)   │
                                                                     └────────────────────┘
                             audit schema: reconciliation results + pipeline_runs log
                             run_pipeline.py runs ingest → transform → publish in order and stops on the first failure
```

| Layer | Schema / place | What is in it |
|---|---|---|
| Source | `data/*.csv` | Kaggle files exactly as downloaded (git-ignored) |
| Raw | `raw_data` | One table per CSV, untouched values, plus `_source_file` and `_loaded_at` |
| Production | `production` | Cleaned star-schema tables and aggregates for the dashboard |
| Audit | `audit` | `reconciliation` (every check, every run) and `pipeline_runs` |
| Cloud staging | Google Sheet | One worksheet per published table + `pipeline_info` |

### Production tables

| Table | Grain | Published? | Used for |
|---|---|---|---|
| `dim_team` | team | yes (`teams`) | team names, arenas |
| `fact_games` | game | yes (`games`) | game-level results, home/away, margin |
| `fact_player_games` | player × game (~600k rows) | **no**, too big for Sheets | source for player/league aggregates |
| `agg_league_season` | season | yes | Q1 3-point trend, Q2 home advantage |
| `agg_team_season` | team × season | yes | Q4 dominance, point differential |
| `agg_player_season` | player × season (≥10 games) | yes | Q5 top scorers & efficiency |
| `agg_win_factors` | season × stat | yes | Q3 which stat wins games |
| `agg_starter_bench` | season × role | yes | bonus: starters vs bench |

## Business questions

1. **The 3-point revolution:** how have 3-point attempts per game, 3P% and scoring changed season by season?
2. **Home-court advantage:** how often does the home team win, and did it change (e.g. 2020-21 with no or few fans)?
3. **What wins games:** when a team wins the battle in FG%, 3P%, FT%, rebounds or assists, how often does it win the game?
4. **Dominant teams:** which teams had the best records each season, and how closely does point differential track win %?
5. **Top players:** who were the most productive and efficient scorers (PPG, true shooting %) each season?

## What each script does

**`ingest.py` (Extract & Load)**
- Downloads the dataset with the Kaggle API (skipped if the CSVs are already in `data/`; `--force` re-downloads).
- **Validation:** schema check (required columns present), empty-file check, null report per column.
- **Full load** into `raw_data` with PostgreSQL `COPY` (fast; re-runs replace the table so rows never duplicate).
- **Reconciliation** source vs target: row count check and SUM check on key numeric columns
  (e.g. `PTS`, `REB`, `AST`). Results go to `audit.reconciliation`; any mismatch stops the pipeline.

**`transform.py` (Refinery)**
- **Cleaning:** drops duplicate `GAME_ID`s and games without scores; converts dates and integer types;
  parses minutes from `"34:25"`, `"34"` and `"34.000000:25"`; removes DNP/inactive rows (no minutes) and fills
  remaining missing stats with 0.
- **Feature engineering:** season label (`2019-20`), game type from the `GAME_ID` prefix (pre-season / regular season /
  playoffs / play-in), margin, total points, month, home/away flag, team win flag, opponent, starter vs bench,
  true shooting % = PTS / (2 × (FGA + 0.44 × FTA)).
- **Joining:** `games` + `teams` (names) and `games_details` + `games` (date, season, result).
- **Aggregation:** the five `agg_*` tables above (regular season only).
- Adds indexes on the large fact table (`player_id`, `game_id`, `season, team_id`) and primary keys.
- **Reconciliation** raw vs production (row counts and point totals after cleaning), plus a quality check that player
  points add up to the team score.

**`publish.py` (Publication)**
- Reads the published tables from `production`, checks the total cell count against a 5M budget
  (Google's hard limit is 10M cells per spreadsheet), then writes each table to its own tab with
  `gspread` + `gspread-dataframe` in 5,000-row chunks. `--dry-run` only prints the sizes.

**`run_pipeline.py` (Automaton)** runs the three steps in order, logs to the console and `logs/pipeline.log`,
stops if a step fails (so stale data is never published), and records each run in `audit.pipeline_runs`.

## How to run

### 1. Prerequisites
- Docker Desktop
- Python 3.10+
- A Kaggle account and a Google account

### 2. Set up the project
```bash
git clone <this repo> && cd nba-pipeline
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env             # Windows: copy .env.example .env
```

### 3. Start PostgreSQL
```bash
docker compose up -d
docker compose ps                # STATUS should say "healthy"
```
Connect with DBeaver if you want to browse the data: host `localhost`, port `5432`, database `nba`,
user `nba`, password `nba_password` (values from `.env`).

### 4. Kaggle API key
1. kaggle.com → your profile picture → **Settings** → **API** → **Create New Token**. This downloads `kaggle.json`.
2. Move it to `~/.kaggle/kaggle.json` (Windows: `C:\Users\<you>\.kaggle\kaggle.json`).
   Or put `KAGGLE_USERNAME` and `KAGGLE_KEY` from that file into `.env`.
3. Open the dataset page once and accept its terms if Kaggle asks.

No API key? Download the zip from the dataset page by hand and unzip the five CSVs into `data/`. `ingest.py` will use them.

### 5. Google API credentials (service account)
1. Go to <https://console.cloud.google.com/> and create a project (e.g. `nba-pipeline`).
2. **APIs & Services → Library**: enable **Google Sheets API** and **Google Drive API**.
3. **APIs & Services → Credentials → Create credentials → Service account**. Give it a name, skip the optional roles, click **Done**.
4. Open the service account → **Keys → Add key → Create new key → JSON**. A `.json` file downloads.
5. Rename it to `credentials.json` and put it in the project folder. It is in `.gitignore`, **never commit it**.
6. Create an empty Google Sheet in your Drive (e.g. "NBA Dashboard Data"). Click **Share** and add the service
   account's email (`...@...iam.gserviceaccount.com`, it is inside `credentials.json` as `client_email`) as **Editor**.
7. Copy the sheet id from its URL `https://docs.google.com/spreadsheets/d/<SHEET_ID>/edit` into `GOOGLE_SHEET_ID` in `.env`.

### 6. Run everything
```bash
python run_pipeline.py                     # ingest -> transform -> publish
python run_pipeline.py --skip-publish      # only the local part (no Google needed)
python run_pipeline.py --steps transform publish
python publish.py --dry-run                # check Google Sheets size limits only
```
Each script can also run on its own: `python ingest.py`, `python transform.py`, `python publish.py`.

Check the reconciliation results in SQL:
```sql
SELECT * FROM audit.reconciliation ORDER BY run_at DESC, layer, table_name;
SELECT * FROM audit.pipeline_runs ORDER BY started_at DESC;
```

### 7. Build the Data Studio (Looker Studio) dashboard
1. Go to <https://lookerstudio.google.com> → **Create → Data source → Google Sheets**.
2. Pick the sheet and a worksheet (e.g. `league_season`), keep "Use first row as headers" on, click **Connect**.
   Repeat for `team_season`, `player_season`, `win_factors`, `starter_bench`, `games`.
3. Suggested five visualizations, one per question:

| # | Chart | Data source | Dimension | Metric(s) |
|---|---|---|---|---|
| Q1 | Combo chart (bars + line) | `league_season` | `season_label` | `fg3a_per_team_game` (bars), `avg_pts_per_team` (line) |
| Q2 | Line chart + scorecard | `league_season` | `season_label` | `home_win_pct` (format as percent) |
| Q3 | Bar chart | `win_factors` | `factor` | AVG `win_pct_when_better`, with a season filter control |
| Q4 | Scatter chart | `team_season` | `team_name` | X = `point_diff`, Y = `win_pct`, season drop-down filter |
| Q5 | Table with heatmap / bar | `player_season` | `player_name`, `team_abbr` | `ppg`, `ts_pct`, `rpg`, `apg`; filter `qualified = 1`, sort by `ppg`, season filter |

4. Add a season drop-down control and a text box showing `pipeline_info.last_refreshed_utc`.
5. **Share → Manage access → Anyone with the link → Viewer**, and copy the link into the report.

Re-running `python run_pipeline.py` refreshes the Google Sheet; the dashboard picks up the new data automatically
(Looker Studio caches for up to ~15 minutes; use **Refresh data** to force it).

## Testing without the real data
`scripts/make_sample_data.py` writes a small synthetic dataset with the same columns and quirks
(duplicate games, DNP rows, mixed minute formats) for testing:
```bash
python scripts/make_sample_data.py data_sample
DATA_DIR=data_sample python run_pipeline.py --dry-run-publish
```

## Project structure
```
nba-pipeline/
├── docker-compose.yml     PostgreSQL 16 container
├── sql/init.sql           creates raw_data, production, audit schemas
├── config.py              settings from .env, DB engine, logging
├── ingest.py              Kaggle -> raw_data (+ validation, reconciliation)
├── transform.py           raw_data -> production (+ reconciliation, indexes)
├── publish.py             production -> Google Sheets
├── run_pipeline.py        one command for the whole pipeline
├── scripts/make_sample_data.py
├── requirements.txt
├── .env.example           copy to .env
└── .gitignore             keeps credentials.json, kaggle.json, .env and data/ out of git
```
