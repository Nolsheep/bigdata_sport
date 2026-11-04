"""Step 2 - Transform (the Refinery).

Reads the raw_data tables into pandas, then:
  * Cleaning        - de-duplicate, fix data types, parse "MM:SS" minutes, handle
                      missing values and players who did not play (DNP).
  * Transformation  - feature engineering: season label, game type, margin,
                      home/away + win flags, true shooting %, starter flag.
  * Joining         - games + teams + games_details.
  * Aggregation     - season, team-season, player-season and win-factor summaries.
Writes the results to the production schema, adds indexes, and reconciles
raw vs production (row counts and point totals).

Run on its own:  python transform.py
"""
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import text

import config
from ingest import _copy_insert

log = config.get_logger("transform")

# First digit of GAME_ID tells the game type (NBA stats API convention).
GAME_TYPES = {"1": "Pre Season", "2": "Regular Season", "3": "All-Star", "4": "Playoffs", "5": "Play-In"}
STAT_COLS = ["FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA", "OREB", "DREB", "REB",
             "AST", "STL", "BLK", "TO", "PF", "PTS", "PLUS_MINUS"]


# --------------------------------------------------------------------------- helpers
def read_raw(engine, table: str) -> pd.DataFrame:
    df = pd.read_sql_table(table, engine, schema=config.RAW_SCHEMA)
    return df.drop(columns=[c for c in df.columns if c.startswith("_")])


def season_label(start_year: pd.Series) -> pd.Series:
    """2019 -> '2019-20'."""
    y = start_year.astype(int)
    return y.astype(str) + "-" + ((y + 1) % 100).astype(str).str.zfill(2)


def parse_minutes(value) -> float:
    """Minutes come as '34:25', '34', '34.000000:25' or NaN. Return decimal minutes."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return np.nan
    s = str(value).strip()
    if not s:
        return np.nan
    try:
        if ":" in s:
            m, sec = s.split(":", 1)
            return float(m) + float(sec) / 60
        return float(s)
    except ValueError:
        return np.nan


def safe_div(a, b):
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        out = np.where(b > 0, a / b, np.nan)
    return out


def write(engine, df: pd.DataFrame, table: str) -> None:
    df.to_sql(table, engine, schema=config.PROD_SCHEMA, if_exists="replace",
              index=False, chunksize=50_000, method=_copy_insert)
    log.info("Wrote %-26s %9s rows x %2d cols", f"{config.PROD_SCHEMA}.{table}", f"{len(df):,}", df.shape[1])


# --------------------------------------------------------------------------- cleaning
def clean_teams(teams: pd.DataFrame) -> pd.DataFrame:
    t = teams.drop_duplicates("TEAM_ID").copy()
    t["TEAM_NAME"] = t["CITY"].str.strip() + " " + t["NICKNAME"].str.strip()
    t["ARENACAPACITY"] = pd.to_numeric(t.get("ARENACAPACITY"), errors="coerce")
    cols = {"TEAM_ID": "team_id", "ABBREVIATION": "team_abbr", "TEAM_NAME": "team_name",
            "NICKNAME": "nickname", "CITY": "city", "ARENA": "arena",
            "ARENACAPACITY": "arena_capacity", "YEARFOUNDED": "year_founded"}
    return t[list(cols)].rename(columns=cols)


def clean_games(games: pd.DataFrame, dim_team: pd.DataFrame) -> pd.DataFrame:
    g = games.copy()
    n0 = len(g)
    g = g.drop_duplicates("GAME_ID")
    dupes = n0 - len(g)
    g = g.dropna(subset=["PTS_home", "PTS_away"])          # games with no box score
    log.info("games: %s raw -> %s clean (%d duplicate GAME_IDs, %d without scores removed)",
             f"{n0:,}", f"{len(g):,}", dupes, n0 - dupes - len(g))

    g["game_date"] = pd.to_datetime(g["GAME_DATE_EST"]).dt.date
    g["season"] = g["SEASON"].astype(int)
    g["season_label"] = season_label(g["season"])
    g["game_type"] = g["GAME_ID"].astype(str).str[0].map(GAME_TYPES).fillna("Other")
    for c in ["PTS_home", "PTS_away", "AST_home", "AST_away", "REB_home", "REB_away"]:
        g[c] = g[c].astype(int)
    g["home_win"] = g["HOME_TEAM_WINS"].astype(int)
    g["margin"] = g["PTS_home"] - g["PTS_away"]
    g["total_pts"] = g["PTS_home"] + g["PTS_away"]
    g["month"] = pd.to_datetime(g["game_date"]).dt.strftime("%Y-%m")

    names = dim_team.set_index("team_id")
    g["home_team"] = g["HOME_TEAM_ID"].map(names["team_name"])
    g["away_team"] = g["VISITOR_TEAM_ID"].map(names["team_name"])
    g["home_abbr"] = g["HOME_TEAM_ID"].map(names["team_abbr"])
    g["away_abbr"] = g["VISITOR_TEAM_ID"].map(names["team_abbr"])
    g["winner"] = np.where(g["home_win"] == 1, g["home_team"], g["away_team"])

    cols = {
        "GAME_ID": "game_id", "game_date": "game_date", "month": "month", "season": "season",
        "season_label": "season_label", "game_type": "game_type",
        "HOME_TEAM_ID": "home_team_id", "home_team": "home_team", "home_abbr": "home_abbr",
        "VISITOR_TEAM_ID": "away_team_id", "away_team": "away_team", "away_abbr": "away_abbr",
        "PTS_home": "pts_home", "PTS_away": "pts_away", "total_pts": "total_pts", "margin": "margin",
        "FG_PCT_home": "fg_pct_home", "FG_PCT_away": "fg_pct_away",
        "FG3_PCT_home": "fg3_pct_home", "FG3_PCT_away": "fg3_pct_away",
        "FT_PCT_home": "ft_pct_home", "FT_PCT_away": "ft_pct_away",
        "AST_home": "ast_home", "AST_away": "ast_away", "REB_home": "reb_home", "REB_away": "reb_away",
        "home_win": "home_win", "winner": "winner",
    }
    return g[list(cols)].rename(columns=cols).sort_values(["game_date", "game_id"]).reset_index(drop=True)


def clean_player_games(details: pd.DataFrame, fact_games: pd.DataFrame) -> pd.DataFrame:
    d = details.copy()
    n0 = len(d)
    d = d.drop_duplicates(["GAME_ID", "PLAYER_ID"])
    dupes = n0 - len(d)

    d["minutes"] = d["MIN"].map(parse_minutes)
    # A player "played" if he logged minutes. DNP / inactive rows have a COMMENT and no stats.
    played = d["minutes"].fillna(0) > 0
    dnp = (~played).sum()
    d = d[played].copy()
    d[STAT_COLS] = d[STAT_COLS].fillna(0).astype(int)
    log.info("games_details: %s raw -> %s player-games (%d duplicates, %s DNP/inactive rows removed)",
             f"{n0:,}", f"{len(d):,}", dupes, f"{dnp:,}")

    d["is_starter"] = d["START_POSITION"].notna() & (d["START_POSITION"].astype(str).str.strip() != "")
    d["role"] = np.where(d["is_starter"], "Starter", "Bench")
    d["start_position"] = d["START_POSITION"].fillna("Bench")

    # Join game context (date, season, type, home/away, result).
    ctx = fact_games[["game_id", "game_date", "season", "season_label", "game_type",
                      "home_team_id", "away_team_id", "home_win"]]
    d = d.merge(ctx, left_on="GAME_ID", right_on="game_id", how="inner")
    d["is_home"] = (d["TEAM_ID"] == d["home_team_id"]).astype(int)
    d["team_win"] = np.where(d["is_home"] == 1, d["home_win"], 1 - d["home_win"])
    d["opponent_id"] = np.where(d["is_home"] == 1, d["away_team_id"], d["home_team_id"])

    # True shooting % = PTS / (2 * (FGA + 0.44 * FTA))
    d["ts_pct"] = safe_div(d["PTS"], 2 * (d["FGA"] + 0.44 * d["FTA"]))

    cols = {
        "game_id": "game_id", "game_date": "game_date", "season": "season", "season_label": "season_label",
        "game_type": "game_type", "TEAM_ID": "team_id", "TEAM_ABBREVIATION": "team_abbr",
        "opponent_id": "opponent_id", "is_home": "is_home", "team_win": "team_win",
        "PLAYER_ID": "player_id", "PLAYER_NAME": "player_name", "start_position": "start_position",
        "role": "role", "minutes": "minutes",
        **{c: c.lower() for c in STAT_COLS}, "ts_pct": "ts_pct",
    }
    out = d[list(cols)].rename(columns=cols).rename(columns={"to": "tov"})
    out["minutes"] = out["minutes"].round(2)
    return out


# --------------------------------------------------------------------------- aggregation
def team_games(fact_games: pd.DataFrame) -> pd.DataFrame:
    """One row per team per game (long format) - easier for team-level aggregates."""
    base = ["game_id", "game_date", "season", "season_label", "game_type"]
    home = fact_games[base + ["home_team_id", "home_team", "pts_home", "pts_away", "fg_pct_home",
                              "fg3_pct_home", "ast_home", "reb_home", "home_win"]].copy()
    home.columns = base + ["team_id", "team_name", "pts", "opp_pts", "fg_pct", "fg3_pct", "ast", "reb", "win"]
    home["is_home"] = 1
    away = fact_games[base + ["away_team_id", "away_team", "pts_away", "pts_home", "fg_pct_away",
                              "fg3_pct_away", "ast_away", "reb_away", "home_win"]].copy()
    away.columns = base + ["team_id", "team_name", "pts", "opp_pts", "fg_pct", "fg3_pct", "ast", "reb", "win"]
    away["win"] = 1 - away["win"]
    away["is_home"] = 0
    return pd.concat([home, away], ignore_index=True)


def agg_league_season(fact_games, player_games) -> pd.DataFrame:
    """Q1 (3-point revolution) + Q2 (home-court advantage), regular season only."""
    rs = fact_games[fact_games["game_type"] == "Regular Season"]
    g = rs.groupby(["season", "season_label"]).agg(
        games=("game_id", "count"),
        avg_total_pts=("total_pts", "mean"),
        home_win_pct=("home_win", "mean"),
        avg_home_margin=("margin", "mean"),
    )
    pg = player_games[player_games["game_type"] == "Regular Season"]
    shots = pg.groupby("season").agg(fg3m=("fg3m", "sum"), fg3a=("fg3a", "sum"), fga=("fga", "sum"),
                                     fgm=("fgm", "sum"), fta=("fta", "sum"), pts=("pts", "sum"),
                                     team_games=("game_id", lambda s: s.nunique() * 2))
    shots["fg3a_per_team_game"] = shots["fg3a"] / shots["team_games"]
    shots["fg3m_per_team_game"] = shots["fg3m"] / shots["team_games"]
    shots["fg3_pct"] = safe_div(shots["fg3m"], shots["fg3a"])
    shots["fg_pct"] = safe_div(shots["fgm"], shots["fga"])
    shots["fg3a_rate"] = safe_div(shots["fg3a"], shots["fga"])       # share of shots that are 3s
    shots["pts_from_3_share"] = safe_div(shots["fg3m"] * 3, shots["pts"])
    shots["ts_pct"] = safe_div(shots["pts"], 2 * (shots["fga"] + 0.44 * shots["fta"]))
    out = g.reset_index().merge(
        shots[["fg3a_per_team_game", "fg3m_per_team_game", "fg3_pct", "fg_pct", "fg3a_rate",
               "pts_from_3_share", "ts_pct"]].reset_index(), on="season", how="left")
    out["avg_pts_per_team"] = out["avg_total_pts"] / 2
    return out.round(4).sort_values("season")


def agg_team_season(tg: pd.DataFrame) -> pd.DataFrame:
    """Q4 (team dominance, point differential vs win %) + Q2 at team level."""
    rs = tg[tg["game_type"] == "Regular Season"].copy()
    rs["home_w"] = rs["win"].where(rs["is_home"] == 1)
    rs["away_w"] = rs["win"].where(rs["is_home"] == 0)
    out = rs.groupby(["season", "season_label", "team_id", "team_name"]).agg(
        games=("game_id", "count"), wins=("win", "sum"),
        ppg=("pts", "mean"), opp_ppg=("opp_pts", "mean"),
        fg_pct=("fg_pct", "mean"), fg3_pct=("fg3_pct", "mean"),
        apg=("ast", "mean"), rpg=("reb", "mean"),
        home_win_pct=("home_w", "mean"), away_win_pct=("away_w", "mean"),
    ).reset_index()
    out["losses"] = out["games"] - out["wins"]
    out["win_pct"] = out["wins"] / out["games"]
    out["point_diff"] = out["ppg"] - out["opp_ppg"]
    out["home_edge"] = out["home_win_pct"] - out["away_win_pct"]
    out["rank_in_season"] = out.groupby("season")["win_pct"].rank(ascending=False, method="min").astype(int)
    return out.round(4).sort_values(["season", "rank_in_season"])


def agg_player_season(pg: pd.DataFrame) -> pd.DataFrame:
    """Q5 (most productive and efficient scorers), regular season only."""
    rs = pg[pg["game_type"] == "Regular Season"]
    g = rs.groupby(["season", "season_label", "player_id", "player_name"])
    out = g.agg(games=("game_id", "nunique"), starts=("role", lambda s: (s == "Starter").sum()),
                minutes=("minutes", "sum"), pts=("pts", "sum"), reb=("reb", "sum"), ast=("ast", "sum"),
                stl=("stl", "sum"), blk=("blk", "sum"), tov=("tov", "sum"),
                fgm=("fgm", "sum"), fga=("fga", "sum"), fg3m=("fg3m", "sum"), fg3a=("fg3a", "sum"),
                ftm=("ftm", "sum"), fta=("fta", "sum"), plus_minus=("plus_minus", "sum")).reset_index()
    # Team = the team he played the most games for that season.
    main_team = (rs.groupby(["season", "player_id", "team_abbr"]).size().rename("n").reset_index()
                 .sort_values("n").drop_duplicates(["season", "player_id"], keep="last"))
    out = out.merge(main_team[["season", "player_id", "team_abbr"]], on=["season", "player_id"], how="left")

    per_game = {"minutes": "mpg", "pts": "ppg", "reb": "rpg", "ast": "apg",
                "stl": "spg", "blk": "bpg", "tov": "topg"}
    for stat, col in per_game.items():
        out[col] = out[stat] / out["games"]
    out["fg_pct"] = safe_div(out["fgm"], out["fga"])
    out["fg3_pct"] = safe_div(out["fg3m"], out["fg3a"])
    out["ft_pct"] = safe_div(out["ftm"], out["fta"])
    out["ts_pct"] = safe_div(out["pts"], 2 * (out["fga"] + 0.44 * out["fta"]))
    # Simple "box-score contribution" per game, a readable productivity score.
    out["pra_pg"] = out["ppg"] + out["rpg"] + out["apg"]
    # Qualified = played in at least 58 games (~70% of 82, the NBA's stat-title rule of thumb).
    out["qualified"] = (out["games"] >= 58).astype(int)
    out["ppg_rank"] = out.groupby("season")["ppg"].rank(ascending=False, method="min").astype(int)
    keep = ["season", "season_label", "player_id", "player_name", "team_abbr", "games", "starts",
            "mpg", "ppg", "rpg", "apg", "spg", "bpg", "topg", "pra_pg", "fg_pct", "fg3_pct",
            "ft_pct", "ts_pct", "fg3a", "plus_minus", "qualified", "ppg_rank"]
    # Keep rotation players only (>= 10 games) so the sheet stays small and meaningful.
    return out.loc[out["games"] >= 10, keep].round(4).sort_values(["season", "ppg_rank"])


def agg_win_factors(fact_games: pd.DataFrame) -> pd.DataFrame:
    """Q3: when a team wins a stat battle (FG%, 3P%, REB, AST, FT%), how often does it win the game?"""
    rs = fact_games[fact_games["game_type"] == "Regular Season"].dropna(
        subset=["fg_pct_home", "fg3_pct_home", "ft_pct_home"])
    factors = {"Field goal %": ("fg_pct_home", "fg_pct_away"), "3-point %": ("fg3_pct_home", "fg3_pct_away"),
               "Free throw %": ("ft_pct_home", "ft_pct_away"), "Rebounds": ("reb_home", "reb_away"),
               "Assists": ("ast_home", "ast_away")}
    rows = []
    for (season, label), grp in rs.groupby(["season", "season_label"]):
        for factor, (h, a) in factors.items():
            decided = grp[grp[h] != grp[a]]                     # ignore ties on the stat
            home_better = decided[h] > decided[a]
            better_won = np.where(home_better, decided["home_win"] == 1, decided["home_win"] == 0)
            rows.append(dict(season=season, season_label=label, factor=factor,
                             games=len(decided), win_pct_when_better=float(np.mean(better_won)) if len(decided) else np.nan))
    return pd.DataFrame(rows).round(4)


def agg_starter_bench(pg: pd.DataFrame) -> pd.DataFrame:
    """Bonus: share of points/minutes from starters vs bench per season."""
    rs = pg[pg["game_type"] == "Regular Season"]
    out = rs.groupby(["season", "season_label", "role"]).agg(pts=("pts", "sum"), minutes=("minutes", "sum"),
                                                             player_games=("player_id", "count")).reset_index()
    tot = out.groupby("season")[["pts", "minutes"]].transform("sum")
    out["pts_share"] = out["pts"] / tot["pts"]
    out["minutes_share"] = out["minutes"] / tot["minutes"]
    return out.round(4)


# --------------------------------------------------------------------------- indexes + reconciliation
def create_indexes(engine) -> None:
    """W3: index the large fact tables on the columns used to filter/join."""
    stmts = [
        "CREATE INDEX IF NOT EXISTS ix_fpg_player ON production.fact_player_games (player_id)",
        "CREATE INDEX IF NOT EXISTS ix_fpg_game ON production.fact_player_games (game_id)",
        "CREATE INDEX IF NOT EXISTS ix_fpg_season_team ON production.fact_player_games (season, team_id)",
        "CREATE INDEX IF NOT EXISTS ix_fg_season ON production.fact_games (season, game_type)",
        "ALTER TABLE production.fact_games ADD PRIMARY KEY (game_id)",
        "ALTER TABLE production.dim_team ADD PRIMARY KEY (team_id)",
    ]
    with engine.begin() as conn:
        for s in stmts:
            conn.execute(text(s))
    log.info("Indexes and primary keys created")


def reconcile(engine, raw_games, fact_games, raw_details, fact_pg) -> None:
    run_ts = datetime.now(timezone.utc)
    raw_g = raw_games.drop_duplicates("GAME_ID").dropna(subset=["PTS_home", "PTS_away"])
    raw_d = raw_details.drop_duplicates(["GAME_ID", "PLAYER_ID"])
    raw_d = raw_d[raw_d["MIN"].map(parse_minutes).fillna(0) > 0]
    raw_d = raw_d[raw_d["GAME_ID"].isin(fact_games["game_id"])]
    with engine.connect() as conn:
        db = conn.execute(text("""
            SELECT (SELECT COUNT(*) FROM production.fact_games),
                   (SELECT SUM(total_pts) FROM production.fact_games),
                   (SELECT COUNT(*) FROM production.fact_player_games),
                   (SELECT SUM(pts) FROM production.fact_player_games)""")).one()
    checks = [
        ("fact_games", "row_count", len(raw_g), db[0]),
        ("fact_games", "sum_total_pts", raw_g["PTS_home"].sum() + raw_g["PTS_away"].sum(), db[1]),
        ("fact_player_games", "row_count", len(raw_d), db[2]),
        ("fact_player_games", "sum_pts", raw_d["PTS"].fillna(0).sum(), db[3]),
    ]
    results = []
    for table, name, src, tgt in checks:
        ok = abs(float(src) - float(tgt)) < 1e-6
        results.append(dict(run_at=run_ts, layer="production", table_name=table, check_name=name,
                            source_value=float(src), target_value=float(tgt), passed=ok))
        log.info("  reconcile %-22s %-16s source=%-14s target=%-14s %s", table, name,
                 f"{float(src):,.0f}", f"{float(tgt):,.0f}", "PASS" if ok else "FAIL")

    # Data-quality cross-check (not a hard failure): do player points add up to the team score?
    team_pts = fact_pg.groupby(["game_id", "team_id"])["pts"].sum().rename("player_pts").reset_index()
    tg = team_games(fact_games)[["game_id", "team_id", "pts"]].merge(team_pts, on=["game_id", "team_id"])
    match = float((tg["pts"] == tg["player_pts"]).mean()) if len(tg) else np.nan
    log.info("  quality: player points add up to the team score in %.2f%% of %s team-games",
             match * 100, f"{len(tg):,}")
    results.append(dict(run_at=run_ts, layer="production", table_name="fact_player_games",
                        check_name="pct_team_pts_match", source_value=1.0, target_value=match,
                        passed=match >= 0.99))
    pd.DataFrame(results).to_sql("reconciliation", engine, schema=config.AUDIT_SCHEMA,
                                 if_exists="append", index=False)
    hard = [r for r in results[:4] if not r["passed"]]
    if hard:
        raise RuntimeError("Production reconciliation failed: " + ", ".join(
            f"{r['table_name']}.{r['check_name']}" for r in hard))


# --------------------------------------------------------------------------- main
def run() -> None:
    engine = config.get_engine()
    config.ensure_schemas(engine)

    log.info("Reading raw_data tables")
    raw = {t: read_raw(engine, t) for t in ["games", "games_details", "teams"]}

    dim_team = clean_teams(raw["teams"])
    fact_games = clean_games(raw["games"], dim_team)
    fact_pg = clean_player_games(raw["games_details"], fact_games)
    tg = team_games(fact_games)

    # Drop old production tables first (CASCADE also removes their indexes/keys).
    with engine.begin() as conn:
        for t in ["dim_team", "fact_games", "fact_player_games"]:
            conn.execute(text(f"DROP TABLE IF EXISTS production.{t} CASCADE"))

    write(engine, dim_team, "dim_team")
    write(engine, fact_games, "fact_games")
    write(engine, fact_pg, "fact_player_games")
    write(engine, agg_league_season(fact_games, fact_pg), "agg_league_season")
    write(engine, agg_team_season(tg), "agg_team_season")
    write(engine, agg_player_season(fact_pg), "agg_player_season")
    write(engine, agg_win_factors(fact_games), "agg_win_factors")
    write(engine, agg_starter_bench(fact_pg), "agg_starter_bench")
    create_indexes(engine)
    reconcile(engine, raw["games"], fact_games, raw["games_details"], fact_pg)
    log.info("Transform finished")


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:  # noqa: BLE001
        log.exception("Transform failed: %s", exc)
        sys.exit(1)
