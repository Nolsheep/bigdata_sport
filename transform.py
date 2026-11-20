"""Step 2 - Transform.

Reads the raw tables from `raw_data`, cleans / enriches / aggregates / joins
them with pandas, and writes analysis-ready tables to the `production` schema.

production tables
-----------------
fact_games          one row per game (cleaned, joined with team names, features)
team_games          one row per team per game (long format, handy for team stats)
team_season_stats   per team per season (regular season) + final standings
player_season_stats per player per season (regular season box-score averages)
season_summary      league-wide trends per season (scoring, 3PT, home advantage)
win_factors         "how often does the team that wins stat X win the game?"
"""
import logging
import time

import numpy as np
import pandas as pd

from db import PROD_SCHEMA, RAW_SCHEMA, ensure_schema, get_engine, write_table

log = logging.getLogger("transform")

GAME_TYPES = {"1": "Preseason", "2": "Regular Season", "3": "All-Star",
              "4": "Playoffs", "5": "Play-In"}
STAT_COLS = ["pts", "fg_pct", "ft_pct", "fg3_pct", "ast", "reb"]


# --------------------------------------------------------------------------- #
# Extract from raw_data
# --------------------------------------------------------------------------- #
def read_raw(engine, table, columns=None):
    cols = ", ".join(f'"{c}"' for c in columns) if columns else "*"
    df = pd.read_sql(f'SELECT {cols} FROM "{RAW_SCHEMA}"."{table}"', engine)
    return df.drop(columns=["_source_file", "_loaded_at"], errors="ignore")


def season_label(season):
    season = int(season)
    return f"{season}-{str(season + 1)[-2:]}"


# --------------------------------------------------------------------------- #
# Cleaning
# --------------------------------------------------------------------------- #
def clean_teams(teams):
    teams = teams.drop_duplicates("team_id").copy()
    teams["team_id"] = teams["team_id"].astype("int64")
    teams["team_name"] = teams["city"].str.strip() + " " + teams["nickname"].str.strip()
    teams["abbreviation"] = teams["abbreviation"].str.strip()
    teams["arenacapacity"] = pd.to_numeric(teams["arenacapacity"], errors="coerce")
    return teams[["team_id", "abbreviation", "team_name", "city", "nickname",
                  "yearfounded", "arena", "arenacapacity"]]


def clean_games(games):
    n0 = len(games)
    g = games.copy()
    g["game_id"] = pd.to_numeric(g["game_id"], errors="coerce").astype("Int64")
    g = g.dropna(subset=["game_id"])
    g = g.drop_duplicates("game_id", keep="first")
    n_dupes = n0 - len(g)

    # Games without a box score (postponed / not yet played) are useless here.
    g = g.dropna(subset=["pts_home", "pts_away"])
    n_missing = n0 - n_dupes - len(g)

    g["game_date"] = pd.to_datetime(g["game_date_est"], errors="coerce")
    g = g.dropna(subset=["game_date"])
    for c in ["home_team_id", "visitor_team_id", "season", "home_team_wins"]:
        g[c] = pd.to_numeric(g[c], errors="coerce").astype("int64")
    for side in ("home", "away"):
        for s in STAT_COLS:
            g[f"{s}_{side}"] = pd.to_numeric(g[f"{s}_{side}"], errors="coerce")
        # A handful of rows have points but no shooting splits -> fill with
        # the season median so the row can still be used for win/loss counts.
        for s in ["fg_pct", "ft_pct", "fg3_pct", "ast", "reb"]:
            col = f"{s}_{side}"
            g[col] = g[col].fillna(g.groupby("season")[col].transform("median"))

    log.info("games: %s raw rows -> %s clean (removed %s duplicates, %s without scores)",
             f"{n0:,}", f"{len(g):,}", n_dupes, n_missing)
    return g.drop(columns=["game_date_est", "game_status_text", "team_id_home",
                           "team_id_away"], errors="ignore")


def parse_minutes(value):
    """'34:12' -> 34.2 ; '34' -> 34.0 ; NaN/'' -> 0."""
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0.0
    s = str(value).strip()
    if not s:
        return 0.0
    if ":" in s:
        mins, secs = s.split(":", 1)
        try:
            return float(mins) + float(secs) / 60
        except ValueError:
            return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def clean_details(details):
    d = details.drop_duplicates(["game_id", "player_id"]).copy()
    d["game_id"] = pd.to_numeric(d["game_id"], errors="coerce").astype("int64")
    d["minutes"] = d["min"].map(parse_minutes)
    num = ["fgm", "fga", "fg3m", "fg3a", "ftm", "fta", "oreb", "dreb", "reb",
           "ast", "stl", "blk", "to", "pf", "pts", "plus_minus"]
    for c in num:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    # DNP rows have a COMMENT (e.g. "DNP - Coach's Decision") and no minutes.
    d["played"] = (d["minutes"] > 0) | (d["pts"] > 0) | (d["fga"] > 0)
    d["start_position"] = d["start_position"].fillna("Bench")
    return d


# --------------------------------------------------------------------------- #
# Feature engineering + joins
# --------------------------------------------------------------------------- #
def build_fact_games(g, teams):
    f = g.copy()
    f["game_type"] = f["game_id"].astype(str).str[0].map(GAME_TYPES).fillna("Other")
    f["season_label"] = f["season"].map(season_label)
    f["game_month"] = f["game_date"].dt.month
    f["month_name"] = f["game_date"].dt.strftime("%b")
    f["day_of_week"] = f["game_date"].dt.day_name()

    names = teams.set_index("team_id")
    for side, col in (("home", "home_team_id"), ("away", "visitor_team_id")):
        f[f"{side}_team"] = f[col].map(names["abbreviation"])
        f[f"{side}_team_name"] = f[col].map(names["team_name"])

    f["total_points"] = f["pts_home"] + f["pts_away"]
    f["point_diff"] = f["pts_home"] - f["pts_away"]
    f["margin"] = f["point_diff"].abs()
    f["winner_team"] = np.where(f["home_team_wins"] == 1, f["home_team"], f["away_team"])
    f["loser_team"] = np.where(f["home_team_wins"] == 1, f["away_team"], f["home_team"])
    f["game_closeness"] = pd.cut(f["margin"], bins=[-1, 5, 10, 20, 200],
                                 labels=["Close (<=5)", "Competitive (6-10)",
                                         "Comfortable (11-20)", "Blowout (>20)"]).astype(str)
    for s in ["fg_pct", "fg3_pct", "ft_pct", "ast", "reb"]:
        f[f"{s}_diff"] = f[f"{s}_home"] - f[f"{s}_away"]

    order = ["game_id", "game_date", "season", "season_label", "game_type", "game_month",
             "month_name", "day_of_week", "home_team_id", "home_team", "home_team_name",
             "visitor_team_id", "away_team", "away_team_name", "pts_home", "pts_away",
             "total_points", "point_diff", "margin", "game_closeness", "home_team_wins",
             "winner_team", "loser_team"]
    rest = [c for c in f.columns if c not in order]
    return f[order + rest].sort_values("game_date").reset_index(drop=True)


def build_team_games(fact):
    """Stack home and away sides into one row per team per game."""
    frames = []
    for side, other, id_col, opp_id in (("home", "away", "home_team_id", "visitor_team_id"),
                                        ("away", "home", "visitor_team_id", "home_team_id")):
        t = pd.DataFrame({
            "game_id": fact["game_id"], "game_date": fact["game_date"],
            "season": fact["season"], "season_label": fact["season_label"],
            "game_type": fact["game_type"],
            "team_id": fact[id_col], "team": fact[f"{side}_team"],
            "team_name": fact[f"{side}_team_name"],
            "opponent_id": fact[opp_id], "opponent": fact[f"{other}_team"],
            "is_home": side == "home",
        })
        for s in STAT_COLS:
            t[s] = fact[f"{s}_{side}"]
            t[f"opp_{s}"] = fact[f"{s}_{other}"]
        frames.append(t)
    tg = pd.concat(frames, ignore_index=True)
    tg["win"] = (tg["pts"] > tg["opp_pts"]).astype(int)
    tg["point_diff"] = tg["pts"] - tg["opp_pts"]
    return tg.sort_values(["game_date", "game_id", "is_home"]).reset_index(drop=True)


def build_team_season_stats(team_games, ranking, teams):
    rs = team_games[team_games["game_type"] == "Regular Season"]
    agg = rs.groupby(["season", "season_label", "team_id", "team", "team_name"]).agg(
        games=("win", "size"), wins=("win", "sum"),
        ppg=("pts", "mean"), opp_ppg=("opp_pts", "mean"), avg_point_diff=("point_diff", "mean"),
        fg_pct=("fg_pct", "mean"), fg3_pct=("fg3_pct", "mean"), ft_pct=("ft_pct", "mean"),
        apg=("ast", "mean"), rpg=("reb", "mean"),
    ).reset_index()
    home = rs[rs["is_home"]].groupby(["season", "team_id"])["win"].mean().rename("home_win_pct")
    away = rs[~rs["is_home"]].groupby(["season", "team_id"])["win"].mean().rename("away_win_pct")
    agg = agg.join(home, on=["season", "team_id"]).join(away, on=["season", "team_id"])
    agg["losses"] = agg["games"] - agg["wins"]
    agg["win_pct"] = agg["wins"] / agg["games"]

    # Join final standings (last standings date of each regular season).
    r = ranking.copy()
    r["season_id"] = r["season_id"].astype(str)
    r = r[r["season_id"].str.startswith("2")]
    r["season"] = r["season_id"].str[1:].astype(int)
    r["standingsdate"] = pd.to_datetime(r["standingsdate"], errors="coerce")
    r["team_id"] = r["team_id"].astype("int64")
    final = (r.sort_values("standingsdate")
              .groupby(["season", "team_id"]).tail(1)
              [["season", "team_id", "conference", "w", "l", "w_pct"]]
              .rename(columns={"w": "standings_w", "l": "standings_l",
                               "w_pct": "standings_w_pct"}))
    agg = agg.merge(final, on=["season", "team_id"], how="left")
    agg["conference_rank"] = (agg.groupby(["season", "conference"])["win_pct"]
                                 .rank(ascending=False, method="min"))
    agg["league_rank"] = agg.groupby("season")["win_pct"].rank(ascending=False, method="min")
    num = agg.select_dtypes("float").columns
    agg[num] = agg[num].round(4)
    return agg.sort_values(["season", "league_rank"]).reset_index(drop=True)


def build_player_season_stats(details, fact):
    games = fact[["game_id", "season", "season_label", "game_type"]]
    d = details.merge(games, on="game_id", how="inner")
    d = d[(d["game_type"] == "Regular Season") & d["played"]]
    grp = d.groupby(["season", "season_label", "player_id", "player_name"])
    p = grp.agg(
        games=("game_id", "nunique"), starts=("start_position", lambda s: (s != "Bench").sum()),
        minutes=("minutes", "sum"), pts=("pts", "sum"), reb=("reb", "sum"), ast=("ast", "sum"),
        stl=("stl", "sum"), blk=("blk", "sum"), tov=("to", "sum"),
        fgm=("fgm", "sum"), fga=("fga", "sum"), fg3m=("fg3m", "sum"), fg3a=("fg3a", "sum"),
        ftm=("ftm", "sum"), fta=("fta", "sum"), plus_minus=("plus_minus", "sum"),
    ).reset_index()
    team = (d.groupby(["season", "player_id"])["team_abbreviation"]
              .agg(lambda s: s.value_counts().index[0]).rename("team"))
    p = p.join(team, on=["season", "player_id"])

    for stat in ["minutes", "pts", "reb", "ast", "stl", "blk", "tov", "plus_minus"]:
        p[f"{stat}_per_game"] = p[stat] / p["games"]
    p["fg_pct"] = np.where(p["fga"] > 0, p["fgm"] / p["fga"].replace(0, np.nan), np.nan)
    p["fg3_pct"] = np.where(p["fg3a"] > 0, p["fg3m"] / p["fg3a"].replace(0, np.nan), np.nan)
    p["ft_pct"] = np.where(p["fta"] > 0, p["ftm"] / p["fta"].replace(0, np.nan), np.nan)
    tsa = 2 * (p["fga"] + 0.44 * p["fta"])
    p["true_shooting_pct"] = np.where(tsa > 0, p["pts"] / tsa.replace(0, np.nan), np.nan)
    p = p.rename(columns={"minutes_per_game": "mpg", "pts_per_game": "ppg",
                          "reb_per_game": "rpg", "ast_per_game": "apg",
                          "stl_per_game": "spg", "blk_per_game": "bpg",
                          "tov_per_game": "topg"})
    # Rank scorers among players with a meaningful sample (>= 40 games).
    qualified = p["games"] >= 40
    p["ppg_rank"] = np.nan
    p.loc[qualified, "ppg_rank"] = (p[qualified].groupby("season")["ppg"]
                                    .rank(ascending=False, method="min"))
    num = p.select_dtypes("float").columns
    p[num] = p[num].round(3)
    return p.sort_values(["season", "ppg"], ascending=[True, False]).reset_index(drop=True)


def build_season_summary(fact, details):
    rs = fact[fact["game_type"] == "Regular Season"]
    s = rs.groupby(["season", "season_label"]).agg(
        games=("game_id", "size"),
        avg_total_points=("total_points", "mean"),
        avg_margin=("margin", "mean"),
        home_win_pct=("home_team_wins", "mean"),
        close_game_pct=("margin", lambda m: (m <= 5).mean()),
        blowout_pct=("margin", lambda m: (m > 20).mean()),
        avg_fg_pct=("fg_pct_home", "mean"),
    ).reset_index()
    s["avg_team_points"] = s["avg_total_points"] / 2
    team_level = pd.concat([rs[["season", "fg3_pct_home", "ast_home", "reb_home"]]
                            .set_axis(["season", "fg3_pct", "ast", "reb"], axis=1),
                            rs[["season", "fg3_pct_away", "ast_away", "reb_away"]]
                            .set_axis(["season", "fg3_pct", "ast", "reb"], axis=1)])
    s = s.merge(team_level.groupby("season").mean().add_prefix("avg_team_").reset_index(),
                on="season", how="left")

    # 3-point volume from the player box scores.
    d = details.merge(rs[["game_id", "season"]], on="game_id", how="inner")
    tg = d.groupby(["season", "game_id", "team_id"])[["fg3a", "fga", "fg3m", "pts"]].sum()
    tg = tg.reset_index().groupby("season").agg(
        avg_team_3pa=("fg3a", "mean"), avg_team_3pm=("fg3m", "mean"),
        avg_team_fga=("fga", "mean"))
    tg["three_point_attempt_rate"] = tg["avg_team_3pa"] / tg["avg_team_fga"]
    s = s.merge(tg.reset_index(), on="season", how="left")
    num = s.select_dtypes("float").columns
    s[num] = s[num].round(4)
    return s.sort_values("season").reset_index(drop=True)


def build_win_factors(team_games):
    """For each stat: in how many games did the team that was better in that
    stat also win? Shown per season so trends can be compared in Data Studio."""
    rs = team_games[(team_games["game_type"] == "Regular Season")]
    labels = {"fg_pct": "Field goal %", "fg3_pct": "3-point %", "ft_pct": "Free throw %",
              "ast": "Assists", "reb": "Rebounds"}
    rows = []
    for stat, label in labels.items():
        decided = rs[rs[stat] != rs[f"opp_{stat}"]]
        better = decided[decided[stat] > decided[f"opp_{stat}"]]
        per_season = better.groupby(["season", "season_label"])["win"].agg(["mean", "size"])
        for (season, sl), r in per_season.iterrows():
            rows.append({"season": season, "season_label": sl, "stat": stat,
                         "stat_label": label, "games": int(r["size"]),
                         "win_pct_when_better": round(r["mean"], 4)})
        rows.append({"season": 0, "season_label": "All seasons", "stat": stat,
                     "stat_label": label, "games": int(len(better)),
                     "win_pct_when_better": round(better["win"].mean(), 4)})
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
def main():
    t0 = time.time()
    engine = get_engine()
    ensure_schema(engine, PROD_SCHEMA)

    log.info("Reading raw tables ...")
    teams = clean_teams(read_raw(engine, "teams"))
    games = clean_games(read_raw(engine, "games"))
    details = clean_details(read_raw(engine, "games_details"))
    ranking = read_raw(engine, "ranking",
                       ["team_id", "season_id", "standingsdate", "conference", "w", "l", "w_pct"])

    fact = build_fact_games(games, teams)
    team_games = build_team_games(fact)
    tables = {
        "dim_teams": teams,
        "fact_games": fact,
        "team_games": team_games,
        "team_season_stats": build_team_season_stats(team_games, ranking, teams),
        "player_season_stats": build_player_season_stats(details, fact),
        "season_summary": build_season_summary(fact, details),
        "win_factors": build_win_factors(team_games),
    }
    for name, df in tables.items():
        write_table(df, name, PROD_SCHEMA, engine)
        log.info("Wrote %s.%-20s %9s rows %3s cols", PROD_SCHEMA, name, f"{len(df):,}", df.shape[1])
    log.info("Transform finished in %.1fs", time.time() - t0)


if __name__ == "__main__":
    main()
