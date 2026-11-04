"""Generate a small synthetic dataset with the SAME columns and quirks as the Kaggle
nathanlauga/nba-games files. Only for testing the pipeline offline - use the real
Kaggle data for the project.  Usage: python scripts/make_sample_data.py data_sample
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd

rng = np.random.default_rng(7)
out = Path(sys.argv[1] if len(sys.argv) > 1 else "data_sample")
out.mkdir(parents=True, exist_ok=True)

teams = pd.DataFrame({
    "LEAGUE_ID": 0, "TEAM_ID": 1610612737 + np.arange(30), "MIN_YEAR": 1949, "MAX_YEAR": 2019,
    "ABBREVIATION": [f"T{i:02d}" for i in range(30)], "NICKNAME": [f"Nick{i}" for i in range(30)],
    "YEARFOUNDED": 1949, "CITY": [f"City{i}" for i in range(30)], "ARENA": [f"Arena{i}" for i in range(30)],
    "ARENACAPACITY": rng.choice([18000.0, np.nan], 30), "OWNER": "x", "GENERALMANAGER": "x",
    "HEADCOACH": "x", "DLEAGUEAFFILIATION": "x"})
teams.to_csv(out / "teams.csv", index=False)

players = {tid: [(tid * 100 + j, f"Player {tid % 100}-{j}") for j in range(13)] for tid in teams.TEAM_ID}
games, details, gid = [], [], 0
for season in range(2015, 2020):
    start = pd.Timestamp(f"{season}-10-20")
    for k in range(240):
        gtype = "2" if k < 220 else "4"
        gid += 1
        game_id = int(f"{gtype}{season % 100:02d}{gid:05d}")
        h, a = rng.choice(teams.TEAM_ID, 2, replace=False)
        box = {}
        for tid in (h, a):
            rows = []
            for j, (pid, name) in enumerate(players[tid]):
                if j >= 11:   # DNP
                    rows.append(dict(PLAYER_ID=pid, PLAYER_NAME=name, START_POSITION=np.nan,
                                     COMMENT="DNP - Coach's Decision", MIN=np.nan))
                    continue
                fga = int(rng.integers(2, 20)); fgm = int(rng.binomial(fga, .46))
                f3a = int(rng.integers(0, min(fga, 4 + season - 2015) + 1)); f3m = min(int(rng.binomial(f3a, .36)), fgm)
                fta = int(rng.integers(0, 8)); ftm = int(rng.binomial(fta, .77))
                oreb, dreb = int(rng.integers(0, 3)), int(rng.integers(0, 8))
                m = int(rng.integers(8, 40))
                mins = rng.choice([f"{m}:{rng.integers(0,60):02d}", str(m), f"{m}.000000:{rng.integers(0,60):02d}"])
                rows.append(dict(PLAYER_ID=pid, PLAYER_NAME=name,
                                 START_POSITION=["F", "F", "C", "G", "G"][j] if j < 5 else np.nan,
                                 COMMENT=np.nan, MIN=mins, FGM=fgm, FGA=fga, FG_PCT=fgm / fga,
                                 FG3M=f3m, FG3A=f3a, FG3_PCT=f3m / f3a if f3a else 0, FTM=ftm, FTA=fta,
                                 FT_PCT=ftm / fta if fta else 0, OREB=oreb, DREB=dreb, REB=oreb + dreb,
                                 AST=int(rng.integers(0, 8)), STL=int(rng.integers(0, 3)), BLK=int(rng.integers(0, 2)),
                                 TO=int(rng.integers(0, 4)), PF=int(rng.integers(0, 5)),
                                 PTS=2 * (fgm - f3m) + 3 * f3m + ftm, PLUS_MINUS=int(rng.integers(-15, 16))))
            df = pd.DataFrame(rows); df["GAME_ID"] = game_id; df["TEAM_ID"] = tid
            df["TEAM_ABBREVIATION"] = teams.set_index("TEAM_ID").loc[tid, "ABBREVIATION"]
            df["TEAM_CITY"] = "c"; df["NICKNAME"] = np.nan
            details.append(df); box[tid] = df.dropna(subset=["MIN"])
        pts = {t: int(box[t].PTS.sum()) for t in (h, a)}
        if pts[h] == pts[a]:
            pts[h] += 1
        s = lambda t, c: box[t][c].sum()
        games.append(dict(GAME_DATE_EST=(start + pd.Timedelta(days=k // 2)).date(), GAME_ID=game_id,
                          GAME_STATUS_TEXT="Final", HOME_TEAM_ID=h, VISITOR_TEAM_ID=a, SEASON=season,
                          TEAM_ID_home=h, PTS_home=pts[h], FG_PCT_home=round(s(h, "FGM") / s(h, "FGA"), 3),
                          FT_PCT_home=round(s(h, "FTM") / max(s(h, "FTA"), 1), 3),
                          FG3_PCT_home=round(s(h, "FG3M") / max(s(h, "FG3A"), 1), 3), AST_home=s(h, "AST"),
                          REB_home=s(h, "REB"), TEAM_ID_away=a, PTS_away=pts[a],
                          FG_PCT_away=round(s(a, "FGM") / s(a, "FGA"), 3),
                          FT_PCT_away=round(s(a, "FTM") / max(s(a, "FTA"), 1), 3),
                          FG3_PCT_away=round(s(a, "FG3M") / max(s(a, "FG3A"), 1), 3), AST_away=s(a, "AST"),
                          REB_away=s(a, "REB"), HOME_TEAM_WINS=int(pts[h] > pts[a])))

g = pd.DataFrame(games)
g = pd.concat([g, g.head(5)])                                   # duplicate GAME_IDs, like the real file
blank = g.head(1).copy(); blank["GAME_ID"] = 29999999; blank[["PTS_home", "PTS_away"]] = np.nan
pd.concat([g, blank]).to_csv(out / "games.csv", index=False)
d = pd.concat(details)
cols = ["GAME_ID", "TEAM_ID", "TEAM_ABBREVIATION", "TEAM_CITY", "PLAYER_ID", "PLAYER_NAME", "NICKNAME",
        "START_POSITION", "COMMENT", "MIN", "FGM", "FGA", "FG_PCT", "FG3M", "FG3A", "FG3_PCT", "FTM", "FTA",
        "FT_PCT", "OREB", "DREB", "REB", "AST", "STL", "BLK", "TO", "PF", "PTS", "PLUS_MINUS"]
d[cols].to_csv(out / "games_details.csv", index=False)
pl = [(name, tid, pid, s) for tid, ps in players.items() for pid, name in ps for s in range(2015, 2020)]
pd.DataFrame(pl, columns=["PLAYER_NAME", "TEAM_ID", "PLAYER_ID", "SEASON"]).to_csv(out / "players.csv", index=False)
pd.DataFrame({"TEAM_ID": teams.TEAM_ID, "LEAGUE_ID": 0, "SEASON_ID": 22019, "STANDINGSDATE": "2020-03-01",
              "CONFERENCE": "East", "TEAM": teams.CITY, "G": 60, "W": 30, "L": 30, "W_PCT": .5,
              "HOME_RECORD": "15-15", "ROAD_RECORD": "15-15", "RETURNTOPLAY": np.nan}).to_csv(out / "ranking.csv", index=False)
print("sample data written to", out)
