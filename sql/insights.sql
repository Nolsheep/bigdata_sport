-- Queries that answer the 5 business questions straight from PostgreSQL.
-- Open in DBeaver (or: docker exec -i nba_postgres psql -U nba -d nba_dw < sql/insights.sql)

-- Q1. Is there a home-court advantage, and is it shrinking?
SELECT season_label, games, ROUND((home_win_pct * 100)::numeric, 1) AS home_win_pct
FROM production.season_summary ORDER BY season;

-- Q2. How have scoring and 3-point shooting changed?
SELECT season_label, ROUND((avg_team_points)::numeric, 1) AS ppg, ROUND((avg_team_3pa)::numeric, 1) AS three_pa,
       ROUND((three_point_attempt_rate * 100)::numeric, 1) AS three_pa_rate_pct,
       ROUND((avg_team_fg3_pct * 100)::numeric, 1) AS fg3_pct
FROM production.season_summary ORDER BY season;

-- Q3. Which teams have been the most successful (regular season)?
SELECT team_name, COUNT(*) AS seasons, SUM(wins) AS wins, SUM(losses) AS losses,
       ROUND((SUM(wins)::numeric / SUM(games) * 100)::numeric, 1) AS win_pct,
       ROUND((AVG(avg_point_diff))::numeric, 2) AS avg_point_diff
FROM production.team_season_stats GROUP BY team_name ORDER BY win_pct DESC LIMIT 10;

-- Best single seasons
SELECT season_label, team_name, wins, losses, ROUND((win_pct * 100)::numeric, 1) AS win_pct
FROM production.team_season_stats ORDER BY win_pct DESC LIMIT 10;

-- Q4. Who are the top scorers? (season scoring leaders, min 40 games)
SELECT season_label, player_name, team, games, ROUND((ppg)::numeric, 1) AS ppg
FROM production.player_season_stats WHERE ppg_rank = 1 ORDER BY season;

-- Most total regular-season points in the dataset
SELECT player_name, SUM(games) AS games, SUM(pts)::int AS total_pts,
       ROUND((SUM(pts)::numeric / SUM(games))::numeric, 1) AS ppg
FROM production.player_season_stats GROUP BY player_id, player_name
ORDER BY total_pts DESC LIMIT 10;

-- Q5. Which stat matters most for winning?
SELECT stat_label, games, ROUND((win_pct_when_better * 100)::numeric, 1) AS win_pct_when_better
FROM production.win_factors WHERE season = 0 ORDER BY win_pct_when_better DESC;

-- Extra: how often are games close vs blowouts?
SELECT game_closeness, COUNT(*) AS games, ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ())::numeric, 1) AS pct
FROM production.fact_games WHERE game_type = 'Regular Season' GROUP BY game_closeness ORDER BY games DESC;
