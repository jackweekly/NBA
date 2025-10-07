CREATE OR REPLACE VIEW marts.dim_game AS
WITH enriched AS (
  SELECT
    g.game_id,
    g.season_id,
    g.season_type,
    g.game_date,
    MAX(CASE WHEN g.is_home THEN g.team_id END) AS home_team_id,
    MAX(CASE WHEN g.is_home = FALSE THEN g.team_id END) AS away_team_id,
    SUM(CASE WHEN g.is_home THEN g.pts END) AS home_pts,
    SUM(CASE WHEN g.is_home = FALSE THEN g.pts END) AS away_pts
  FROM silver.team_game g
  GROUP BY g.game_id, g.season_id, g.season_type, g.game_date
)
SELECT * FROM enriched;

CREATE OR REPLACE VIEW marts.fact_team_game AS
SELECT
  game_id,
  team_id,
  season_id,
  season_type,
  game_date,
  is_home,
  team_abbreviation,
  team_name,
  wl,
  min,
  fgm,
  fga,
  fg_pct,
  fg3m,
  fg3a,
  fg3_pct,
  ftm,
  fta,
  ft_pct,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  tov,
  pf,
  pts,
  plus_minus,
  calc_pts,
  pts_mismatch
FROM silver.team_game;

CREATE OR REPLACE VIEW marts.dim_team AS
SELECT DISTINCT team_id, team_abbreviation, team_name, league
FROM silver.team_dim;

CREATE OR REPLACE VIEW marts.dim_player AS
SELECT DISTINCT player_id, player_name, team_id
FROM silver.player;
