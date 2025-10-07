CREATE OR REPLACE VIEW marts.dim_game AS
WITH enriched AS (
  SELECT
    g.game_id,
    g.season_id,
    g.start_year,
    g.season_type,
    g.game_date,
    MAX(CASE WHEN g.is_home THEN g.team_id END) AS home_team_id,
    MAX(CASE WHEN g.is_home = FALSE THEN g.team_id END) AS away_team_id,
    SUM(CASE WHEN g.is_home THEN g.pts_silver END) AS home_pts,
    SUM(CASE WHEN g.is_home = FALSE THEN g.pts_silver END) AS away_pts
  FROM silver.team_game g
  GROUP BY g.game_id, g.season_id, g.start_year, g.season_type, g.game_date
)
SELECT * FROM enriched;

CREATE OR REPLACE VIEW marts.fact_team_game AS
SELECT
  game_id,
  team_id,
  season_id,
  start_year,
  season_type,
  game_date,
  is_home,
  team_abbreviation,
  team_name,
  wl,
  minutes_raw AS min,
  minutes_raw,
  min_silver,
  min_bad,
  fgm,
  fga,
  fg_pct_raw,
  fg_pct_silver,
  fg3m,
  fg3a,
  fg3_pct_raw,
  fg3_pct_silver,
  ftm,
  fta,
  ft_pct_raw,
  ft_pct_silver,
  oreb,
  dreb,
  reb,
  ast,
  stl,
  blk,
  tov,
  pf,
  pts_original,
  pts_silver,
  pts_from_bx,
  pts_calc,
  pts_calc AS calc_pts,
  pts_mismatch_flag,
  plus_minus,
  has_bx,
  has_pbp,
  has_pts_from_bx,
  team_known,
  row_valid_any,
  row_valid_modern,
  game_id_prefix,
  game_id_prefix_known
FROM silver.team_game;

CREATE OR REPLACE VIEW marts.dim_team AS
SELECT DISTINCT team_id, team_abbreviation, team_name, league
FROM silver.team_dim;

CREATE OR REPLACE VIEW marts.dim_player AS
SELECT DISTINCT player_id, player_name, team_id
FROM silver.player;
