PRAGMA threads=4;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS marts;

-- ===== SILVER LAYER =====
CREATE OR REPLACE VIEW silver.team_game AS
SELECT
  CAST(season_id AS VARCHAR)                  AS season_id,
  LPAD(CAST(game_id AS VARCHAR), 10, '0')     AS game_id,
  CAST(team_id AS VARCHAR)                    AS team_id,
  LOWER(team_abbreviation)                    AS team_abbreviation,
  team_name,
  TRY_CAST(game_date AS DATE)                 AS game_date,
  LOWER(COALESCE(season_type, 'Regular Season')) AS season_type,
  wl,
  CAST(min AS DOUBLE)                         AS min,
  CAST(fgm AS DOUBLE)                         AS fgm,
  CAST(fga AS DOUBLE)                         AS fga,
  CAST(fg_pct AS DOUBLE)                      AS fg_pct,
  CAST(fg3m AS DOUBLE)                        AS fg3m,
  CAST(fg3a AS DOUBLE)                        AS fg3a,
  CAST(fg3_pct AS DOUBLE)                     AS fg3_pct,
  CAST(ftm AS DOUBLE)                         AS ftm,
  CAST(fta AS DOUBLE)                         AS fta,
  CAST(ft_pct AS DOUBLE)                      AS ft_pct,
  CAST(oreb AS DOUBLE)                        AS oreb,
  CAST(dreb AS DOUBLE)                        AS dreb,
  CAST(reb AS DOUBLE)                         AS reb,
  CAST(ast AS DOUBLE)                         AS ast,
  CAST(stl AS DOUBLE)                         AS stl,
  CAST(blk AS DOUBLE)                         AS blk,
  CAST(tov AS DOUBLE)                         AS tov,
  CAST(pf AS DOUBLE)                          AS pf,
  CAST(pts AS DOUBLE)                         AS pts,
  CAST(plus_minus AS DOUBLE)                  AS plus_minus,
  CAST(video_available AS BOOLEAN)            AS video_available,
  matchup
FROM bronze_game_log_team;

CREATE OR REPLACE VIEW silver.game AS
SELECT
  CAST(game_id AS VARCHAR) AS game_id,
  MAX(season_id)           AS season_id,
  MAX(season_type)         AS season_type,
  MAX(game_date)           AS game_date
FROM silver.team_game
GROUP BY game_id;

CREATE OR REPLACE VIEW silver.team AS
SELECT
  CAST(team_id AS VARCHAR) AS team_id,
  ANY_VALUE(team_abbreviation) AS team_abbreviation,
  ANY_VALUE(team_name) AS team_name
FROM silver.team_game
WHERE team_id IS NOT NULL
GROUP BY team_id;

CREATE OR REPLACE VIEW silver.player AS
SELECT DISTINCT
  CAST(id AS VARCHAR) AS player_id,
  full_name AS player_name,
  NULL::VARCHAR AS team_id
FROM bronze_player
WHERE id IS NOT NULL;

-- ===== MARTS =====
CREATE OR REPLACE VIEW marts.dim_game AS
WITH enriched AS (
  SELECT
    g.game_id,
    g.season_id,
    g.season_type,
    g.game_date,
    MAX(CASE WHEN POSITION('vs.' IN LOWER(matchup)) > 0 THEN team_id END) AS home_team_id,
    MAX(CASE WHEN POSITION('@'   IN LOWER(matchup)) > 0 THEN team_id END) AS away_team_id,
    SUM(CASE WHEN POSITION('vs.' IN LOWER(matchup)) > 0 THEN pts END)     AS home_pts,
    SUM(CASE WHEN POSITION('@'   IN LOWER(matchup)) > 0 THEN pts END)     AS away_pts
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
  plus_minus
FROM silver.team_game;

CREATE OR REPLACE VIEW marts.dim_team AS
SELECT DISTINCT team_id, team_abbreviation, team_name
FROM silver.team;

CREATE OR REPLACE VIEW marts.dim_player AS
SELECT DISTINCT player_id, player_name, team_id
FROM silver.player;
