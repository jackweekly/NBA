PRAGMA threads=4;

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS marts;

CREATE OR REPLACE VIEW staging.team_game AS
WITH raw_union AS (
    SELECT * FROM raw_game
    UNION
    SELECT * FROM raw_games
)
SELECT
    CAST(season_id AS VARCHAR) AS season_id,
    CAST(team_id AS VARCHAR) AS team_id,
    team_abbreviation,
    team_name,
    CAST(game_id AS VARCHAR) AS game_id,
    CAST(TRY_CAST(game_date AS DATE) AS DATE) AS game_date,
    matchup,
    wl,
    CAST(min AS INTEGER) AS min,
    CAST(fgm AS DOUBLE) AS fgm,
    CAST(fga AS DOUBLE) AS fga,
    CAST(fg_pct AS DOUBLE) AS fg_pct,
    CAST(fg3m AS DOUBLE) AS fg3m,
    CAST(fg3a AS DOUBLE) AS fg3a,
    CAST(fg3_pct AS DOUBLE) AS fg3_pct,
    CAST(ftm AS DOUBLE) AS ftm,
    CAST(fta AS DOUBLE) AS fta,
    CAST(ft_pct AS DOUBLE) AS ft_pct,
    CAST(oreb AS DOUBLE) AS oreb,
    CAST(dreb AS DOUBLE) AS dreb,
    CAST(reb AS DOUBLE) AS reb,
    CAST(ast AS DOUBLE) AS ast,
    CAST(stl AS DOUBLE) AS stl,
    CAST(blk AS DOUBLE) AS blk,
    CAST(tov AS DOUBLE) AS tov,
    CAST(pf AS DOUBLE) AS pf,
    CAST(pts AS DOUBLE) AS pts,
    CAST(plus_minus AS INTEGER) AS plus_minus,
    CAST(video_available AS BOOLEAN) AS video_available,
    COALESCE(season_type, 'Regular Season') AS season_type
FROM raw_union;

CREATE OR REPLACE VIEW staging.team AS
SELECT DISTINCT
    CAST(team_id AS VARCHAR) AS team_id,
    team_abbreviation,
    team_name
FROM staging.team_game;

CREATE OR REPLACE VIEW staging.player AS
SELECT DISTINCT
    CAST(player_id AS VARCHAR) AS player_id,
    player_name,
    team_id
FROM (
    SELECT * FROM raw_player
    UNION
    SELECT * FROM raw_players
)
WHERE player_id IS NOT NULL;

CREATE OR REPLACE VIEW marts.dim_game AS
WITH tagged AS (
    SELECT
        game_id,
        MAX(season_id) AS season_id,
        MAX(season_type) AS season_type,
        MAX(game_date) AS game_date,
        MAX(CASE WHEN POSITION('vs.' IN LOWER(matchup)) > 0 THEN team_id END) AS home_team_id,
        MAX(CASE WHEN POSITION('@' IN LOWER(matchup)) > 0 THEN team_id END) AS away_team_id,
        SUM(CASE WHEN POSITION('vs.' IN LOWER(matchup)) > 0 THEN pts END) AS home_pts,
        SUM(CASE WHEN POSITION('@' IN LOWER(matchup)) > 0 THEN pts END) AS away_pts
    FROM staging.team_game
    GROUP BY game_id
)
SELECT
    game_id,
    season_id,
    season_type,
    game_date,
    home_team_id,
    away_team_id,
    home_pts,
    away_pts
FROM tagged;

CREATE OR REPLACE VIEW marts.fact_team_game AS
SELECT
    CAST(game_id AS VARCHAR) AS game_id,
    CAST(team_id AS VARCHAR) AS team_id,
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
FROM staging.team_game;

CREATE OR REPLACE VIEW marts.dim_team AS
SELECT DISTINCT team_id, team_abbreviation, team_name
FROM staging.team;

CREATE OR REPLACE VIEW marts.dim_player AS
SELECT DISTINCT player_id, player_name, team_id
FROM staging.player;
