CREATE OR REPLACE VIEW helper.helper_season_year AS
WITH seasons AS (
  SELECT DISTINCT CAST(season_id AS VARCHAR) AS season_id
  FROM bronze_game_log_team
  WHERE season_id IS NOT NULL
)
SELECT
  season_id,
  TRY_CAST(SUBSTRING(season_id, 1, 4) AS INTEGER) AS start_year
FROM seasons;
