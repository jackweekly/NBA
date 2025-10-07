CREATE OR REPLACE VIEW silver.game AS
SELECT
  game_id,
  MAX(season_id)    AS season_id,
  MAX(season_type)  AS season_type,
  MAX(game_date)    AS game_date
FROM silver.team_game
GROUP BY game_id;
