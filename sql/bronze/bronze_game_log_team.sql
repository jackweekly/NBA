DROP VIEW IF EXISTS bronze_game_log_team;
DROP TABLE IF EXISTS bronze_game_log_team;
CREATE OR REPLACE VIEW bronze_game_log_team AS
SELECT
  game_id,
  team_id_home AS team_id,
  game_date
FROM bronze_game_norm
UNION ALL
SELECT
  game_id,
  team_id_away AS team_id,
  game_date
FROM bronze_game_norm;