CREATE OR REPLACE VIEW silver.box_score_team_norm AS
SELECT
  game_id,
  team_id_home AS team_id,
  'home' AS side,
  * EXCLUDE (team_id_home, team_id_away)
FROM bronze_box_score_team
UNION ALL
SELECT
  game_id,
  team_id_away AS team_id,
  'away' AS side,
  * EXCLUDE (team_id_home, team_id_away)
FROM bronze_box_score_team;
