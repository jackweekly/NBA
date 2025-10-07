CREATE OR REPLACE VIEW silver.team_game_cov AS
WITH sg AS (
  SELECT * FROM silver.team_game
),
bx AS (
  SELECT DISTINCT game_id, team_id FROM silver.box_score_team_norm
),
pbp AS (
  SELECT DISTINCT game_id FROM bronze_play_by_play
)
SELECT
  sg.*,
  (bx.game_id IS NOT NULL) AS has_box_score_team,
  (pbp.game_id IS NOT NULL) AS has_pbp
FROM sg
LEFT JOIN bx
  ON sg.game_id = bx.game_id AND sg.team_id = bx.team_id
LEFT JOIN pbp
  ON sg.game_id = pbp.game_id;
