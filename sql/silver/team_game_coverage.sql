CREATE OR REPLACE VIEW silver.team_game_cov AS
WITH bx AS (
  SELECT DISTINCT game_id, team_id
  FROM silver.box_score_team_norm
),
pbp AS (
  SELECT DISTINCT LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id
  FROM bronze_play_by_play
)
SELECT
  sg.*,
  COALESCE(sg.has_bx, bx.game_id IS NOT NULL) AS has_box_score_team,
  COALESCE(sg.has_pbp, pbp.game_id IS NOT NULL) AS has_pbp
FROM silver.team_game sg
LEFT JOIN bx
  ON sg.game_id = bx.game_id AND sg.team_id = bx.team_id
LEFT JOIN pbp
  ON sg.game_id = pbp.game_id;
