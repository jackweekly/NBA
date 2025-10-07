CREATE OR REPLACE VIEW silver.player AS
SELECT DISTINCT
  CAST(id AS VARCHAR) AS player_id,
  full_name AS player_name,
  NULL::VARCHAR AS team_id
FROM bronze_player
WHERE id IS NOT NULL;
