CREATE OR REPLACE VIEW silver.team AS
SELECT
  team_id,
  team_abbreviation,
  team_name
FROM silver.team_dim;
