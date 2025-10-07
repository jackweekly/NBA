CREATE TABLE IF NOT EXISTS silver.home_away_overrides (
  game_id VARCHAR,
  team_id VARCHAR,
  is_home BOOLEAN,
  PRIMARY KEY (game_id, team_id)
);
