CREATE TABLE IF NOT EXISTS bronze_game_log_team (
  game_id VARCHAR,
  team_id VARCHAR,
  game_date DATE,
  season_type VARCHAR,
  PRIMARY KEY (game_id, team_id, season_type)
);