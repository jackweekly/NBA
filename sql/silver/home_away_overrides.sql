CREATE TABLE IF NOT EXISTS silver.home_away_overrides (
  game_id BIGINT PRIMARY KEY,
  date DATE,
  season INTEGER,
  team_id_home INTEGER,
  team_id_away INTEGER,
  home_override BOOLEAN,
  away_override BOOLEAN,
  source VARCHAR,
  updated_at TIMESTAMP DEFAULT NOW()
);
