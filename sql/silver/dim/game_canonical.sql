-- Canonical identity for a game: (game_id, game_date, home_team_id, away_team_id, season, season_type)
create or replace table silver__game_canonical as
select
  cast(game_id as bigint)      as game_id_canon,
  cast(game_date_local as date)as game_date_local,
  try_cast(home_team_id as int)as home_team_id,
  try_cast(away_team_id as int)as away_team_id,
  try_cast(season as int)      as season,
  season_type
from silver__dim_games;