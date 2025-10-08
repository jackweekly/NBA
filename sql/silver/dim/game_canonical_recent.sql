-- Recent canonical built from bronze_box_score_team (has 2023+ for you)
create or replace table silver__game_canonical_recent as
select distinct
  cast(game_id as bigint)                as game_id_canon,
  cast(game_date_est as date)            as game_date_local,
  try_cast(team_id_home as int)          as home_team_id,
  try_cast(team_id_away as int)          as away_team_id,
  -- derive season from date (Aug -> Dec maps to next year season)
  case
    when month(cast(game_date_est as date)) >= 8
      then year(cast(game_date_est as date)) + 1
    else year(cast(game_date_est as date))
  end                                     as season,
  -- season_type may be missing here; default to REG (you can refine later)
  'REG'                                   as season_type
from bronze_box_score_team
where game_date_est is not null
  and team_id_home is not null
  and team_id_away is not null;