-- Union-of-truth for game identity across eras
create or replace table silver__game_canonical_union as
select distinct
  game_id_canon, game_date_local, home_team_id, away_team_id, season, season_type
from silver__game_canonical

union all

select distinct
  game_id_canon, game_date_local, home_team_id, away_team_id, season, season_type
from silver__game_canonical_recent;