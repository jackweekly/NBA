create or replace table silver__game_id_bridge as
with canon as (
  select
    game_id_canon,
    game_date_local,
    least(home_team_id, away_team_id)    as team_id_low,
    greatest(home_team_id, away_team_id) as team_id_high
  from silver__game_canonical_union
),
joined as (
  select
    c.game_id_canon,
    t.game_id_teamlog
  from canon c
  join silver__teamlog_games t
    on c.game_date_local = t.game_date_local
   and c.team_id_low     = t.team_id_low
   and c.team_id_high    = t.team_id_high
)
select distinct game_id_teamlog, game_id_canon
from joined;