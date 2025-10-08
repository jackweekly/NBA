-- Apply overrides on top of the current union
create or replace table silver__dim_games_union as
with ovr as (
  select * from silver__season_type_overrides
)
select
  g.game_id,
  g.game_date_local,
  g.home_team_id,
  g.away_team_id,
  g.season,
  coalesce(o.season_type_new, g.season_type) as season_type
from silver__dim_games_union g
left join ovr o using (game_id);