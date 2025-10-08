-- One row per teamlog game_id with (date, home/away ids).
-- If home/away fields don't exist, we fall back to least/greatest of team ids.
create or replace table silver__teamlog_games as
with raw as (
  select
    cast(game_id as bigint)           as game_id_teamlog,
    cast(game_date as date)           as game_date_local,
    null::int                         as team_id_home,
    null::int                         as team_id_away,
    try_cast(nullif(team_id,'')      as int) as team_id_single
  from bronze_game_log_team
),
pairs as (
  -- If home/away exist in the table, keep them.
  select
    game_id_teamlog, game_date_local,
    team_id_home, team_id_away
  from raw
  where team_id_home is not null and team_id_away is not null

  union all

  -- Otherwise, reconstruct pairs by grouping two rows per same game_id_teamlog
  select
    r1.game_id_teamlog,
    coalesce(r1.game_date_local, r2.game_date_local) as game_date_local,
    least(r1.team_id_single, r2.team_id_single)      as team_id_home,  -- provisional; just a pair
    greatest(r1.team_id_single, r2.team_id_single)   as team_id_away
  from raw r1
  join raw r2
    on r1.game_id_teamlog = r2.game_id_teamlog
   and r1.team_id_single  < r2.team_id_single
  where (r1.team_id_home is null or r1.team_id_away is null)
    and r1.team_id_single is not null
    and r2.team_id_single is not null
)
select distinct
  game_id_teamlog,
  game_date_local,
  team_id_home,
  team_id_away,
  least(team_id_home, team_id_away)    as team_id_low,
  greatest(team_id_home, team_id_away) as team_id_high
from pairs
where team_id_home is not null and team_id_away is not null;