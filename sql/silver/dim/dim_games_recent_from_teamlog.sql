-- Canonical recent games from team logs with robust season_type resolution
create or replace table silver__dim_games_recent as
with raw as (
  select
    cast(game_id as bigint)            as game_id,
    cast(game_date as date)            as game_date_local,
    try_cast(team_id as int)           as team_id,
    lower(nullif(trim(season_type), '')) as season_type_raw
  from bronze_game_log_team
  where game_id is not null and game_date is not null
),
-- infer home/away deterministically (two team_ids per game; min=home, max=away is stable in your feed)
teams as (
  select
    game_id,
    game_date_local,
    min(team_id) as home_team_id,
    max(team_id) as away_team_id
  from raw
  group by 1,2
),
-- majority vote for season_type, never default to REG blindly
stype as (
  select
    game_id,
    any_value(game_date_local) as game_date_local,
    -- count labels
    sum(case when season_type_raw like '%pre%'  then 1 else 0 end) as c_pre,
    sum(case when season_type_raw like '%post%' then 1 else 0 end) as c_post,
    sum(case when season_type_raw like '%reg%'  then 1 else 0 end) as c_reg,
    sum(case when season_type_raw is null or season_type_raw='' then 1 else 0 end) as c_null
  from raw
  group by 1
),
resolved as (
  select
    s.game_id,
    s.game_date_local,
    case
      when c_post > greatest(c_reg, c_pre) then 'POST'
      when c_pre  > greatest(c_reg, c_post) then 'PRE'
      when c_reg  > greatest(c_pre, c_post) then 'REG'
      -- month heuristic fallback ONLY if tie/unknown
      when strftime('%m', s.game_date_local) in ('09','10') then 'PRE'
      when strftime('%m', s.game_date_local) in ('05','06','07') then 'POST'
      else 'REG'
    end as season_type
  from stype s
)
select
  t.game_id,
  t.game_date_local,
  t.home_team_id,
  t.away_team_id,
  case when month(t.game_date_local) >= 8 then year(t.game_date_local) + 1 else year(t.game_date_local) end as season,
  r.season_type
from teams t
join resolved r using (game_id, game_date_local);