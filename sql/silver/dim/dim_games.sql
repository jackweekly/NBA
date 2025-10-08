create or replace table silver__dim_games as
with g as (
  select
    cast(bg.game_id as bigint)                                   as game_id,
    cast(bg.game_date as date)                                   as game_date_local,
    coalesce(bg.season_type, 'Regular Season')                   as season_type_raw,
    try_cast(bg.season_id as int)                                as season_raw,
    try_cast(bg.team_id_home as int)                             as home_team_id
  from bronze_game bg
),
away as (
  -- bronze_box_score_team has both home/away in one row; grab away id once per game
  select
    cast(game_id as bigint)                    as game_id,
    max(try_cast(team_id_away as int))         as away_team_id
  from bronze_box_score_team
  group by 1
)
select
  g.game_id,
  g.game_date_local,
  g.season_type_raw,
  g.season_raw,
  g.home_team_id,
  a.away_team_id,
  case
    when month(g.game_date_local) >= 8 then year(g.game_date_local) + 1
    else year(g.game_date_local)
  end                                                as season,
  case
    when lower(g.season_type_raw) like '%pre%'  then 'PRE'
    when lower(g.season_type_raw) like '%post%' then 'POST'
    else 'REG'
  end                                                as season_type
from g
left join away a using (game_id);