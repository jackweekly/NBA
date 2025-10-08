-- One row per (game, team) with opponent + home flag + totals
create or replace table silver__team_games as
with base as (
  select
    d.season, d.season_type, d.game_date_local,
    s.game_id,
    s.team_id,
    case when s.team_id = d.home_team_id then true
         when s.team_id = d.away_team_id then false
         else null end                         as is_home,
    case when s.team_id = d.home_team_id then d.away_team_id
         when s.team_id = d.away_team_id then d.home_team_id
         else null end                         as opp_team_id,
    s.pts, s.fgm, s.fga, s.tpm, s.tpa, s.ftm, s.fta,
    s.oreb, s.dreb, s.ast, s.stl, s.blk, s.tov, s.pf,
    cast(null as int)                          as minutes_team
  from silver__source_team_stats s
  join silver__dim_games_union d using (game_id)
)
select *
from base
where is_home is not null;