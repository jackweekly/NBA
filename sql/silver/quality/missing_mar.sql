-- For team_games: how missingness clusters by season, season_type, team, home/away
create or replace table silver__team_games_missing as
select
  season, season_type, team_id, is_home,
  sum( (pts is null)::int )  as miss_pts,
  sum( (fgm is null)::int )  as miss_fgm,
  sum( (fga is null)::int )  as miss_fga,
  sum( (tpm is null)::int )  as miss_tpm,
  sum( (tpa is null)::int )  as miss_tpa,
  sum( (ftm is null)::int )  as miss_ftm,
  sum( (fta is null)::int )  as miss_fta,
  sum( (oreb is null)::int ) as miss_oreb,
  sum( (dreb is null)::int ) as miss_dreb,
  sum( (ast is null)::int )  as miss_ast,
  sum( (stl is null)::int )  as miss_stl,
  sum( (blk is null)::int )  as miss_blk,
  sum( (tov is null)::int )  as miss_tov,
  sum( (pf is null)::int )   as miss_pf,
  count(*)                   as n_games
from silver__team_games
group by 1,2,3,4;

-- Top clusters where a metric is structurally missing (>20% of games in the group)
create or replace table silver__team_games_missing_hotspots as
select *
from (
  select season, season_type, team_id, is_home, n_games,
         'pts'  as metric, cast(miss_pts  as int) as miss, round(miss_pts*100.0/nullif(n_games,0),2) as pct from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'fgm', miss_fgm, round(miss_fgm*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'fga', miss_fga, round(miss_fga*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'tpm', miss_tpm, round(miss_tpm*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'tpa', miss_tpa, round(miss_tpa*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'ftm', miss_ftm, round(miss_ftm*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'fta', miss_fta, round(miss_fta*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'oreb', miss_oreb, round(miss_oreb*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'dreb', miss_dreb, round(miss_dreb*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'ast', miss_ast, round(miss_ast*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'stl', miss_stl, round(miss_stl*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'blk', miss_blk, round(miss_blk*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'tov', miss_tov, round(miss_tov*100.0/nullif(n_games,0),2) from silver__team_games_missing union all
  select season, season_type, team_id, is_home, n_games,
         'pf',  miss_pf,  round(miss_pf*100.0/nullif(n_games,0),2) from silver__team_games_missing
)
where pct >= 20.0
order by pct desc, n_games desc;