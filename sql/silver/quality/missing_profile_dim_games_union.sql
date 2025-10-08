create or replace table silver__missing_profile_dim_games_union as
select
  'silver__dim_games_union' as table_name,
  'game_id' as column_name,
  count(*) as n_rows,
  sum(case when game_id is null then 1 else 0 end) as n_null,
  round( sum(case when game_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct game_id) as n_distinct,
  cast(min(game_id) as varchar) as v_min,
  cast(max(game_id) as varchar) as v_max
from silver__dim_games_union
union all
select
  'silver__dim_games_union' as table_name,
  'game_date_local' as column_name,
  count(*) as n_rows,
  sum(case when game_date_local is null then 1 else 0 end) as n_null,
  round( sum(case when game_date_local is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct game_date_local) as n_distinct,
  cast(min(game_date_local) as varchar) as v_min,
  cast(max(game_date_local) as varchar) as v_max
from silver__dim_games_union
union all
select
  'silver__dim_games_union' as table_name,
  'home_team_id' as column_name,
  count(*) as n_rows,
  sum(case when home_team_id is null then 1 else 0 end) as n_null,
  round( sum(case when home_team_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct home_team_id) as n_distinct,
  cast(min(home_team_id) as varchar) as v_min,
  cast(max(home_team_id) as varchar) as v_max
from silver__dim_games_union
union all
select
  'silver__dim_games_union' as table_name,
  'away_team_id' as column_name,
  count(*) as n_rows,
  sum(case when away_team_id is null then 1 else 0 end) as n_null,
  round( sum(case when away_team_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct away_team_id) as n_distinct,
  cast(min(away_team_id) as varchar) as v_min,
  cast(max(away_team_id) as varchar) as v_max
from silver__dim_games_union
union all
select
  'silver__dim_games_union' as table_name,
  'season' as column_name,
  count(*) as n_rows,
  sum(case when season is null then 1 else 0 end) as n_null,
  round( sum(case when season is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct season) as n_distinct,
  cast(min(season) as varchar) as v_min,
  cast(max(season) as varchar) as v_max
from silver__dim_games_union
union all
select
  'silver__dim_games_union' as table_name,
  'season_type' as column_name,
  count(*) as n_rows,
  sum(case when season_type is null then 1 else 0 end) as n_null,
  round( sum(case when season_type is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct season_type) as n_distinct,
  cast(min(season_type) as varchar) as v_min,
  cast(max(season_type) as varchar) as v_max
from silver__dim_games_union;