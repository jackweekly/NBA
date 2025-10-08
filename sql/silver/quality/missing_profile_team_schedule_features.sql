create or replace table silver__missing_profile_team_schedule_features as
select
  'silver__team_schedule_features' as table_name,
  'season' as column_name,
  count(*) as n_rows,
  sum(case when season is null then 1 else 0 end) as n_null,
  round( sum(case when season is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct season) as n_distinct,
  cast(min(season) as varchar) as v_min,
  cast(max(season) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'season_type' as column_name,
  count(*) as n_rows,
  sum(case when season_type is null then 1 else 0 end) as n_null,
  round( sum(case when season_type is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct season_type) as n_distinct,
  cast(min(season_type) as varchar) as v_min,
  cast(max(season_type) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'game_date_local' as column_name,
  count(*) as n_rows,
  sum(case when game_date_local is null then 1 else 0 end) as n_null,
  round( sum(case when game_date_local is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct game_date_local) as n_distinct,
  cast(min(game_date_local) as varchar) as v_min,
  cast(max(game_date_local) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'game_id' as column_name,
  count(*) as n_rows,
  sum(case when game_id is null then 1 else 0 end) as n_null,
  round( sum(case when game_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct game_id) as n_distinct,
  cast(min(game_id) as varchar) as v_min,
  cast(max(game_id) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'team_id' as column_name,
  count(*) as n_rows,
  sum(case when team_id is null then 1 else 0 end) as n_null,
  round( sum(case when team_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct team_id) as n_distinct,
  cast(min(team_id) as varchar) as v_min,
  cast(max(team_id) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'is_home' as column_name,
  count(*) as n_rows,
  sum(case when is_home is null then 1 else 0 end) as n_null,
  round( sum(case when is_home is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct is_home) as n_distinct,
  cast(min(is_home) as varchar) as v_min,
  cast(max(is_home) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'opp_team_id' as column_name,
  count(*) as n_rows,
  sum(case when opp_team_id is null then 1 else 0 end) as n_null,
  round( sum(case when opp_team_id is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct opp_team_id) as n_distinct,
  cast(min(opp_team_id) as varchar) as v_min,
  cast(max(opp_team_id) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'pts' as column_name,
  count(*) as n_rows,
  sum(case when pts is null then 1 else 0 end) as n_null,
  round( sum(case when pts is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct pts) as n_distinct,
  cast(min(pts) as varchar) as v_min,
  cast(max(pts) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'fgm' as column_name,
  count(*) as n_rows,
  sum(case when fgm is null then 1 else 0 end) as n_null,
  round( sum(case when fgm is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct fgm) as n_distinct,
  cast(min(fgm) as varchar) as v_min,
  cast(max(fgm) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'fga' as column_name,
  count(*) as n_rows,
  sum(case when fga is null then 1 else 0 end) as n_null,
  round( sum(case when fga is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct fga) as n_distinct,
  cast(min(fga) as varchar) as v_min,
  cast(max(fga) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'tpm' as column_name,
  count(*) as n_rows,
  sum(case when tpm is null then 1 else 0 end) as n_null,
  round( sum(case when tpm is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct tpm) as n_distinct,
  cast(min(tpm) as varchar) as v_min,
  cast(max(tpm) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'tpa' as column_name,
  count(*) as n_rows,
  sum(case when tpa is null then 1 else 0 end) as n_null,
  round( sum(case when tpa is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct tpa) as n_distinct,
  cast(min(tpa) as varchar) as v_min,
  cast(max(tpa) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'ftm' as column_name,
  count(*) as n_rows,
  sum(case when ftm is null then 1 else 0 end) as n_null,
  round( sum(case when ftm is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct ftm) as n_distinct,
  cast(min(ftm) as varchar) as v_min,
  cast(max(ftm) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'fta' as column_name,
  count(*) as n_rows,
  sum(case when fta is null then 1 else 0 end) as n_null,
  round( sum(case when fta is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct fta) as n_distinct,
  cast(min(fta) as varchar) as v_min,
  cast(max(fta) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'oreb' as column_name,
  count(*) as n_rows,
  sum(case when oreb is null then 1 else 0 end) as n_null,
  round( sum(case when oreb is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct oreb) as n_distinct,
  cast(min(oreb) as varchar) as v_min,
  cast(max(oreb) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'dreb' as column_name,
  count(*) as n_rows,
  sum(case when dreb is null then 1 else 0 end) as n_null,
  round( sum(case when dreb is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct dreb) as n_distinct,
  cast(min(dreb) as varchar) as v_min,
  cast(max(dreb) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'ast' as column_name,
  count(*) as n_rows,
  sum(case when ast is null then 1 else 0 end) as n_null,
  round( sum(case when ast is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct ast) as n_distinct,
  cast(min(ast) as varchar) as v_min,
  cast(max(ast) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'stl' as column_name,
  count(*) as n_rows,
  sum(case when stl is null then 1 else 0 end) as n_null,
  round( sum(case when stl is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct stl) as n_distinct,
  cast(min(stl) as varchar) as v_min,
  cast(max(stl) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'blk' as column_name,
  count(*) as n_rows,
  sum(case when blk is null then 1 else 0 end) as n_null,
  round( sum(case when blk is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct blk) as n_distinct,
  cast(min(blk) as varchar) as v_min,
  cast(max(blk) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'tov' as column_name,
  count(*) as n_rows,
  sum(case when tov is null then 1 else 0 end) as n_null,
  round( sum(case when tov is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct tov) as n_distinct,
  cast(min(tov) as varchar) as v_min,
  cast(max(tov) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'pf' as column_name,
  count(*) as n_rows,
  sum(case when pf is null then 1 else 0 end) as n_null,
  round( sum(case when pf is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct pf) as n_distinct,
  cast(min(pf) as varchar) as v_min,
  cast(max(pf) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'minutes_team' as column_name,
  count(*) as n_rows,
  sum(case when minutes_team is null then 1 else 0 end) as n_null,
  round( sum(case when minutes_team is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct minutes_team) as n_distinct,
  cast(min(minutes_team) as varchar) as v_min,
  cast(max(minutes_team) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'gnum' as column_name,
  count(*) as n_rows,
  sum(case when gnum is null then 1 else 0 end) as n_null,
  round( sum(case when gnum is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct gnum) as n_distinct,
  cast(min(gnum) as varchar) as v_min,
  cast(max(gnum) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'prev_date' as column_name,
  count(*) as n_rows,
  sum(case when prev_date is null then 1 else 0 end) as n_null,
  round( sum(case when prev_date is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct prev_date) as n_distinct,
  cast(min(prev_date) as varchar) as v_min,
  cast(max(prev_date) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'days_rest' as column_name,
  count(*) as n_rows,
  sum(case when days_rest is null then 1 else 0 end) as n_null,
  round( sum(case when days_rest is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct days_rest) as n_distinct,
  cast(min(days_rest) as varchar) as v_min,
  cast(max(days_rest) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'rest_bucket' as column_name,
  count(*) as n_rows,
  sum(case when rest_bucket is null then 1 else 0 end) as n_null,
  round( sum(case when rest_bucket is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct rest_bucket) as n_distinct,
  cast(min(rest_bucket) as varchar) as v_min,
  cast(max(rest_bucket) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'pts_l5_avg' as column_name,
  count(*) as n_rows,
  sum(case when pts_l5_avg is null then 1 else 0 end) as n_null,
  round( sum(case when pts_l5_avg is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct pts_l5_avg) as n_distinct,
  cast(min(pts_l5_avg) as varchar) as v_min,
  cast(max(pts_l5_avg) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'tov_l5_avg' as column_name,
  count(*) as n_rows,
  sum(case when tov_l5_avg is null then 1 else 0 end) as n_null,
  round( sum(case when tov_l5_avg is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct tov_l5_avg) as n_distinct,
  cast(min(tov_l5_avg) as varchar) as v_min,
  cast(max(tov_l5_avg) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'tpm_l10_avg' as column_name,
  count(*) as n_rows,
  sum(case when tpm_l10_avg is null then 1 else 0 end) as n_null,
  round( sum(case when tpm_l10_avg is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct tpm_l10_avg) as n_distinct,
  cast(min(tpm_l10_avg) as varchar) as v_min,
  cast(max(tpm_l10_avg) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'reb_l10_avg' as column_name,
  count(*) as n_rows,
  sum(case when reb_l10_avg is null then 1 else 0 end) as n_null,
  round( sum(case when reb_l10_avg is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct reb_l10_avg) as n_distinct,
  cast(min(reb_l10_avg) as varchar) as v_min,
  cast(max(reb_l10_avg) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'is_0in2' as column_name,
  count(*) as n_rows,
  sum(case when is_0in2 is null then 1 else 0 end) as n_null,
  round( sum(case when is_0in2 is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct is_0in2) as n_distinct,
  cast(min(is_0in2) as varchar) as v_min,
  cast(max(is_0in2) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'is_b2b' as column_name,
  count(*) as n_rows,
  sum(case when is_b2b is null then 1 else 0 end) as n_null,
  round( sum(case when is_b2b is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct is_b2b) as n_distinct,
  cast(min(is_b2b) as varchar) as v_min,
  cast(max(is_b2b) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'is_3in4' as column_name,
  count(*) as n_rows,
  sum(case when is_3in4 is null then 1 else 0 end) as n_null,
  round( sum(case when is_3in4 is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct is_3in4) as n_distinct,
  cast(min(is_3in4) as varchar) as v_min,
  cast(max(is_3in4) as varchar) as v_max
from silver__team_schedule_features
union all
select
  'silver__team_schedule_features' as table_name,
  'is_4in6' as column_name,
  count(*) as n_rows,
  sum(case when is_4in6 is null then 1 else 0 end) as n_null,
  round( sum(case when is_4in6 is null then 1 else 0 end) * 100.0 / nullif(count(*),0), 3) as pct_null,
  count(distinct is_4in6) as n_distinct,
  cast(min(is_4in6) as varchar) as v_min,
  cast(max(is_4in6) as varchar) as v_max
from silver__team_schedule_features;