create or replace table silver__missing_profile as
select * from silver__missing_profile_dim_games_union
union all
select * from silver__missing_profile_team_games
union all
select * from silver__missing_profile_team_schedule_features
order by table_name, pct_null desc, column_name;