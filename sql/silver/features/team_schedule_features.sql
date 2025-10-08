create or replace table silver__team_schedule_features as
with tg as (select * from silver__team_games),
ordered as (
  select
    *,
    row_number() over (partition by team_id, season order by game_date_local) as gnum,
    lag(game_date_local) over (partition by team_id, season order by game_date_local) as prev_date
  from tg
),
rested as (
  select
    *,
    datediff('day', prev_date, game_date_local) as days_rest,
    case when days_rest = 0 then '0D' when days_rest = 1 then 'B2B'
         when days_rest between 2 and 3 then '2-3D'
         when days_rest between 4 and 6 then '4-6D'
         else '7D+' end as rest_bucket
  from ordered
),
rolling as (
  select
    *,
    avg(pts) over w  as pts_l5_avg,
    avg(tov) over w  as tov_l5_avg,
    avg(tpm) over w10 as tpm_l10_avg,
    avg(oreb + dreb) over w10 as reb_l10_avg
  from rested
  window w  as (partition by team_id order by game_date_local rows between 5 preceding  and 1 preceding),
         w10 as (partition by team_id order by game_date_local rows between 10 preceding and 1 preceding)
),
b2bflags as (
  select
    *,
    (days_rest = 0) as is_0in2,
    (days_rest = 1) as is_b2b,
    (days_rest <= 2 and gnum >= 3
      and game_date_local - lag(game_date_local,2) over (partition by team_id, season order by game_date_local) <= 4) as is_3in4,
    (days_rest <= 2 and gnum >= 4
      and game_date_local - lag(game_date_local,3) over (partition by team_id, season order by game_date_local) <= 6) as is_4in6
  from rolling
)
select * from b2bflags;