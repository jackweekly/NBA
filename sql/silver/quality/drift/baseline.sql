-- Determine latest finished season in REG data
create or replace table silver__helper_latest_season as
select max(season) as latest_season
from silver__team_games
where season_type='REG';

-- Baseline seasons = latest_season and latest_season-1
create or replace table silver__drift_baseline as
with ls as (select latest_season from silver__helper_latest_season),
base as (
  select * 
  from silver__team_games
  where season_type='REG'
    and season in ( (select latest_season from ls) - 1,
                    (select latest_season from ls) )
)
select
  -- league-level distribution over the baseline window
  avg(pts)  as mean_pts,  stddev(pts)  as sd_pts,
  avg(fga)  as mean_fga,  stddev(fga)  as sd_fga,
  avg(tpm)  as mean_tpm,  stddev(tpm)  as sd_tpm,
  avg(fta)  as mean_fta,  stddev(fta)  as sd_fta,
  avg(oreb + dreb) as mean_reb, stddev(oreb + dreb) as sd_reb,
  avg(tov)  as mean_tov,  stddev(tov)  as sd_tov,
  avg(pf)   as mean_pf,   stddev(pf)   as sd_pf
from base;

-- Baseline bins (deciles) for PSI; compute cutpoints via quantiles
create or replace table silver__drift_bins as
with ls as (select latest_season from silver__helper_latest_season),
base as (
  select * 
  from silver__team_games
  where season_type='REG'
    and season in ( (select latest_season from ls) - 1,
                    (select latest_season from ls) )
),
q as (
  select
    quantile_cont(pts,  [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_pts,
    quantile_cont(fga,  [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_fga,
    quantile_cont(tpm,  [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_tpm,
    quantile_cont(fta,  [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_fta,
    quantile_cont(oreb + dreb, [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_reb,
    quantile_cont(tov,  [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_tov,
    quantile_cont(pf,   [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9]) as q_pf
  from base
)
select * from q;