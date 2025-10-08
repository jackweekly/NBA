-- Monthly aggregates on REG season only
create or replace table silver__drift_monthly as
with g as (
  select
    date_trunc('month', game_date_local)::date as ym,
    season,
    team_id,
    pts, fga, tpm, fta, tov, pf, (oreb + dreb) as reb
  from silver__team_games
  where season_type = 'REG'
),
f as (
  select
    date_trunc('month', game_date_local)::date as ym,
    season,
    team_id,
    pts_l5_avg, tpm_l10_avg, reb_l10_avg
  from silver__team_schedule_features
)
select
  coalesce(g.ym, f.ym) as ym,
  coalesce(g.season, f.season) as season,
  coalesce(g.team_id, f.team_id) as team_id,

  avg(pts)  as mean_pts,
  avg(fga)  as mean_fga,
  avg(tpm)  as mean_tpm,
  avg(fta)  as mean_fta,
  avg(reb)  as mean_reb,
  avg(tov)  as mean_tov,
  avg(pf)   as mean_pf,

  avg(pts_l5_avg)  as mean_pts_l5,
  avg(tpm_l10_avg) as mean_tpm_l10,
  avg(reb_l10_avg) as mean_reb_l10
from g
full join f using (ym, season, team_id)
group by 1,2,3;

-- Collapse to league-level (optional but useful)
create or replace table silver__drift_monthly_league as
select
  ym,
  avg(mean_pts)     as mean_pts,
  avg(mean_fga)     as mean_fga,
  avg(mean_tpm)     as mean_tpm,
  avg(mean_fta)     as mean_fta,
  avg(mean_reb)     as mean_reb,
  avg(mean_tov)     as mean_tov,
  avg(mean_pf)      as mean_pf,
  avg(mean_pts_l5)  as mean_pts_l5,
  avg(mean_tpm_l10) as mean_tpm_l10,
  avg(mean_reb_l10) as mean_reb_l10
from silver__drift_monthly
group by 1;