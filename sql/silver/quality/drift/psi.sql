-- Helper: assign decile bin given a value and cutpoint array
create or replace macro _bin10(val, cuts) as
case
  when val is null then null
  when val <= cuts[1] then 1
  when val <= cuts[2] then 2
  when val <= cuts[3] then 3
  when val <= cuts[4] then 4
  when val <= cuts[5] then 5
  when val <= cuts[6] then 6
  when val <= cuts[7] then 7
  when val <= cuts[8] then 8
  when val <= cuts[9] then 9
  else 10
end;

-- Build baseline bin proportions per metric (P)
create or replace table silver__psi_baseline as
with ls as (select latest_season from silver__helper_latest_season),
base as (
  select *
  from silver__team_games
  where season_type='REG'
    and season in ( (select latest_season from ls) - 1,
                    (select latest_season from ls) )
),
cuts as (select * from silver__drift_bins),
binned as (
  select
    _bin10(pts,  cuts.q_pts)  as b_pts,
    _bin10(fga,  cuts.q_fga)  as b_fga,
    _bin10(tpm,  cuts.q_tpm)  as b_tpm,
    _bin10(fta,  cuts.q_fta)  as b_fta,
    _bin10(oreb + dreb, cuts.q_reb) as b_reb,
    _bin10(tov,  cuts.q_tov)  as b_tov,
    _bin10(pf,   cuts.q_pf)   as b_pf
  from base, cuts
)
select
  'pts' as metric, b_pts as bin, count(*) * 1.0 / sum(count(*)) over () as p
from binned group by 1,2
union all
select 'fga', b_fga, count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
union all
select 'tpm', b_tpm, count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
union all
select 'fta', b_fta, count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
union all
select 'reb', b_reb, count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
union all
select 'tov', b_tov, count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
union all
select 'pf',  b_pf,  count(*) * 1.0 / sum(count(*)) over () from binned group by 1,2
;

-- Monthly current bin proportions (Q) and PSI = sum((p-q)*ln(p/q))
create or replace table silver__psi_monthly as
with m as (select * from silver__drift_monthly),  -- team-level monthly rows exist, but we need raw game rows to bin
raw as (
  select
    date_trunc('month', game_date_local)::date as ym,
    pts, fga, tpm, fta, (oreb + dreb) as reb, tov, pf
  from silver__team_games
  where season_type='REG'
),
cuts as (select * from silver__drift_bins),
binned as (
  select
    ym,
    _bin10(pts, cuts.q_pts) as b_pts,
    _bin10(fga, cuts.q_fga) as b_fga,
    _bin10(tpm, cuts.q_tpm) as b_tpm,
    _bin10(fta, cuts.q_fta) as b_fta,
    _bin10(reb, cuts.q_reb) as b_reb,
    _bin10(tov, cuts.q_tov) as b_tov,
    _bin10(pf,  cuts.q_pf)  as b_pf
  from raw, cuts
),
q as (
  select 'pts' as metric, ym, b_pts as bin, count(*)*1.0/sum(count(*)) over (partition by ym) as q
  from binned group by 1,2,3
  union all
  select 'fga', ym, b_fga, count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
  union all
  select 'tpm', ym, b_tpm, count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
  union all
  select 'fta', ym, b_fta, count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
  union all
  select 'reb', ym, b_reb, count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
  union all
  select 'tov', ym, b_tov, count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
  union all
  select 'pf',  ym, b_pf,  count(*)*1.0/sum(count(*)) over (partition by ym) from binned group by 1,2,3
)
select
  q.ym, q.metric,
  sum( (p.p - q.q) * ln( nullif(p.p,0) / nullif(q.q,0) ) ) as psi
from q
join silver__psi_baseline p
  on p.metric = q.metric and p.bin = q.bin
group by q.ym, q.metric
order by q.ym desc, q.metric;