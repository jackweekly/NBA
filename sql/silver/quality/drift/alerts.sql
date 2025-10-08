create or replace view silver__drift_alerts as
with z as (select * from silver__drift_mean_shift),
p as (select * from silver__psi_monthly)
select
  z.ym,
  -- 3-sigma flags (mean-shift)
  (flag_pts_3sigma or flag_fga_3sigma or flag_tpm_3sigma or flag_fta_3sigma
   or flag_reb_3sigma or flag_tov_3sigma or flag_pf_3sigma)          as any_3sigma_flag,

  -- PSI flags aggregated per month
  max(case when p.metric='pts' and p.psi>0.25 then 1 else 0 end) as psi_pts_major,
  max(case when p.metric='fga' and p.psi>0.25 then 1 else 0 end) as psi_fga_major,
  max(case when p.metric='tpm' and p.psi>0.25 then 1 else 0 end) as psi_tpm_major,
  max(case when p.metric='fta' and p.psi>0.25 then 1 else 0 end) as psi_fta_major,
  max(case when p.metric='reb' and p.psi>0.25 then 1 else 0 end) as psi_reb_major,
  max(case when p.metric='tov' and p.psi>0.25 then 1 else 0 end) as psi_tov_major,
  max(case when p.metric='pf'  and p.psi>0.25 then 1 else 0 end) as psi_pf_major
from z
left join p on p.ym = z.ym
group by z.ym, any_3sigma_flag
order by z.ym desc;