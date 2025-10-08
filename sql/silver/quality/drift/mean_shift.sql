-- Join monthly league means to baseline; flag 3-sigma shifts
create or replace table silver__drift_mean_shift as
with b as (select * from silver__drift_baseline),
m as (select * from silver__drift_monthly_league)
select
  m.ym,
  (m.mean_pts  - b.mean_pts  ) / nullif(b.sd_pts,  0) as z_pts,
  (m.mean_fga  - b.mean_fga  ) / nullif(b.sd_fga,  0) as z_fga,
  (m.mean_tpm  - b.mean_tpm  ) / nullif(b.sd_tpm,  0) as z_tpm,
  (m.mean_fta  - b.mean_fta  ) / nullif(b.sd_fta,  0) as z_fta,
  (m.mean_reb  - b.mean_reb  ) / nullif(b.sd_reb,  0) as z_reb,
  (m.mean_tov  - b.mean_tov  ) / nullif(b.sd_tov,  0) as z_tov,
  (m.mean_pf   - b.mean_pf   ) / nullif(b.sd_pf,   0) as z_pf,
  -- simple boolean flags
  abs((m.mean_pts  - b.mean_pts  ) / nullif(b.sd_pts,  0)) >= 3 as flag_pts_3sigma,
  abs((m.mean_fga  - b.mean_fga  ) / nullif(b.sd_fga,  0)) >= 3 as flag_fga_3sigma,
  abs((m.mean_tpm  - b.mean_tpm  ) / nullif(b.sd_tpm,  0)) >= 3 as flag_tpm_3sigma,
  abs((m.mean_fta  - b.mean_fta  ) / nullif(b.sd_fta,  0)) >= 3 as flag_fta_3sigma,
  abs((m.mean_reb  - b.mean_reb  ) / nullif(b.sd_reb,  0)) >= 3 as flag_reb_3sigma,
  abs((m.mean_tov  - b.mean_tov  ) / nullif(b.sd_tov,  0)) >= 3 as flag_tov_3sigma,
  abs((m.mean_pf   - b.mean_pf   ) / nullif(b.sd_pf,   0)) >= 3 as flag_pf_3sigma
from m, b;