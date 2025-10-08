create or replace table silver__feature_quality as
select
  'pts_l5_avg'   as feature, 1 - avg((pts_l5_avg is null)::int)   as completeness
from silver__team_schedule_features
union all
select 'tpm_l10_avg', 1 - avg((tpm_l10_avg is null)::int)
from silver__team_schedule_features
union all
select 'reb_l10_avg', 1 - avg((reb_l10_avg is null)::int)
from silver__team_schedule_features
union all
select 'rest_bucket', 1 - avg((rest_bucket is null)::int)
from silver__team_schedule_features;