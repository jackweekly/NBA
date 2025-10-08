create or replace table ml__feature_asserts as
select
  -- target presence on recent months
  sum((y_win is null)::int) as null_labels,
  -- leakage check: opponent features present (join coverage)
  sum((pts_l5_avg_opp is null and y_win is not null)::int) as missing_opp_feats_on_labeled
from ml__team_game_features
where game_date_local >= date '2023-10-01';