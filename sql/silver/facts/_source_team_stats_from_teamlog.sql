-- Team totals per (game_id, team_id) sourced from team logs; columns aligned to neutral names
create or replace table silver__source_team_stats as
select
  cast(game_id as bigint)           as game_id,
  try_cast(team_id as int)          as team_id,
  try_cast(pts  as int)             as pts,
  try_cast(fgm  as int)             as fgm,
  try_cast(fga  as int)             as fga,
  try_cast(fg3m as int)             as tpm,
  try_cast(fg3a as int)             as tpa,
  try_cast(ftm  as int)             as ftm,
  try_cast(fta  as int)             as fta,
  try_cast(oreb as int)             as oreb,
  try_cast(dreb as int)             as dreb,
  try_cast(ast  as int)             as ast,
  try_cast(stl  as int)             as stl,
  try_cast(blk  as int)             as blk,
  try_cast(tov  as int)             as tov,
  try_cast(pf   as int)             as pf
from bronze_game_log_team;