CREATE OR REPLACE TABLE silver__player_games as
with src as (
  select
    -- identity
    try_cast(game_id   as bigint) as game_id,
    try_cast(player_id as int)    as player_id,
    try_cast(team_id   as int)    as team_id,

    -- minutes string like '34:21' -> decimal minutes
    cast(min as varchar)          as minutes_raw,

    -- counting stats (try_cast keeps us resilient to odd strings)
    try_cast(fgm as int)  as fgm,
    try_cast(fga as int)  as fga,
    try_cast(fg3m as int) as fg3m,
    try_cast(fg3a as int) as fg3a,
    try_cast(ftm as int)  as ftm,
    try_cast(fta as int)  as fta,
    try_cast(oreb as int) as oreb,
    try_cast(dreb as int) as dreb,
    try_cast(ast  as int) as ast,
    try_cast(stl  as int) as stl,
    try_cast(blk  as int) as blk,
    try_cast(tov  as int) as tov,
    try_cast(pf   as int) as pf,
    try_cast(pts  as int) as pts
  from bronze_box_score
  where game_id is not null and player_id is not null and team_id is not null
),
min_parsed as (
  select
    *,
    -- robust minutes parser: 'MM:SS' -> decimal minutes; NULL-safe
    case
      when minutes_raw is null then null
      when strpos(minutes_raw, ':') > 0 then
        try_cast(split_part(minutes_raw, ':', 1) as double)
        + try_cast(split_part(minutes_raw, ':', 2) as double)/60.0
      else try_cast(minutes_raw as double)
    end as minutes
  from src
),
dedup as (
  -- If feed ever contains multiple rows per player-game, collapse by sum
  select
    game_id, player_id, team_id,
    max(minutes)            as minutes,     -- if duplicates, keep max minutes reported
    sum(fgm) as fgm, sum(fga) as fga,
    sum(fg3m) as fg3m, sum(fg3a) as fg3a,
    sum(ftm) as ftm, sum(fta) as fta,
    sum(oreb) as oreb, sum(dreb) as dreb,
    sum(ast) as ast, sum(stl) as stl, sum(blk) as blk,
    sum(tov) as tov, sum(pf) as pf, sum(pts) as pts
  from min_parsed
  group by 1,2,3
)
select
  d.season,
  d.season_type,
  d.game_date_local,
  x.game_id, x.player_id, x.team_id,
  -- minutes as DOUBLE (decimal minutes); easy to convert later if you want integer/floor
  x.minutes,
  x.pts, x.fgm, x.fga, x.fg3m as fg3m, x.fg3a as fg3a,
  x.ftm, x.fta, x.oreb, x.dreb, x.ast, x.stl, x.blk, x.tov, x.pf
from dedup x
left join silver__dim_games_union d
  using (game_id);