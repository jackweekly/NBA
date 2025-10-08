-- Heuristic month map:
-- PRE : Sep (09)–Oct (10) before regular season tips (historically some Sep scrimmages)
-- REG : Oct (10)–Apr (04)
-- POST: Apr (04)–Jun (06)
-- JUL/AUG: treat as POST if legacy has playoffs; else leave as is (rare in NBA proper; Summer League is different comp)

create or replace table silver__dim_games_legacy_fixed as
with src as (
  select *
  from silver__dim_games                -- legacy table
),
fixed as (
  select
    game_id,
    game_date_local,
    home_team_id,
    away_team_id,
    season,
    case
      when strftime('%m', game_date_local) in ('09') then 'PRE'
      when strftime('%m', game_date_local) in ('10','11','12','01','02','03') then 'REG'
      when strftime('%m', game_date_local) in ('04') then
        -- April can be REG or POST; prefer explicit label if already POST, else REG
        case when season_type='POST' then 'POST' else 'REG' end
      when strftime('%m', game_date_local) in ('05','06') then 'POST'
      when strftime('%m', game_date_local) in ('07','08') then
        coalesce(season_type, 'POST')  -- safest default for rare legacy rows
      else coalesce(season_type,'REG')
    end as season_type
  from src
)
select * from fixed;