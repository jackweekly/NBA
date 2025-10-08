create or replace table silver__season_type_overrides as
select
  game_id,
  case
    when strftime('%m', game_date_local) in ('05','06','07') then 'POST'
    when strftime('%m', game_date_local) in ('09','10')     then 'REG'
    else season_type
  end as season_type_new
from silver__dim_games_union
where (strftime('%m', game_date_local) in ('05','06','07') and season_type='REG')
   or (strftime('%m', game_date_local) in ('09','10') and season_type='POST');