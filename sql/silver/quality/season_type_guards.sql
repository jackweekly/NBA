-- Flag impossible REG months
create or replace table silver__season_type_impossible as
select
  game_id, game_date_local, season_type
from silver__dim_games_union
where (strftime('%m', game_date_local) in ('05','06','07') and season_type='REG')
   or (strftime('%m', game_date_local) in ('09','10') and season_type='POST');