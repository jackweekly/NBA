-- Fail the build if impossible combos exist
create or replace table silver__season_type_assert as
select
  count(*) as bad_rows
from silver__dim_games_union
where (strftime('%m', game_date_local) in ('05','06','07') and season_type='REG')
   or (strftime('%m', game_date_local) in ('09','10') and season_type='POST');