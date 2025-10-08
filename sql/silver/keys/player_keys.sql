-- silver__player_keys
-- Source: bronze_player (observed columns: id, first_name, last_name)
create or replace table silver__player_keys as
with base as (
  select
    cast(id as int) as player_id,
    lower(trim(coalesce(first_name,'') || ' ' || coalesce(last_name,''))) as player_name_lc
  from bronze_player
)
select player_id, player_name_lc
from base
where player_id is not null
qualify row_number() over (partition by player_id) = 1;