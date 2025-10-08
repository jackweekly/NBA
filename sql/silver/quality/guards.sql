-- Fail rows that violate simple physical constraints; make a view of "clean only"
create or replace view silver__team_games_clean as
select *
from silver__team_games
where
  fgm is null or fga is null or fgm <= fga
and tpm is null or tpa is null or tpm <= tpa
and fgm is null or tpm is null or fgm >= tpm
and fga is null or fta is null or fga >= 0 and fta >= 0
and pts is null or pts >= 0
and oreb is null or dreb is null or (oreb >= 0 and dreb >= 0)
;