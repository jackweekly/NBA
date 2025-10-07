from __future__ import annotations

import pandas as pd
import pytest

from nba_db import extract


@pytest.mark.network
def test_real_league_log_single_day():
    start = end = pd.Timestamp('2024-04-10').date()
    try:
        frame = extract.get_league_game_log_from_date(start, end)
    except Exception as exc:  # noqa: BLE001 - network error fallback
        pytest.skip(f"NBA stats API unavailable: {exc}")
    assert not frame.empty
    assert 'game_id' in frame.columns


@pytest.mark.network
def test_real_box_score_and_pbp():
    game_id = '0022301159'  # 2024-04-10 CHA @ ATL
    try:
        team_box, player_box = extract.get_box_score(game_id)
        pbp = extract.get_play_by_play(game_id)
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"NBA stats API unavailable: {exc}")

    assert not team_box.empty
    assert not player_box.empty
    assert not pbp.empty
    assert set(team_box.columns) >= {'team_id', 'game_id'}
    assert set(player_box.columns) >= {'player_id', 'game_id'}
    assert set(pbp.columns) >= {'game_id', 'eventnum'}
