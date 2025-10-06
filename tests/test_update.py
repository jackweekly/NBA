from __future__ import annotations

from datetime import date
import types

from requests import exceptions as requests_exceptions

import pandas as pd
import pytest

from nba_db import update


def _setup_config(tmp_path, monkeypatch):
    config = tmp_path / "config.yaml"
    config.write_text("raw:\n  raw_dir: data/raw\n", encoding="utf-8")
    monkeypatch.setattr(update, "_project_root", lambda: tmp_path)


def test_daily_incremental_appends(monkeypatch, tmp_path):
    _setup_config(tmp_path, monkeypatch)
    game_csv = tmp_path / "data/raw/game.csv"
    game_csv.parent.mkdir(parents=True, exist_ok=True)
    existing = pd.DataFrame({
        "GAME_ID": ["0001"],
        "GAME_DATE": ["2020-01-01"],
    })
    existing.to_csv(game_csv, index=False)

    new_frame = pd.DataFrame({
        "GAME_ID": ["0002"],
        "GAME_DATE": ["2020-01-02"],
    })

    def fake_get_from_date(start: date, end_date=None):  # noqa: ARG001
        return new_frame

    monkeypatch.setattr(update, "get_league_game_log_from_date", fake_get_from_date)

    result = update.daily(start_date="2020-01-02")

    saved = pd.read_csv(game_csv)
    assert len(saved) == 2
    assert result.appended is True
    assert result.rows_written == len(new_frame)


def test_daily_without_history_file_raises(monkeypatch, tmp_path):
    _setup_config(tmp_path, monkeypatch)
    with pytest.raises(FileNotFoundError):
        update.daily()


def test_init_writes_all_resources(monkeypatch, tmp_path):
    _setup_config(tmp_path, monkeypatch)

    players = pd.DataFrame({"PLAYER_ID": [1]})
    teams = pd.DataFrame({"TEAM_ID": [2]})
    games = pd.DataFrame({"GAME_ID": ["0001"], "GAME_DATE": ["2020-01-01"]})
    summaries = pd.DataFrame({"GAME_ID": ["0001"], "DETAIL": ["sample"]})

    monkeypatch.setattr(update, "_fetch_all_players", lambda: players)
    monkeypatch.setattr(update, "_fetch_all_teams", lambda: teams)
    monkeypatch.setattr(update, "get_league_game_log_all", lambda: games)
    monkeypatch.setattr(update, "_fetch_game_summaries", lambda ids: summaries)

    result = update.init()

    assert result.row_counts == {
        "players": 1,
        "teams": 1,
        "games": 1,
        "game_summaries": 1,
    }

    assert (tmp_path / "data/raw/common_player_info.csv").exists()
    assert (tmp_path / "data/raw/team_info_common.csv").exists()
    assert (tmp_path / "data/raw/game.csv").exists()
    assert (tmp_path / "data/raw/game_summary.csv").exists()


def test_get_league_game_log_from_date_bounds(monkeypatch):
    calls: list[tuple[str, str, str]] = []

    class FakeEndpoint:
        def __init__(self, *, season, season_type_all_star, date_from_nullable, date_to_nullable, **_):
            calls.append((season, season_type_all_star, date_from_nullable, date_to_nullable))

        def get_data_frames(self):
            return [pd.DataFrame({"GAME_ID": ["0001"]})]

    monkeypatch.setattr(update, "leaguegamelog", types.SimpleNamespace(LeagueGameLog=FakeEndpoint))

    start = date(2020, 8, 1)
    end = date(2021, 1, 1)
    frame = update.get_league_game_log_from_date(start, end)

    assert not frame.empty
    assert any(season == "2020-21" for season, *_ in calls)


def test_call_with_retry_retries_then_succeeds(monkeypatch):
    attempts = 0
    sleeps: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleeps.append(seconds)

    timeouts_seen: list[int | float] = []

    def flaky_call(timeout):
        timeouts_seen.append(timeout)
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise requests_exceptions.ReadTimeout("timeout", request=None)
        return pd.DataFrame({"value": [1]})

    monkeypatch.setattr(update.time, "sleep", fake_sleep)

    frame = update._call_with_retry("example", flaky_call)

    assert attempts == 3
    assert sleeps == [1, 2]
    assert timeouts_seen == [update.DEFAULT_TIMEOUT, update.DEFAULT_TIMEOUT, update.DEFAULT_TIMEOUT]
    assert not frame.empty


def test_call_with_retry_exhausts_attempts(monkeypatch):
    attempts = 0
    sleeps: list[int] = []

    def fake_sleep(seconds: int) -> None:
        sleeps.append(seconds)

    def failing_call(timeout):
        nonlocal attempts
        attempts += 1
        raise requests_exceptions.Timeout("timeout", request=None)

    monkeypatch.setattr(update.time, "sleep", fake_sleep)

    with pytest.raises(RuntimeError) as excinfo:
        update._call_with_retry("player", failing_call)

    assert "Failed to fetch player" in str(excinfo.value)
    assert attempts == update.MAX_REQUEST_RETRIES
    assert sleeps == [1, 2, 4, 8]


def test_call_with_retry_respects_timeout_plan(monkeypatch):
    attempts = 0
    seen_timeouts: list[int | float] = []

    def fake_sleep(seconds: int) -> None:
        pass

    def failing_call(timeout):
        nonlocal attempts
        attempts += 1
        seen_timeouts.append(timeout)
        raise requests_exceptions.ConnectTimeout("timeout", request=None)

    monkeypatch.setattr(update.time, "sleep", fake_sleep)

    custom_timeouts = (1, 2, 4)

    with pytest.raises(RuntimeError):
        update._call_with_retry("custom", failing_call, timeouts=custom_timeouts)

    assert attempts == update.MAX_REQUEST_RETRIES
    assert seen_timeouts == [1, 2, 4, 4, 4]
