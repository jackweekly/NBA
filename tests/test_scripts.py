from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from nba_db import paths, update as nba_update

import run_daily_update


@pytest.fixture(autouse=True)
def _override_paths(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    game_csv = raw_dir / "game.csv"
    raw_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paths, "RAW_DIR", raw_dir, raising=False)
    monkeypatch.setattr(paths, "GAME_CSV", game_csv, raising=False)
    monkeypatch.setattr(nba_update, "RAW_DIR", raw_dir, raising=False)
    monkeypatch.setattr(nba_update, "GAME_CSV", game_csv, raising=False)
    yield


def test_build_parser_supports_overrides():
    parser = run_daily_update._build_parser()
    args = parser.parse_args(["--start-date", "2020-01-01", "--end-date", "2020-01-07"])
    assert args.start_date == "2020-01-01"
    assert args.end_date == "2020-01-07"


def test_run_daily_update_calls_update(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_daily(*, start_date=None, end_date=None):
        calls.append({"start_date": start_date, "end_date": end_date})
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, True, 5)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    exit_code = run_daily_update.main(["--start-date", "2020-01-02"])

    assert exit_code == 0
    assert calls == [{"start_date": "2020-01-02", "end_date": None}]


def test_run_daily_update_sets_numeric_environment(monkeypatch):
    def fake_daily(*, start_date=None, end_date=None):  # noqa: ARG001
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, True, 0)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    for key in run_daily_update._NUMERIC_ENV_DEFAULTS:
        monkeypatch.delenv(key, raising=False)

    exit_code = run_daily_update.main([])

    assert exit_code == 0
    for key, value in run_daily_update._NUMERIC_ENV_DEFAULTS.items():
        assert os.environ[key] == value


def test_daily_result_written(tmp_path):
    # Ensure atomic writer is reachable from script tests
    frame = pd.DataFrame({"game_id": ["0001"], "team_id": ["T1"], "season_type": ["Regular Season"], "game_date": ["2020-01-01"]})
    nba_update._atomic_write_csv(nba_update._canonicalise(frame), nba_update.GAME_CSV)
    saved = pd.read_csv(nba_update.GAME_CSV)
    assert len(saved) == 1
