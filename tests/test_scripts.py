from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from nba_db import update as nba_update

import run_daily_update


@pytest.fixture(autouse=True)
def _set_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("raw:\n  raw_dir: data/raw\n", encoding="utf-8")
    monkeypatch.setattr(nba_update, "_project_root", lambda: tmp_path)
    yield


def test_daily_fetch_all_history(monkeypatch, tmp_path):
    frame = pd.DataFrame({"GAME_ID": ["0001"], "GAME_DATE": ["2020-01-01"]})
    records: list[dict[str, object]] = []

    def fake_get_all():
        records.append({"called": True})
        return frame

    monkeypatch.setattr(nba_update, "get_league_game_log_all", fake_get_all)

    result = nba_update.daily(fetch_all_history=True)

    assert records == [{"called": True}]
    assert result.output_path == tmp_path / "data/raw/game.csv"
    saved = pd.read_csv(result.output_path)
    expected = frame.rename(columns=str.lower)
    saved["game_id"] = saved["game_id"].astype(int)
    expected["game_id"] = expected["game_id"].astype(int)
    pd.testing.assert_frame_equal(saved, expected, check_dtype=False)


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([], (False, None)),
        (["--fetch-all-history"], (True, None)),
        (["2020-10-01"], (False, "2020-10-01")),
    ],
)
def test_parse_args(argv, expected):
    assert run_daily_update._parse_args(argv) == expected


def test_run_daily_update_calls_daily(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_daily(*, fetch_all_history=False, start_date=None, **kwargs):
        calls.append({
            "fetch_all_history": fetch_all_history,
            "start_date": start_date,
        })
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, False)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    run_daily_update.main(["--fetch-all-history"])

    assert calls == [{"fetch_all_history": True, "start_date": None}]


def test_run_daily_update_sets_numeric_environment(monkeypatch):
    def fake_daily(**kwargs):  # noqa: ARG001 - behaviour under test
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, False)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    for key in run_daily_update._NUMERIC_ENV_DEFAULTS:
        monkeypatch.delenv(key, raising=False)

    run_daily_update.main([])

    for key, value in run_daily_update._NUMERIC_ENV_DEFAULTS.items():
        assert os.environ[key] == value
