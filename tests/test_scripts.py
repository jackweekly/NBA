from __future__ import annotations

import os
from pathlib import Path

import pandas as pd
import pytest

from nba_db import paths
from nba_db import update as nba_update

import run_daily_update


@pytest.fixture(autouse=True)
def _set_config(tmp_path, monkeypatch):
    config_path = tmp_path / "config.yaml"
    config_path.write_text("raw:\n  raw_dir: data/raw\n", encoding="utf-8")
    monkeypatch.setattr(paths, "project_root", lambda: tmp_path)
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
    saved.columns = saved.columns.str.lower()
    expected = frame.rename(columns=str.lower)
    expected["season_type"] = "Regular Season"
    saved["game_id"] = saved["game_id"].astype(int)
    expected["game_id"] = expected["game_id"].astype(int)
    pd.testing.assert_frame_equal(saved[expected.columns], expected, check_dtype=False)
    assert result.final_row_count == len(expected)


@pytest.mark.parametrize(
    "argv, expected",
    [
        ([], {"start_date": None, "fetch_all_history": False, "end_date": None}),
        (["--fetch-all-history"], {"start_date": None, "fetch_all_history": True, "end_date": None}),
        (["2020-10-01", "--end-date", "2020-10-31"], {"start_date": "2020-10-01", "fetch_all_history": False, "end_date": "2020-10-31"}),
    ],
)
def test_build_parser(argv, expected):
    parser = run_daily_update._build_parser()
    args = parser.parse_args(argv)
    assert {"start_date": args.start_date, "fetch_all_history": args.fetch_all_history, "end_date": args.end_date} == expected


def test_run_daily_update_calls_daily(monkeypatch):
    calls: list[dict[str, object]] = []

    def fake_daily(*, fetch_all_history=False, start_date=None, end_date=None, **kwargs):
        calls.append({
            "fetch_all_history": fetch_all_history,
            "start_date": start_date,
            "end_date": end_date,
        })
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, False, 10)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    exit_code = run_daily_update.main(["--fetch-all-history"])

    assert exit_code == 0
    assert calls == [{"fetch_all_history": True, "start_date": None, "end_date": None}]


def test_run_daily_update_sets_numeric_environment(monkeypatch):
    def fake_daily(**kwargs):  # noqa: ARG001 - behaviour under test
        return nba_update.DailyUpdateResult(Path("game.csv"), 0, False, 0)

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    for key in run_daily_update._NUMERIC_ENV_DEFAULTS:
        monkeypatch.delenv(key, raising=False)

    exit_code = run_daily_update.main([])

    assert exit_code == 0

    for key, value in run_daily_update._NUMERIC_ENV_DEFAULTS.items():
        assert os.environ[key] == value
