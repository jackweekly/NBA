from __future__ import annotations

import os

import pytest

from nba_db import update as nba_update
from nbapredictor import nbadb_sync

import run_daily_update


def test_daily_fetch_all_history(monkeypatch):
    captured: list[dict[str, object]] = []

    def fake_update_raw_data(**kwargs):
        captured.append(kwargs)
        return nbadb_sync.UpdateSummary([], [], [], [])

    monkeypatch.setattr(nbadb_sync, "update_raw_data", fake_update_raw_data)

    result = nba_update.daily(fetch_all_history=True, output_dir="/tmp/output")

    assert captured == [
        {
            "output_dir": "/tmp/output",
            "start_date": nbadb_sync.HISTORICAL_START_DATE.isoformat(),
            "end_date": None,
            "bootstrap_kaggle": False,
            "force": False,
            "fetch_all_history": True,
        }
    ]
    assert result.summary.downloaded_files == []


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

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    run_daily_update.main(["--fetch-all-history"])

    assert calls == [{"fetch_all_history": True, "start_date": None}]


def test_run_daily_update_sets_numeric_environment(monkeypatch):
    def fake_daily(**kwargs):  # noqa: ARG001 - behaviour under test
        return None

    monkeypatch.setattr(nba_update, "daily", fake_daily)

    for key in run_daily_update._NUMERIC_ENV_DEFAULTS:
        monkeypatch.delenv(key, raising=False)

    run_daily_update.main([])

    for key, value in run_daily_update._NUMERIC_ENV_DEFAULTS.items():
        assert os.environ[key] == value
