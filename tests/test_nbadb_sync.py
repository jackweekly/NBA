from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from nbapredictor import nbadb_sync


def test_season_for_date_regular_year():
    assert nbadb_sync._season_for_date(date(2024, 12, 25)) == "2024-25"


def test_season_for_date_before_july():
    assert nbadb_sync._season_for_date(date(2025, 3, 10)) == "2024-25"


def test_default_start_date_october():
    assert nbadb_sync._default_start_date(date(2025, 3, 5)) == date(2024, 10, 1)


def test_update_raw_data_creates_files(tmp_path, monkeypatch):
    calls: list[date] = []

    def fake_fetch_game_logs(target_date: date) -> pd.DataFrame:
        calls.append(target_date)
        return pd.DataFrame(
            {
                "GAME_ID": ["0001"],
                "TEAM_ID": ["1610612737"],
                "GAME_DATE": [target_date.isoformat()],
            }
        )

    monkeypatch.setattr(nbadb_sync, "_fetch_game_logs_for_date", fake_fetch_game_logs)
    monkeypatch.setattr(
        nbadb_sync,
        "fetch_dataset_metadata",
        lambda session=None: {"title": "NBA Database"},
    )

    summary = nbadb_sync.update_raw_data(
        output_dir=tmp_path,
        start_date="2024-10-01",
        end_date="2024-10-02",
    )

    assert len(summary.downloaded_files) == 2
    assert calls == [date(2024, 10, 1), date(2024, 10, 2)]

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["last_updated"] == "2024-10-02"

    for file_path in summary.downloaded_files:
        assert Path(file_path).exists()
