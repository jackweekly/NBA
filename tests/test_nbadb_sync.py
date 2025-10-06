from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")

from nbapredictor import nbadb_sync


def test_season_for_date_regular_year():
    assert nbadb_sync._season_for_date(date(2024, 12, 25)) == "2024-25"


def test_season_for_date_before_july():
    assert nbadb_sync._season_for_date(date(2025, 3, 10)) == "2024-25"


def test_default_start_date_historical():
    assert nbadb_sync._default_start_date(date(2025, 3, 5)) == date(1946, 11, 1)


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
    assert summary.processed_seasons == []

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["last_updated"] == "2024-10-02"

    for file_path in summary.downloaded_files:
        assert Path(file_path).exists()


def test_update_raw_data_fetch_all_history(tmp_path, monkeypatch):
    seasons_requested: list[str] = []

    def fake_fetch_game_logs_for_season(season: str) -> pd.DataFrame:
        seasons_requested.append(season)
        return pd.DataFrame(
            {
                "SEASON_ID": ["2"],
                "GAME_ID": ["0001"],
            }
        )

    monkeypatch.setattr(
        nbadb_sync,
        "_fetch_game_logs_for_season",
        fake_fetch_game_logs_for_season,
    )
    monkeypatch.setattr(
        nbadb_sync,
        "fetch_dataset_metadata",
        lambda session=None: {"title": "NBA Database"},
    )

    summary = nbadb_sync.update_raw_data(
        output_dir=tmp_path,
        end_date="1948-06-30",
        fetch_all_history=True,
    )

    assert seasons_requested == ["1946-47", "1947-48"]
    assert summary.processed_seasons == ["1946-47", "1947-48"]
    assert summary.downloaded_files == [
        tmp_path / "leaguegamelog" / "season=1946-47" / "part-000.csv",
        tmp_path / "leaguegamelog" / "season=1947-48" / "part-000.csv",
    ]

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["historical_seasons"] == ["1946-47", "1947-48"]
    assert manifest["last_updated"] == "1948-06-30"

    for file_path in summary.downloaded_files:
        assert file_path.exists()


def test_bootstrap_kaggle_imports_dataset(tmp_path, monkeypatch):
    dataset_root = tmp_path / "external" / "wyatt"
    dataset_root.mkdir(parents=True)

    pd.DataFrame(
        {
            "GAME_ID": ["0001", "0002", "0003"],
            "GAME_DATE": ["2023-10-05", "2024-06-12", "2024-10-26"],
            "TEAM_ID": ["A", "B", "C"],
        }
    ).to_csv(dataset_root / "game.csv", index=False)

    monkeypatch.setattr(nbadb_sync, "bootstrap_kaggle_dump", lambda destination: dataset_root)
    monkeypatch.setattr(
        nbadb_sync,
        "fetch_dataset_metadata",
        lambda session=None: {"title": "NBA Database"},
    )

    raw_dir = tmp_path / "raw"
    summary = nbadb_sync.update_raw_data(
        output_dir=raw_dir,
        bootstrap_kaggle=True,
        end_date="2024-10-26",
    )

    expected_files = {
        raw_dir / "leaguegamelog" / "season=2023-24" / "part-000.csv",
        raw_dir / "leaguegamelog" / "season=2024-25" / "part-000.csv",
    }
    assert expected_files.issubset(set(summary.downloaded_files))
    assert summary.processed_dates == []
    assert set(summary.processed_seasons) <= {"2023-24", "2024-25"}

    manifest = json.loads((raw_dir / "manifest.json").read_text())
    assert manifest["last_updated"] == "2024-10-26"
    assert set(manifest["historical_seasons"]) >= {"2023-24", "2024-25"}
    assert manifest["bootstrap"]["dataset"] == nbadb_sync.KAGGLE_DATASET_ID
