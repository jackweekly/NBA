from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from nba_db import paths, update


def _override_paths(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    game_csv = raw_dir / "game.csv"
    raw_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(paths, "RAW_DIR", raw_dir, raising=False)
    monkeypatch.setattr(paths, "GAME_CSV", game_csv, raising=False)
    monkeypatch.setattr(update, "RAW_DIR", raw_dir, raising=False)
    monkeypatch.setattr(update, "GAME_CSV", game_csv, raising=False)


def test_daily_appends_new_rows(monkeypatch, tmp_path):
    _override_paths(tmp_path, monkeypatch)

    existing = pd.DataFrame(
        {
            "game_id": ["0001"],
            "team_id": ["T1"],
            "season_type": ["Regular Season"],
            "game_date": ["2020-01-01"],
        }
    )
    update._atomic_write_csv(update._canonicalise(existing), update.GAME_CSV)

    new_rows = pd.DataFrame(
        {
            "GAME_ID": ["0002"],
            "TEAM_ID": ["T2"],
            "SEASON_TYPE": ["Playoffs"],
            "GAME_DATE": ["2020-01-02"],
        }
    )

    calls: list[tuple[date, date | None]] = []

    def fake_fetch(start: date, end: date | None = None):  # noqa: ARG001
        calls.append((start, end))
        return new_rows

    monkeypatch.setattr(update.extract, "get_league_game_log_from_date", fake_fetch)

    result = update.daily()

    saved = pd.read_csv(update.GAME_CSV)
    assert len(saved) == 2
    assert set(saved.columns) >= {"game_id", "team_id", "season_type", "game_date"}
    assert result.rows_written == 1
    assert result.final_row_count == 2
    assert calls == [(date(2020, 1, 2), None)]


def test_daily_no_new_games_logs_zero(monkeypatch, tmp_path, caplog):
    _override_paths(tmp_path, monkeypatch)

    existing = pd.DataFrame(
        {
            "game_id": ["0001"],
            "team_id": ["T1"],
            "season_type": ["Regular Season"],
            "game_date": ["2020-01-01"],
        }
    )
    update._atomic_write_csv(update._canonicalise(existing), update.GAME_CSV)

    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        lambda start, end=None: pd.DataFrame(),
    )

    with caplog.at_level("INFO"):
        result = update.daily()

    assert result.rows_written == 0
    assert "returned no games" in " ".join(caplog.messages)


def test_daily_requires_existing_seed(monkeypatch, tmp_path):
    _override_paths(tmp_path, monkeypatch)

    with pytest.raises(FileNotFoundError):
        update.daily()


def test_daily_start_date_override(monkeypatch, tmp_path):
    _override_paths(tmp_path, monkeypatch)

    existing = pd.DataFrame(
        {
            "game_id": ["0001"],
            "team_id": ["T1"],
            "season_type": ["Regular Season"],
            "game_date": ["2020-01-01"],
        }
    )
    update._atomic_write_csv(update._canonicalise(existing), update.GAME_CSV)

    calls: list[tuple[date, date | None]] = []

    def fake_fetch(start: date, end: date | None = None):  # noqa: ARG001
        calls.append((start, end))
        return pd.DataFrame(
            {
                "game_id": ["0002"],
                "team_id": ["T2"],
                "season_type": ["Regular Season"],
                "game_date": ["2020-01-03"],
            }
        )

    monkeypatch.setattr(update.extract, "get_league_game_log_from_date", fake_fetch)

    update.daily(start_date="2020-01-10", end_date="2020-01-12")

    assert calls == [(date(2020, 1, 10), date(2020, 1, 12))]


def test_daily_deduplicates(monkeypatch, tmp_path):
    _override_paths(tmp_path, monkeypatch)

    existing = pd.DataFrame(
        {
            "game_id": ["0001"],
            "team_id": ["T1"],
            "season_type": ["Regular Season"],
            "game_date": ["2020-01-01"],
        }
    )
    update._atomic_write_csv(update._canonicalise(existing), update.GAME_CSV)

    duplicate = pd.DataFrame(
        {
            "game_id": ["0001"],
            "team_id": ["T1"],
            "season_type": ["Regular Season"],
            "game_date": ["2020-01-01"],
        }
    )

    monkeypatch.setattr(update.extract, "get_league_game_log_from_date", lambda start, end=None: duplicate)

    result = update.daily()

    saved = pd.read_csv(update.GAME_CSV)
    assert len(saved) == 1
    assert result.rows_written == 0
