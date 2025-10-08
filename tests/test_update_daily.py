from __future__ import annotations

import time
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import requests
import pandas as pd
import pytest

from nba_db import update


@pytest.fixture()
def temp_paths_fixture(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    game_csv = raw_dir / "game.csv"
    duckdb_path = tmp_path / "data" / "nba.duckdb"

    monkeypatch.setattr(update, "RAW_DIR", raw_dir)
    monkeypatch.setattr(update, "GAME_CSV", game_csv)
    monkeypatch.setattr(update, "DUCKDB_PATH", duckdb_path)

    return {
        "raw_dir": raw_dir,
        "game_csv": game_csv,
        "duckdb_path": duckdb_path,
    }


@pytest.fixture()
def empty_db_fixture(temp_paths_fixture):
    # Ensure game.csv is empty
    pd.DataFrame(columns=[
        "season_id", "team_id", "team_abbreviation", "team_name",
        "game_id", "game_date", "season_type", "wl"
    ]).to_csv(temp_paths_fixture["game_csv"], index=False)

    # Ensure DuckDB is empty (or created empty)
    con = duckdb.connect(str(temp_paths_fixture["duckdb_path"]))
    try:
        con.execute("DROP TABLE IF EXISTS bronze_game_log_team;")
        con.execute("""
            CREATE TABLE bronze_game_log_team (
                season_id VARCHAR, team_id VARCHAR, team_abbreviation VARCHAR,
                team_name VARCHAR, game_id VARCHAR, game_date DATE,
                season_type VARCHAR, wl VARCHAR
            );
        """)
    finally:
        con.close()
    return temp_paths_fixture


@pytest.fixture()
def populated_db_fixture(temp_paths_fixture):
    existing = pd.DataFrame(
        [
            {
                "season_id": "22022",
                "team_id": "1610612737",
                "team_abbreviation": "ATL",
                "team_name": "Atlanta Hawks",
                "game_id": "0022200001",
                "game_date": date(2022, 10, 18),
                "season_type": "Regular Season",
                "wl": "W",
            }
        ]
    )
    existing['game_date'] = pd.to_datetime(existing['game_date'])
    existing.to_csv(temp_paths_fixture["game_csv"], index=False)

    con = duckdb.connect(str(temp_paths_fixture["duckdb_path"]))
    try:
        con.execute("DROP TABLE IF EXISTS bronze_game_log_team;")
        con.execute("""
            CREATE TABLE bronze_game_log_team (
                season_id VARCHAR, team_id VARCHAR, team_abbreviation VARCHAR,
                team_name VARCHAR, game_id VARCHAR, game_date DATE,
                season_type VARCHAR, wl VARCHAR
            );
        """)
        con.register("existing_df", existing)
        con.execute("INSERT INTO bronze_game_log_team SELECT * FROM existing_df")
    finally:
        con.close()
    return temp_paths_fixture


def test_daily_no_existing_data(empty_db_fixture, monkeypatch):
    new_rows = pd.DataFrame(
        [
            {
                "season_id": "22023",
                "team_id": "1610612738",
                "team_abbreviation": "BOS",
                "team_name": "Boston Celtics",
                "game_id": "0022300001",
                "game_date": date(2023, 10, 18),
                "season_type": "Regular Season",
                "wl": "W",
            }
        ]
    )
    new_rows['game_date'] = pd.to_datetime(new_rows['game_date'])

    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        lambda *args, **kwargs: new_rows.copy(),
    )
    monkeypatch.setattr(update.extract, "PER_GAME_SLEEP_SECONDS", 0)
    monkeypatch.setattr(update.extract, "get_box_score", lambda *args, **kwargs: (pd.DataFrame(), pd.DataFrame()))
    monkeypatch.setattr(update.extract, "get_play_by_play", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(time, "sleep", lambda _: None)

    csv_frame = pd.read_csv(empty_db_fixture["game_csv"], dtype={'season_id': str, 'team_id': str, 'game_id': str})
    csv_frame['game_date'] = pd.to_datetime(csv_frame['game_date'])
    pd.testing.assert_frame_equal(csv_frame, new_rows)

    con = duckdb.connect(str(empty_db_fixture["duckdb_path"]))
    try:
        db_frame = con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
        pd.testing.assert_frame_equal(db_frame, new_rows)
    finally:
        con.close()

def test_daily_appends_and_updates_duckdb(populated_db_fixture, monkeypatch):
    new_rows = pd.DataFrame(
        [
            {
                "season_id": "22023",
                "team_id": "1610612738",
                "team_abbreviation": "BOS",
                "team_name": "Boston Celtics",
                "game_id": "0022300001",
                "game_date": date(2023, 10, 18),
                "season_type": "Regular Season",
                "wl": "W",
            },
            {
                "season_id": "22023",
                "team_id": "1610612760",
                "team_abbreviation": "OKC",
                "team_name": "Oklahoma City Thunder",
                "game_id": "0022300001",
                "game_date": date(2023, 10, 18),
                "season_type": "Regular Season",
                "wl": "L",
            },
        ]
    )
    new_rows['game_date'] = pd.to_datetime(new_rows['game_date'])

    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        lambda *args, **kwargs: new_rows.copy(),
    )

    team_box = pd.DataFrame(
        [
            {"game_id": "0022300001", "team_id": "1610612738", "pts": 120},
            {"game_id": "0022300001", "team_id": "1610612760", "pts": 115},
        ]
    )
    player_box = pd.DataFrame(
        [
            {
                "game_id": "0022300001",
                "team_id": "1610612738",
                "player_id": "203507",
                "pts": 35,
            }
        ]
    )
    pbp = pd.DataFrame(
        [
            {
                "game_id": "0022300001",
                "eventnum": 1,
                "team_id": "1610612738",
                "description": "Jump Ball",
            }
        ]
    )

    monkeypatch.setattr(update.extract, "PER_GAME_SLEEP_SECONDS", 0)
    monkeypatch.setattr(update.extract, "get_box_score", lambda *args, **kwargs: (team_box.copy(), player_box.copy()))
    monkeypatch.setattr(update.extract, "get_play_by_play", lambda *args, **kwargs: pbp.copy())
    monkeypatch.setattr(time, "sleep", lambda _: None)

    result = update.daily(start_date="2023-10-18", end_date="2023-10-18")

    assert result.rows_written == 2
    csv_frame = pd.read_csv(populated_db_fixture["game_csv"], dtype={'season_id': str, 'team_id': str, 'game_id': str})
    csv_frame['game_date'] = pd.to_datetime(csv_frame['game_date'])
    # The existing test had a hardcoded frame, let's make it dynamic
    expected_csv_frame = pd.concat([
        pd.DataFrame(
            [
                {
                    "season_id": "22022",
                    "team_id": "1610612737",
                    "team_abbreviation": "ATL",
                    "team_name": "Atlanta Hawks",
                    "game_id": "0022200001",
                    "game_date": "2022-10-18",
                    "season_type": "Regular Season",
                    "wl": "W",
                }
            ]
        ).assign(game_date=lambda df: pd.to_datetime(df["game_date"])),
        new_rows
    ], ignore_index=True)
    pd.testing.assert_frame_equal(csv_frame, expected_csv_frame)
    con = duckdb.connect(str(populated_db_fixture["duckdb_path"]))
    try:
        db_frame = con.execute("SELECT * FROM bronze_game_log_team ORDER BY game_id").fetch_df()
        expected_db_frame = pd.concat([
            pd.DataFrame(
                [
                    {
                        "season_id": "22022",
                        "team_id": "1610612737",
                        "team_abbreviation": "ATL",
                        "team_name": "Atlanta Hawks",
                        "game_id": "0022200001",
                        "game_date": date(2022, 10, 18),
                        "season_type": "Regular Season",
                        "wl": "W",
                    }
                ]
            ).assign(game_date=lambda df: pd.to_datetime(df["game_date"])),
            new_rows
        ], ignore_index=True)
        pd.testing.assert_frame_equal(db_frame, expected_db_frame)

        assert con.execute("SELECT COUNT(*) FROM bronze_box_score_team").fetchone()[0] == len(team_box)
        assert con.execute("SELECT COUNT(*) FROM bronze_box_score_player").fetchone()[0] == len(player_box)
        assert con.execute("SELECT COUNT(*) FROM bronze_play_by_play").fetchone()[0] == len(pbp)
    finally:
        con.close()


def test_daily_existing_data_no_new_available(populated_db_fixture, monkeypatch):
    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        lambda *args, **kwargs: pd.DataFrame(),
    )
    monkeypatch.setattr(time, "sleep", lambda _: None)

    result = update.daily()

    assert result.rows_written == 0
    assert result.final_row_count == 1 # One existing row

    csv_frame = pd.read_csv(populated_db_fixture["game_csv"], dtype={'season_id': str, 'team_id': str, 'game_id': str})
    csv_frame['game_date'] = pd.to_datetime(csv_frame['game_date'])
    expected_csv_frame = pd.DataFrame(
        [
            {
                "season_id": "22022",
                "team_id": "1610612737",
                "team_abbreviation": "ATL",
                "team_name": "Atlanta Hawks",
                "game_id": "0022200001",
                "game_date": "2022-10-18",
                "season_type": "Regular Season",
                "wl": "W",
            }
        ]
    ).assign(game_date=lambda df: pd.to_datetime(df["game_date"]))
    pd.testing.assert_frame_equal(csv_frame, expected_csv_frame)

    con = duckdb.connect(str(populated_db_fixture["duckdb_path"]))
    try:
        db_frame = con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
        pd.testing.assert_frame_equal(db_frame, expected_csv_frame)
    finally:
        con.close()


def test_daily_fetch_all_history(empty_db_fixture, monkeypatch):
    new_rows = pd.DataFrame(
        [
            {
                "season_id": "22023",
                "team_id": "1610612738",
                "team_abbreviation": "BOS",
                "team_name": "Boston Celtics",
                "game_id": "0022300001",
                "game_date": date(2023, 10, 18),
                "season_type": "Regular Season",
                "wl": "W",
            },
            {
                "season_id": "22023",
                "team_id": "1610612760",
                "team_abbreviation": "OKC",
                "team_name": "Oklahoma City Thunder",
                "game_id": "0022300002",
                "game_date": date(2023, 10, 19),
                "season_type": "Regular Season",
                "wl": "L",
            },
        ]
    )
    new_rows['game_date'] = pd.to_datetime(new_rows['game_date'])

    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        lambda *args, **kwargs: new_rows.copy(),
    )
    monkeypatch.setattr(update.extract, "PER_GAME_SLEEP_SECONDS", 0)
    monkeypatch.setattr(update.extract, "get_box_score", lambda *args, **kwargs: (pd.DataFrame(), pd.DataFrame()))
    monkeypatch.setattr(update.extract, "get_play_by_play", lambda *args, **kwargs: pd.DataFrame())
    monkeypatch.setattr(time, "sleep", lambda _: None)

    result = update.daily(fetch_all_history=True)

    assert result.rows_written == 2
    assert result.final_row_count == 2

    csv_frame = pd.read_csv(empty_db_fixture["game_csv"], dtype={'season_id': str, 'team_id': str, 'game_id': str})
    csv_frame['game_date'] = pd.to_datetime(csv_frame['game_date'])
    pd.testing.assert_frame_equal(csv_frame, new_rows)

    con = duckdb.connect(str(empty_db_fixture["duckdb_path"]))
    try:
        db_frame = con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
        pd.testing.assert_frame_equal(db_frame, new_rows)
    finally:
        con.close()


def test_daily_start_end_date_args(empty_db_fixture, monkeypatch):
    mock_get_league_game_log = MagicMock(return_value=pd.DataFrame())
    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        mock_get_league_game_log,
    )
    monkeypatch.setattr(time, "sleep", lambda _: None)

    start_date_str = "2023-01-01"
    end_date_str = "2023-01-31"
    update.daily(start_date=start_date_str, end_date=end_date_str)

    mock_get_league_game_log.assert_called_once_with(date(2023, 1, 1), date(2023, 1, 31))


def test_daily_api_error_handling(empty_db_fixture, monkeypatch):
    def mock_get_league_game_log(*args, **kwargs):
        raise requests.exceptions.RequestException("API is down")

    monkeypatch.setattr(
        update.extract,
        "get_league_game_log_from_date",
        mock_get_league_game_log,
    )
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(requests.exceptions.RequestException, match="API is down"):
        update.daily(start_date="2023-01-01")


def test_real_api_league_log():
    start = end = pd.Timestamp('2024-04-10').date()
    try:
        frame = update.extract.get_league_game_log_from_date(start, end)
    except requests.exceptions.RequestException as exc:  # noqa: BLE001
        pytest.skip(f"NBA stats API unavailable: {exc}")
    assert not frame.empty
    assert 'GAME_ID' in {col.upper() for col in frame.columns}