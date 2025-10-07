from __future__ import annotations

import time

import duckdb
import requests
import pandas as pd
import pytest

from nba_db import update


@pytest.fixture()
def temp_env(tmp_path, monkeypatch):
    raw_dir = tmp_path / "data" / "raw"
    raw_dir.mkdir(parents=True)
    game_csv = raw_dir / "game.csv"
    duckdb_path = tmp_path / "data" / "nba.duckdb"

    monkeypatch.setattr(update, "RAW_DIR", raw_dir)
    monkeypatch.setattr(update, "GAME_CSV", game_csv)
    monkeypatch.setattr(update, "DUCKDB_PATH", duckdb_path)

    existing = pd.DataFrame(
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
    )
    existing.to_csv(game_csv, index=False)

    con = duckdb.connect(str(duckdb_path))
    try:
        con.register("existing_df", existing)
        con.execute("CREATE TABLE bronze_game_log_team AS SELECT * FROM existing_df")
    finally:
        con.close()

    return {
        "raw_dir": raw_dir,
        "game_csv": game_csv,
        "duckdb_path": duckdb_path,
    }


def test_daily_appends_and_updates_duckdb(temp_env, monkeypatch):
    new_rows = pd.DataFrame(
        [
            {
                "season_id": "22023",
                "team_id": "1610612738",
                "team_abbreviation": "BOS",
                "team_name": "Boston Celtics",
                "game_id": "0022300001",
                "game_date": "2023-10-18",
                "season_type": "Regular Season",
                "wl": "W",
            },
            {
                "season_id": "22023",
                "team_id": "1610612760",
                "team_abbreviation": "OKC",
                "team_name": "Oklahoma City Thunder",
                "game_id": "0022300001",
                "game_date": "2023-10-18",
                "season_type": "Regular Season",
                "wl": "L",
            },
        ]
    )

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
    assert result.final_row_count == 3

    csv_frame = pd.read_csv(temp_env["game_csv"])
    assert len(csv_frame) == 3

    con = duckdb.connect(str(temp_env["duckdb_path"]))
    try:
        assert con.execute("SELECT COUNT(*) FROM bronze_game_log_team").fetchone()[0] == 3
        assert con.execute("SELECT COUNT(*) FROM bronze_box_score_team").fetchone()[0] == len(team_box)
        assert con.execute("SELECT COUNT(*) FROM bronze_box_score_player").fetchone()[0] == len(player_box)
        assert con.execute("SELECT COUNT(*) FROM bronze_play_by_play").fetchone()[0] == len(pbp)
    finally:
        con.close()


def test_real_api_league_log():
    start = end = pd.Timestamp('2024-04-10').date()
    try:
        frame = update.extract.get_league_game_log_from_date(start, end)
    except requests.exceptions.RequestException as exc:  # noqa: BLE001
        pytest.skip(f"NBA stats API unavailable: {exc}")
    assert not frame.empty
    assert 'GAME_ID' in {col.upper() for col in frame.columns}
