from __future__ import annotations

import sqlite3
from pathlib import Path

import duckdb
import pandas as pd

from nba_db.duckdb_seed import seed_duckdb
from scripts import apply_schema


def _make_sqlite(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE Game (
                GAME_ID TEXT,
                TEAM_ID TEXT,
                SEASON_ID TEXT,
                GAME_DATE TEXT,
                TEAM_ABBREVIATION TEXT,
                TEAM_NAME TEXT,
                MATCHUP TEXT,
                WL TEXT,
                MIN INTEGER,
                FGM REAL,
                FGA REAL,
                FG_PCT REAL,
                FG3M REAL,
                FG3A REAL,
                FG3_PCT REAL,
                FTM REAL,
                FTA REAL,
                FT_PCT REAL,
                OREB REAL,
                DREB REAL,
                REB REAL,
                AST REAL,
                STL REAL,
                BLK REAL,
                TOV REAL,
                PF REAL,
                PTS REAL,
                PLUS_MINUS REAL,
                VIDEO_AVAILABLE INTEGER,
                SEASON_TYPE TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO Game VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                "0022300001",
                "1610612738",
                "22023",
                "2023-10-18",
                "BOS",
                "Boston Celtics",
                "BOS vs. NYK",
                "W",
                240,
                40,
                80,
                0.5,
                10,
                30,
                0.333,
                20,
                25,
                0.8,
                10,
                30,
                40,
                25,
                8,
                5,
                12,
                18,
                110,
                5,
                1,
                "Regular Season",
            ),
        )
        conn.execute(
            """
            CREATE TABLE Player (
                id INTEGER,
                full_name TEXT
            )
            """
        )
        conn.execute(
            "INSERT INTO Player VALUES (?, ?)", (203507, "Giannis Antetokounmpo"))
    finally:
        conn.commit()
        conn.close()


def test_seed_duckdb_from_sqlite(tmp_path):
    sqlite_path = tmp_path / "nba.sqlite"
    _make_sqlite(sqlite_path)
    db_path = tmp_path / "warehouse.duckdb"

    seed_duckdb(
        database_path=db_path,
        sqlite_path=sqlite_path,
        bootstrap_dir=tmp_path,
        game_log_csv=tmp_path / "game.csv",
    )

    con = duckdb.connect(str(db_path))
    try:
        tables = {
            name
            for (name,) in con.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name='main'"
            ).fetchall()
        }
        assert "bronze_game" in tables
        count = con.execute("SELECT COUNT(*) FROM bronze_game").fetchone()[0]
        assert count == 1
    finally:
        con.close()


def test_apply_schema(tmp_path, monkeypatch):
    sqlite_path = tmp_path / "nba.sqlite"
    _make_sqlite(sqlite_path)
    db_path = tmp_path / "warehouse.duckdb"

    seed_duckdb(
        database_path=db_path,
        sqlite_path=sqlite_path,
        bootstrap_dir=tmp_path,
        game_log_csv=tmp_path / "game.csv",
    )

    con = duckdb.connect(str(db_path))
    try:
        team_frame = pd.DataFrame(
            [
                {
                    "season_id": "22023",
                    "team_id": "1610612738",
                    "team_abbreviation": "BOS",
                    "team_name": "Boston Celtics",
                    "game_id": "0022300001",
                    "game_date": "2023-10-18",
                    "matchup": "BOS vs. NYK",
                    "wl": "W",
                    "min": 240,
                    "fgm": 40,
                    "fga": 80,
                    "fg_pct": 0.5,
                    "fg3m": 10,
                    "fg3a": 30,
                    "fg3_pct": 0.333,
                    "ftm": 20,
                    "fta": 25,
                    "ft_pct": 0.8,
                    "oreb": 10,
                    "dreb": 30,
                    "reb": 40,
                    "ast": 25,
                    "stl": 8,
                    "blk": 5,
                    "tov": 12,
                    "pf": 18,
                    "pts": 110,
                    "plus_minus": 5,
                    "season_type": "Regular Season",
                    "video_available": 1,
                }
            ]
        )
        con.register("payload_team", team_frame)
        con.execute("CREATE OR REPLACE TABLE bronze_game_log_team AS SELECT * FROM payload_team")

        player_frame = pd.DataFrame(
            [{"id": 203507, "full_name": "Giannis Antetokounmpo"}]
        )
        con.register("payload_player", player_frame)
        con.execute("CREATE OR REPLACE TABLE bronze_player AS SELECT * FROM payload_player")
    finally:
        con.close()

    monkeypatch.setattr(apply_schema, "DUCKDB_PATH", db_path)
    apply_schema.main()

    con = duckdb.connect(str(db_path))
    try:
        views = {
            (schema, name)
            for (schema, name) in con.execute(
                "SELECT table_schema, table_name FROM information_schema.views"
            ).fetchall()
        }
        assert ('silver', 'team_game') in views
        assert ('marts', 'fact_team_game') in views
    finally:
        con.close()
