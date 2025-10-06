#!/usr/bin/env python3
"""Load Kaggle CSV exports into a local DuckDB database."""
from __future__ import annotations

import pathlib
import re
import sys

import duckdb


RAW_DIR = pathlib.Path("data/raw")
DB_PATH = pathlib.Path("data/nba.duckdb")


def _sanitize(stem: str) -> str:
    """Convert a file stem to a safe DuckDB table name."""

    cleaned = re.sub(r"[^a-z0-9_]+", "_", stem.lower())
    return f"raw_{cleaned}".strip("_")


def main() -> int:
    if not RAW_DIR.exists():
        print(f"[ERR] {RAW_DIR} not found. Run run_init.py first.", file=sys.stderr)
        return 1

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("PRAGMA threads=4;")

    for csv_path in RAW_DIR.rglob("*.csv"):
        table_name = _sanitize(csv_path.stem)
        print(f"[LOAD] {csv_path} -> {table_name}")
        con.execute(
            """
            CREATE OR REPLACE TABLE %s AS
            SELECT * FROM read_csv_auto(?, sample_size=-1)
            """
            % table_name,
            [str(csv_path)],
        )

    # Ensure both raw_game and raw_games exist for downstream SQL compatibility.
    tables = {name for (name,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name IS NULL AND schema_name = 'main'"
    ).fetchall()}
    if "raw_game" in tables and "raw_games" not in tables:
        con.execute("CREATE OR REPLACE VIEW raw_games AS SELECT * FROM raw_game")
        tables.add("raw_games")
    elif "raw_games" in tables and "raw_game" not in tables:
        con.execute("CREATE OR REPLACE VIEW raw_game AS SELECT * FROM raw_games")
        tables.add("raw_game")

    if "raw_player" in tables and "raw_players" not in tables:
        con.execute("CREATE OR REPLACE VIEW raw_players AS SELECT * FROM raw_player")
    elif "raw_players" in tables and "raw_player" not in tables:
        con.execute("CREATE OR REPLACE VIEW raw_player AS SELECT * FROM raw_players")

    print(f"✅ Loaded CSVs into {DB_PATH}")

    summary = con.execute(
        """
        SELECT table_name, row_count
        FROM duckdb_tables()
        WHERE database_name IS NULL
          AND schema_name = 'main'
          AND table_name LIKE 'raw_%'
        ORDER BY table_name
        """
    ).fetchall()

    for table_name, row_count in summary:
        print(f"  - {table_name:<30} rows={row_count}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
