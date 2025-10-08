#!/usr/bin/env python3
"""Apply the warehouse schema definitions to DuckDB."""
from __future__ import annotations

import pathlib
import sys

import duckdb
from pathlib import Path

from nba_db.paths import DUCKDB_PATH


def _cols(con, table_name: str):
    return {
        r[0].lower()
        for r in con.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE lower(table_name)=?
        """, [table_name.lower()]).fetchall()
    }

def _ensure_medallion_schemas(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")
    con.execute("CREATE SCHEMA IF NOT EXISTS warehouse;")

def _create_bronze_game_norm(con):
    cols = _cols(con, "bronze_game")

    # Choose an expression that always yields an integer season
    if "season" in cols:
        season_expr = "CAST(season AS INTEGER)"
    elif "season_id" in cols:
        season_expr = "CAST(season_id AS INTEGER)"
    else:
        # last resort: derive from GAME_ID prefix
        season_expr = "CAST(substr(CAST(game_id AS VARCHAR), 1, 4) AS INTEGER)"

    con.execute(f"""
        CREATE OR REPLACE VIEW bronze_game_norm AS
        SELECT
            *,
            {season_expr} AS season_id
        FROM bronze_game
    """)


_SUBDIR_ORDER = {
    "silver": [
        "home_away_overrides.sql",
        "create_game_and_minutes.sql",
        "box_score_team_norm.sql",
        "team_dim.sql",
        "team.sql",
        "player.sql",
        "team_game.sql",
        "game.sql",
        "team_game_coverage.sql",
    ],
}

def _collect_sql_files(root: pathlib.Path) -> list[pathlib.Path]:
    ordered: list[pathlib.Path] = []
    build = root / "build_schema.sql"
    if not build.exists():
        raise FileNotFoundError(build)
    ordered.append(build)

    for subdir_name in ("helpers", "bronze", "silver", "ext", "ref", "marts"):
        subdir = root / subdir_name
        if not subdir.exists():
            continue
        if subdir_name in _SUBDIR_ORDER:
            for name in _SUBDIR_ORDER[subdir_name]:
                path = subdir / name
                if path.exists():
                    ordered.append(path)
            remaining = sorted(p for p in subdir.glob("*.sql") if p.name not in _SUBDIR_ORDER[subdir_name])
            ordered.extend(remaining)
        else:
            ordered.extend(sorted(subdir.glob("*.sql")))
    return ordered


def main() -> int:
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute('PRAGMA threads=4;')

    try:
        _ensure_medallion_schemas(con)
        _create_bronze_game_norm(con)

        sql_root = pathlib.Path("sql")
        try:
            sql_files = _collect_sql_files(sql_root)
        except FileNotFoundError as exc:
            print(f"[ERR] {exc}", file=sys.stderr)
            return 1

        for sql_file in sql_files:
            try:
                con.execute(sql_file.read_text())
            except Exception as e:
                print(f"==== Failed in file: {sql_file} ====")
                raise
    finally:
        con.close()

    print(f"Applied schema to {DUCKDB_PATH}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())