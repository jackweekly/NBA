#!/usr/bin/env python3
"""Apply the warehouse schema definitions to DuckDB."""
from __future__ import annotations

import pathlib
import sys

import duckdb

from nba_db.paths import DUCKDB_PATH


_SUBDIR_ORDER = {
    "silver": [
        "home_away_overrides.sql",
        "team_dim.sql",
        "team.sql",
        "player.sql",
        "game.sql",
        "box_score_team_norm.sql",
        "team_game.sql",
        "team_game_coverage.sql",
    ],
}


def _collect_sql_files(root: pathlib.Path) -> list[pathlib.Path]:
    ordered: list[pathlib.Path] = []
    build = root / "build_schema.sql"
    if not build.exists():
        raise FileNotFoundError(build)
    ordered.append(build)

    for subdir_name in ("helpers", "silver", "marts"):
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


def _statements_from(path: pathlib.Path) -> list[str]:
    text = path.read_text()
    return [stmt.strip() for stmt in text.split(';') if stmt.strip()]


def main() -> int:
    sql_root = pathlib.Path("sql")
    try:
        sql_files = _collect_sql_files(sql_root)
    except FileNotFoundError as exc:
        print(f"[ERR] {exc}", file=sys.stderr)
        return 1

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute('PRAGMA threads=4;')

    try:
        for sql_file in sql_files:
            for statement in _statements_from(sql_file):
                con.execute(statement)
    finally:
        con.close()

    print(f"Applied schema to {DUCKDB_PATH}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
