#!/usr/bin/env python3
"""Apply the warehouse schema definitions to DuckDB."""
from __future__ import annotations

import pathlib
import sys

import duckdb

from nba_db.paths import DUCKDB_PATH


def main() -> int:
    sql_path = pathlib.Path('sql/build_schema.sql')
    if not sql_path.exists():
        print(f"[ERR] {sql_path} not found", file=sys.stderr)
        return 1

    sql_text = sql_path.read_text()
    statements = [stmt.strip() for stmt in sql_text.split(';') if stmt.strip()]

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute('PRAGMA threads=4;')

    try:
        for statement in statements:
            con.execute(statement)
    finally:
        con.close()

    print(f"Applied schema to {DUCKDB_PATH}")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
