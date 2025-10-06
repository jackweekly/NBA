"""Utilities for seeding the project DuckDB database."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import duckdb

from .paths import (
    DUCKDB_PATH,
    GAME_CSV,
    RAW_BOOTSTRAP_DIR,
    WYATT_DATASET_DIR,
)

LOGGER = logging.getLogger(__name__)

SQLITE_TABLE_MAP: dict[str, str] = {
    "game": "bronze_game",
    "team": "bronze_team",
    "player": "bronze_player",
    "line_score": "bronze_box_score_team",
    "box_score": "bronze_box_score_team",
    "play_by_play": "bronze_play_by_play",
}


def _coalesce_sqlite_tables(connection: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Return a mapping of DuckDB table name -> SQLite select statement."""

    rows = connection.execute(
        """
        SELECT table_name
        FROM duckdb_tables()
        WHERE database_name = 'seed'
          AND schema_name = 'main'
        """
    ).fetchall()
    existing = {name.lower(): name for (name,) in rows}

    mapping: dict[str, str] = {}
    for source_lower, target in SQLITE_TABLE_MAP.items():
        if target in mapping:
            continue
        source_name = existing.get(source_lower)
        if source_name:
            mapping[target] = f"SELECT * FROM seed.main.{source_name}"
    return mapping


def _load_csv_tables(connection: duckdb.DuckDBPyConnection, csv_paths: Iterable[Path]) -> None:
    for csv_path in csv_paths:
        if not csv_path.exists():
            continue
        table_name = f"bronze_{csv_path.stem.lower()}"
        LOGGER.info("Loading CSV %s -> %s", csv_path, table_name)
        connection.execute(
            f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto(?, sample_size=-1)
            """,
            [csv_path.as_posix()],
        )


def seed_duckdb(
    *,
    database_path: Path | None = None,
    sqlite_path: Path | None = None,
    bootstrap_dir: Path | None = None,
    game_log_csv: Path | None = None,
) -> Path:
    """Seed the DuckDB file with Kaggle and bootstrap artefacts."""

    database = database_path or DUCKDB_PATH
    sqlite_source = sqlite_path
    if sqlite_source is None:
        sqlite_candidates = sorted((WYATT_DATASET_DIR or Path()).rglob("*.sqlite"))
        sqlite_source = sqlite_candidates[0] if sqlite_candidates else None

    bootstrap_root = bootstrap_dir or RAW_BOOTSTRAP_DIR
    game_csv = game_log_csv or GAME_CSV

    database.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(database))
    con.execute("PRAGMA threads=4;")

    if sqlite_source and sqlite_source.exists():
        LOGGER.info("Attaching Kaggle SQLite %s", sqlite_source)
        con.execute(f"ATTACH '{sqlite_source.as_posix()}' AS seed (TYPE SQLITE)")
        mapping = _coalesce_sqlite_tables(con)
        for table_name, select_stmt in mapping.items():
            LOGGER.info("Copying %s -> %s", select_stmt.split()[-1], table_name)
            con.execute(f"CREATE OR REPLACE TABLE {table_name} AS {select_stmt}")
        con.execute("DETACH seed")
    else:
        LOGGER.info("No Kaggle SQLite source found; skipping ATTACH step")

    csv_sources: list[Path] = []
    if bootstrap_root and bootstrap_root.exists():
        csv_sources.extend(sorted(bootstrap_root.glob("*.csv")))
    if game_csv and game_csv.exists():
        csv_sources.append(game_csv)

    seen: set[Path] = set()
    ordered_sources: list[Path] = []
    for path in csv_sources:
        if path not in seen:
            ordered_sources.append(path)
            seen.add(path)

    _load_csv_tables(con, ordered_sources)

    tables = {name for (name,) in con.execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name IS NULL AND schema_name = 'main'"
    ).fetchall()}

    if 'bronze_game_log_team' not in tables:
        if 'bronze_game' in tables:
            LOGGER.info('Creating bronze_game_log_team from bronze_game')
            con.execute('CREATE OR REPLACE TABLE bronze_game_log_team AS SELECT * FROM bronze_game')
        else:
            con.execute('CREATE TABLE IF NOT EXISTS bronze_game_log_team (game_id VARCHAR, team_id VARCHAR)')

    con.close()
    LOGGER.info("Seeded DuckDB at %s", database)
    return database


def main() -> int:  # pragma: no cover - CLI wrapper
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    seed_duckdb()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
