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
    "box_score": "bronze_box_score", # Changed to bronze_box_score
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

def _merge_csv(con, table_name, csv_path, pks):
    if not csv_path.exists():
        LOGGER.warning(f"CSV file {csv_path} not found, skipping merge for {table_name}.")
        return

    LOGGER.info("Merging CSV %s -> %s", csv_path, table_name)
    
    temp_table_name = f"{table_name}_csv"
    con.execute(f"""
      CREATE OR REPLACE TEMP TABLE {temp_table_name} AS
      SELECT * FROM read_csv_auto(?, IGNORE_ERRORS=true)
    """, [str(csv_path)])

    con.execute(f"""
      CREATE TABLE IF NOT EXISTS {table_name} AS
      SELECT * FROM {temp_table_name} WHERE 1=0
    """)
    
    temp_cols = {c[0].lower() for c in con.execute(f"DESCRIBE {temp_table_name}").fetchall()}
    if pks and not all(pk.lower() in temp_cols for pk in pks):
        pks = None 
        LOGGER.warning(f"Not all PKs for {table_name} found in {csv_path}. Will dedupe on all columns.")

    if pks:
        on_clause = " AND ".join([f"t.{pk} = s.{pk}" for pk in pks])
        con.execute(f"""
          MERGE INTO {table_name} t
          USING {temp_table_name} s
          ON {on_clause}
          WHEN NOT MATCHED THEN INSERT *
        """)
        
        partition_by = ", ".join(pks)
        con.execute(f"""
          CREATE OR REPLACE TABLE {table_name} AS
          SELECT * EXCLUDE rn
          FROM (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY 1) AS rn
            FROM {table_name}
          )
          WHERE rn = 1
        """)
    else:
        con.execute(f"INSERT INTO {table_name} SELECT * FROM {temp_table_name}")
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT DISTINCT * FROM {table_name}")

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

    database.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(database))
    con.execute("PRAGMA threads=16;")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

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

    _merge_csv(con, "bronze_box_score", bootstrap_root / "box_score.csv", ["game_id", "team_id", "player_id"])
    _merge_csv(con, "bronze_box_score_team", bootstrap_root / "line_score.csv", ["game_id", "team_id"])
    _merge_csv(con, "bronze_game", bootstrap_root / "game.csv", ["game_id"])
    _merge_csv(con, "bronze_player", bootstrap_root / "player.csv", ["player_id"])
    _merge_csv(con, "bronze_team", bootstrap_root / "team.csv", ["team_id"])
    _merge_csv(con, "bronze_play_by_play", bootstrap_root / "play_by_play.csv", None) # No PK

    con.close()
    LOGGER.info("Seeded DuckDB at %s", database)
    return database


def main() -> int:  # pragma: no cover - CLI wrapper
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    seed_duckdb()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())