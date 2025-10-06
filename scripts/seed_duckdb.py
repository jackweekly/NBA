#!/usr/bin/env python3
"""CLI wrapper to seed the project DuckDB database."""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

from nba_db.duckdb_seed import seed_duckdb
from nba_db.paths import DUCKDB_PATH, GAME_CSV, RAW_BOOTSTRAP_DIR, WYATT_DATASET_DIR


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--db",
        type=Path,
        default=DUCKDB_PATH,
        help=f"Path to DuckDB database (default: {DUCKDB_PATH})",
    )
    parser.add_argument(
        "--sqlite",
        type=Path,
        default=None,
        help="Optional path to Kaggle SQLite file (default: auto-detect)",
    )
    parser.add_argument(
        "--bootstrap-dir",
        type=Path,
        default=RAW_BOOTSTRAP_DIR,
        help=f"Directory containing bootstrap CSVs (default: {RAW_BOOTSTRAP_DIR})",
    )
    parser.add_argument(
        "--game-csv",
        type=Path,
        default=GAME_CSV,
        help=f"Path to consolidated game log CSV (default: {GAME_CSV})",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose logging",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or [])
    logging.basicConfig(
        level=logging.INFO if not args.verbose else logging.DEBUG,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    seed_duckdb(
        database_path=args.db,
        sqlite_path=args.sqlite,
        bootstrap_dir=args.bootstrap_dir,
        game_log_csv=args.game_csv,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main(sys.argv[1:]))
