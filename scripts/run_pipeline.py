#!/usr/bin/env python3
"""One-shot bootstrap and daily refresh runner for the NBA warehouse."""
from __future__ import annotations

import argparse
import logging
import subprocess
from pathlib import Path

from nba_db.paths import DUCKDB_PATH, WYATT_DATASET_DIR

KAGGLE_SQLITE = WYATT_DATASET_DIR / "nba.sqlite"


def _run(cmd: list[str]) -> None:
    logging.info("Running: %s", " ".join(cmd))
    subprocess.run(cmd, check=True)


def _ensure_kaggle_dataset(force: bool) -> None:
    if force or not KAGGLE_SQLITE.exists():
        logging.info("Kaggle dataset missing or force requested; invoking run_init.py")
        cmd = ["python", "run_init.py"]
        if force:
            cmd.append("--force")
        _run(cmd)
    else:
        logging.info("Kaggle dataset already present at %s", KAGGLE_SQLITE)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--force-kaggle",
        action="store_true",
        help="Force re-download of the Kaggle bootstrap dataset before seeding",
    )
    parser.add_argument(
        "--skip-daily",
        action="store_true",
        help="Skip the daily incremental update (useful for bootstrapping only)",
    )
    args = parser.parse_args(argv or [])

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    _ensure_kaggle_dataset(force=args.force_kaggle)

    logging.info("Seeding DuckDB at %s", DUCKDB_PATH)
    _run(["python", "scripts/seed_duckdb.py"])

    logging.info("Applying warehouse schema views")
    _run(["python", "scripts/apply_schema.py"])

    if not args.skip_daily:
        logging.info("Running daily incremental update")
        _run(["python", "run_daily_update.py"])
    else:
        logging.info("Skipping daily update per --skip-daily")

    logging.info("Pipeline completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
