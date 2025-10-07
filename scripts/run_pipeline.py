#!/usr/bin/env python3
"""One-shot bootstrap and daily refresh runner for the NBA warehouse."""
from __future__ import annotations

import argparse
import logging
import subprocess
import time
from pathlib import Path

from nba_db.paths import DUCKDB_PATH, WYATT_DATASET_DIR

KAGGLE_SQLITE = WYATT_DATASET_DIR / "nba.sqlite"


def _run(cmd: list[str]) -> None:
    """Execute ``cmd`` while logging start/finish and duration."""

    logging.info("→ Running: %s", " ".join(cmd))
    started = time.perf_counter()
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as exc:  # pragma: no cover - external command failure
        logging.error("Command failed (exit %s): %s", exc.returncode, " ".join(cmd))
        raise
    duration = time.perf_counter() - started
    logging.info("✓ Completed in %.1fs", duration)


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
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging for extra insight",
    )
    args = parser.parse_args(argv or [])

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    overall_start = time.perf_counter()
    logging.info("Starting pipeline run")

    _ensure_kaggle_dataset(force=args.force_kaggle)

    logging.info("Seeding DuckDB at %s", DUCKDB_PATH)
    _run(["python", "scripts/seed_duckdb.py"])

    logging.info("Applying warehouse schema views")
    _run(["python", "scripts/apply_schema.py"])

    logging.info("Running warehouse quality checks (pre-daily)")
    _run(["python", "scripts/check_quality.py"])

    if not args.skip_daily:
        logging.info("Running daily incremental update")
        daily_cmd = ["python", "run_daily_update.py"]
        if args.verbose:
            logging.debug("Propagating --verbose flag to run_daily_update.py")
            daily_cmd.append("--verbose")
        _run(daily_cmd)

        logging.info("Running warehouse quality checks (post-daily)")
        _run(["python", "scripts/check_quality.py"])
    else:
        logging.info("Skipping daily update per --skip-daily")

    total_duration = time.perf_counter() - overall_start
    logging.info("Pipeline run completed successfully in %.1fs", total_duration)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
