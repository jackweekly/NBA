#!/usr/bin/env python3
"""One-shot bootstrap and daily refresh runner for the NBA warehouse."""
from __future__ import annotations

import argparse
import logging
import subprocess
import time
from pathlib import Path

import duckdb

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


def _duckdb_is_seeded() -> bool:
    if not DUCKDB_PATH.exists():
        return False
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        tables = con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name IS NULL AND schema_name = 'main'"
        ).fetchall()
        return any(name == "bronze_game" for (name,) in tables)
    finally:
        con.close()


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
        "--offline-only",
        action="store_true",
        help="Run home/away override resolution without network calls",
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

    if args.force_kaggle or not _duckdb_is_seeded():
        logging.info("Seeding DuckDB at %s", DUCKDB_PATH)
        _run(["python", "scripts/seed_duckdb.py"])
    else:
        logging.info("DuckDB already seeded at %s; skipping seeding step", DUCKDB_PATH)

    logging.info("Resolving home/away overrides")
    fetch_cmd = ["python", "scripts/fetch_home_away_overrides.py"]
    if args.verbose:
        fetch_cmd.append("--verbose")
    if args.offline_only:
        fetch_cmd.append("--offline-only")
    _run(fetch_cmd)

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

        logging.info("Resolving home/away overrides after daily update")
        fetch_cmd_post = ["python", "scripts/fetch_home_away_overrides.py"]
        if args.verbose:
            fetch_cmd_post.append("--verbose")
        if args.offline_only:
            fetch_cmd_post.append("--offline-only")
        _run(fetch_cmd_post)

        logging.info("Running warehouse quality checks (post-daily)")
        _run(["python", "scripts/check_quality.py"])
    else:
        logging.info("Skipping daily update per --skip-daily")

    total_duration = time.perf_counter() - overall_start
    logging.info("Pipeline run completed successfully in %.1fs", total_duration)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
