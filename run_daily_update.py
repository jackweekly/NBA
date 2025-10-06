#!/usr/bin/env python3
"""Entry point for running the NBA data daily update."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from nba_db.paths import load_config, raw_data_dir


_NUMERIC_ENV_DEFAULTS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OPENBLAS_CORETYPE": "HASWELL",
}


def _configure_numeric_environment() -> None:
    for key, value in _NUMERIC_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the lightweight nba_db daily updater")
    parser.add_argument("start_date", nargs="?", help="Override the start date (YYYY-MM-DD)")
    parser.add_argument(
        "--fetch-all-history",
        action="store_true",
        help="Ignore resume logic and rebuild the entire game log",
    )
    parser.add_argument(
        "--output-dir",
        help="Override the raw data directory from config.yaml",
    )
    parser.add_argument(
        "--end-date",
        help="Optional inclusive end date for incremental runs (YYYY-MM-DD)",
    )
    return parser


def _bootstrap_warning(config: dict[str, object], override: Optional[str]) -> None:
    raw_dir = raw_data_dir(config=config, override=override)
    watermark = raw_dir / "bootstrap" / ".watermark"
    if not watermark.exists():
        logging.warning(
            "--fetch-all-history requested but no bootstrap watermark found at %s; "
            "this may be slow and rate-limit prone",
            watermark,
        )


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]

    project_root = Path(__file__).resolve().parent
    if str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))

    _configure_numeric_environment()

    from nba_db import update
    from nba_db.logger import init_logger

    parser = _build_parser()
    args = parser.parse_args(argv)

    init_logger(logger_type="console")

    config = load_config()
    if args.fetch_all_history:
        _bootstrap_warning(config, args.output_dir)

    logging.info("Starting daily update at %s", datetime.now().isoformat(timespec="seconds"))

    try:
        if args.fetch_all_history:
            result = update.daily(fetch_all_history=True, output_dir=args.output_dir)
        else:
            result = update.daily(
                start_date=args.start_date,
                end_date=args.end_date,
                output_dir=args.output_dir,
            )
    except FileNotFoundError as exc:
        logging.error("Daily update failed: %s", exc)
        return 1

    logging.info(
        "Daily update wrote %s new rows to %s (appended=%s)",
        result.rows_written,
        result.output_path,
        result.appended,
    )
    if result.rows_written == 0:
        logging.info("No new games were appended in this run")
    logging.info("Final game log row count: %s", result.final_row_count)
    logging.info("Finished daily update at %s", datetime.now().isoformat(timespec="seconds"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
