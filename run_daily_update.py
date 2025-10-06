#!/usr/bin/env python3
"""Entry point for running the NBA data daily update."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional

from nba_db.paths import ROOT


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
    parser.add_argument("--start-date", help="Optional ISO start date override (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Optional ISO end date override (YYYY-MM-DD)")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    argv = list(argv) if argv is not None else sys.argv[1:]

    src_path = ROOT / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    _configure_numeric_environment()

    from nba_db import update

    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    logging.info("Starting daily update at %s", datetime.now().isoformat(timespec="seconds"))

    try:
        result = update.daily(start_date=args.start_date, end_date=args.end_date)
    except (FileNotFoundError, RuntimeError) as exc:
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
