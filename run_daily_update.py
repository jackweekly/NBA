#!/usr/bin/env python3
"""Entry point for running the NBA data daily update."""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import date, datetime, timedelta
from typing import Optional

from nba_db.paths import ROOT, WATERMARK_PATH


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
    parser.add_argument(
        "--fetch-all-history",
        action="store_true",
        help="Download the full historical league game log ignoring existing files",
    )
    return parser


def _read_watermark() -> Optional[date]:
    try:
        text = WATERMARK_PATH.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not text:
        return None
    try:
        return date.fromisoformat(text)
    except ValueError:
        logging.warning("Ignoring malformed watermark at %s: %r", WATERMARK_PATH, text)
        return None


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

    resolved_start = args.start_date
    if args.fetch_all_history:
        logging.info("Fetching full historical game log; ignoring bootstrap watermark and existing data")
    else:
        if resolved_start is None:
            watermark_date = _read_watermark()
            if watermark_date is not None:
                resolved_start = (watermark_date + timedelta(days=1)).isoformat()
                logging.info(
                    "Using bootstrap watermark %s -> start date %s",
                    watermark_date.isoformat(),
                    resolved_start,
                )

    try:
        result = update.daily(
            start_date=resolved_start,
            end_date=args.end_date,
            fetch_all_history=args.fetch_all_history,
        )
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
