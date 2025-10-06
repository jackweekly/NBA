#!/usr/bin/env python3
"""Entry point for running the NBA data daily update."""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


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


def _parse_args(argv: list[str]) -> tuple[bool, Optional[str]]:
    fetch_all_history = False
    start_date: Optional[str] = None

    if not argv:
        return fetch_all_history, start_date

    if argv[0] == "--fetch-all-history":
        fetch_all_history = True
    else:
        start_date = argv[0]

    return fetch_all_history, start_date


def main(argv: Optional[list[str]] = None) -> None:
    argv = list(argv) if argv is not None else sys.argv[1:]

    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root))

    _configure_numeric_environment()

    from nba_db import update
    from nba_db.logger import init_logger

    init_logger(logger_type="console")

    fetch_all_history, start_date = _parse_args(argv)

    print(f"Starting daily update at {datetime.now().isoformat(timespec='seconds')}")
    if fetch_all_history:
        result = update.daily(fetch_all_history=True)
    elif start_date:
        result = update.daily(start_date=start_date)
    else:
        result = update.daily()
    print(
        f"Wrote {result.rows_written} rows to {result.output_path}"
        f" (appended={result.appended})"
    )
    print(f"Finished daily update at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
