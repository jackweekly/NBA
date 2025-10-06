#!/usr/bin/env python3
"""Command line interface for updating local NBA raw data dumps."""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path


_NUMERIC_ENV_DEFAULTS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OPENBLAS_CORETYPE": "HASWELL",
}

for _env_key, _env_value in _NUMERIC_ENV_DEFAULTS.items():
    os.environ.setdefault(_env_key, _env_value)

from nba_db import update as nba_update


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory used to store daily CSV dumps.",
    )
    parser.add_argument(
        "--start-date",
        help=(
            "Optional ISO date (YYYY-MM-DD) marking the first day to download. "
            "Defaults to 1946-11-01 when omitted."
        ),
    )
    parser.add_argument(
        "--end-date",
        help="Optional ISO date (YYYY-MM-DD) marking the last day to download.",
    )
    parser.add_argument(
        "--fetch-all-history",
        action="store_true",
        help=(
            "Download the full historical league game log instead of an incremental"
            " update."
        ),
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (default: INFO).",
    )
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )
    result = nba_update.daily(
        output_dir=Path(args.output_dir),
        start_date=args.start_date,
        end_date=args.end_date,
        fetch_all_history=args.fetch_all_history,
    )
    print(
        f"Wrote {result.rows_written} rows to {result.output_path} "
        f"(appended={result.appended})"
    )


if __name__ == "__main__":
    main()
