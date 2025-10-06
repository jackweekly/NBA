#!/usr/bin/env python3
"""Command line interface for updating local NBA raw data dumps."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from nbapredictor.nbadb_sync import update_raw_data


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        default="data/raw",
        help="Directory used to store daily CSV dumps.",
    )
    parser.add_argument(
        "--start-date",
        help="Optional ISO date (YYYY-MM-DD) marking the first day to download.",
    )
    parser.add_argument(
        "--end-date",
        help="Optional ISO date (YYYY-MM-DD) marking the last day to download.",
    )
    parser.add_argument(
        "--bootstrap-kaggle",
        action="store_true",
        help="Download the upstream Kaggle dataset before fetching daily stats.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download CSV files even if they already exist.",
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
    summary = update_raw_data(
        output_dir=Path(args.output_dir),
        start_date=args.start_date,
        end_date=args.end_date,
        bootstrap_kaggle=args.bootstrap_kaggle,
        force=args.force,
    )
    print(json.dumps(summary.to_dict(), indent=2))


if __name__ == "__main__":
    main()
