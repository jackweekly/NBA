#!/usr/bin/env python3
"""One-time bootstrap script for the NBA data pipeline."""
from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path


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


def main() -> None:
    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root))

    _configure_numeric_environment()

    from nba_db import update

    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"Starting init at {timestamp}")
    result = update.init()
    print("Bootstrap completed. Files written:")
    print(f"  Players: {result.player_path} ({result.row_counts['players']} rows)")
    print(f"  Teams: {result.team_path} ({result.row_counts['teams']} rows)")
    print(f"  Games: {result.game_path} ({result.row_counts['games']} rows)")
    print(
        f"  Game summaries: {result.game_summary_path} "
        f"({result.row_counts['game_summaries']} rows)"
    )
    print(f"Finished init at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
