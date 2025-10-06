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
    if result.last_game_date:
        print(
            "Bootstrap completed up to",
            result.last_game_date.isoformat(),
        )
    print(f"Imported {len(result.downloaded_files)} season files from {result.dataset_dir}")
    print(f"Finished init at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
