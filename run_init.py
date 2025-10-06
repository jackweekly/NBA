#!/usr/bin/env python3
"""One-time bootstrap script for the NBA data pipeline."""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path


def main() -> None:
    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root))

    from nba_db import update

    timestamp = datetime.now().isoformat(timespec="seconds")
    print(f"Starting init at {timestamp}")
    update.init()
    print(f"Finished init at {datetime.now().isoformat(timespec='seconds')}")


if __name__ == "__main__":
    main()
