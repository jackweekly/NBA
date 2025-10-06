#!/usr/bin/env python3
"""Quick validation helper for the consolidated game log."""
from __future__ import annotations

import sys

import pandas as pd

from nba_db.paths import GAME_CSV


def main() -> int:
    game_path = GAME_CSV
    if not game_path.exists():
        print(f"No game.csv found at {game_path}", file=sys.stderr)
        return 1

    frame = pd.read_csv(game_path)
    frame.columns = [col.lower() for col in frame.columns]
    if "game_date" not in frame.columns:
        print("game_date column missing", file=sys.stderr)
        return 1
    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")

    missing_dates = frame["game_date"].isna().sum()
    print(f"Loaded {len(frame):,} rows from {game_path}")
    print(f"Missing game_date entries: {missing_dates}")
    print("Rows by season_type:")
    if "season_type" in frame.columns:
        counts = frame["season_type"].fillna("<missing>").value_counts().sort_index()
        for season_type, count in counts.items():
            print(f"  {season_type}: {count:,}")
    else:
        print("  season_type column missing")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
