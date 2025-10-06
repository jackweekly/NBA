"""Shared filesystem locations for the lightweight NBA data pipeline."""
from __future__ import annotations

from pathlib import Path


ROOT: Path = Path(__file__).resolve().parents[1]
"""Repository root resolved at import time."""


RAW_DIR: Path = ROOT / "data" / "raw"
"""Directory containing raw CSV exports."""


GAME_CSV: Path = RAW_DIR / "game.csv"
"""Canonical location of the consolidated per-team game log."""


__all__ = ["ROOT", "RAW_DIR", "GAME_CSV"]
