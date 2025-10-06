"""Shared filesystem locations for the lightweight NBA data pipeline."""
from __future__ import annotations

from pathlib import Path
import os
ROOT = Path(__file__).resolve().parents[2]  # repo root (…/NBA)
RAW_DIR = ROOT / "data" / "raw"
GAME_CSV = Path(os.getenv("NBA_GAME_CSV", RAW_DIR / "game.csv"))


__all__ = ["ROOT", "RAW_DIR", "GAME_CSV"]
