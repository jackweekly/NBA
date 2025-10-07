"""Miscellaneous helpers for the lightweight NBA data pipeline."""
from __future__ import annotations

import pandas as pd


VALID_GAME_ID_PREFIXES = ("001", "002", "003", "004", "005")


def get_proxies() -> list[str]:
    """Return configured HTTP proxies for stats.nba.com requests."""

    return []


def canonicalize_game_ids(frame: pd.DataFrame, *, column: str = "game_id") -> pd.DataFrame:
    """Return ``frame`` with ``column`` stripped to digits, zero-padded, and filtered."""

    if column not in frame.columns or frame.empty:
        return frame.copy()

    result = frame.copy()
    result[column] = (
        result[column]
        .astype(str)
        .str.extract(r"(\d+)", expand=False)
        .fillna("")
        .str.zfill(10)
    )
    valid_mask = result[column].str[:3].isin(VALID_GAME_ID_PREFIXES)
    invalid = result.loc[~valid_mask, column].dropna().unique()
    if len(invalid):
        print(f"[canon] dropping {len(invalid)} invalid game_ids (sample: {list(invalid)[:6]})")
    return result.loc[valid_mask].reset_index(drop=True)


__all__ = ["VALID_GAME_ID_PREFIXES", "canonicalize_game_ids", "get_proxies"]
