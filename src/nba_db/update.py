"""Incremental game log updater built around the Kaggle bootstrap seed."""
from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from . import extract
from .paths import GAME_CSV, RAW_DIR


LOGGER = logging.getLogger(__name__)

GAME_LOG_PRIMARY_KEY = ("game_id", "team_id", "season_type")
DEFAULT_SEASON_TYPE = "Regular Season"


def _canonicalise(df):
    """Lowercase columns and parse game_date; noop on None/empty."""
    if df is None or df.empty:
        return df
    df = df.copy()
    df.columns = df.columns.str.lower()
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    return df

def _atomic_write_csv(df: pd.DataFrame, out_path: Path | str):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=out_path.parent, suffix=".tmp") as tmp:
        df.to_csv(tmp.name, index=False)
        tmp.flush()
        os.fsync(tmp.fileno())
        tmp_name = tmp.name
    os.replace(tmp_name, out_path)

def _get_last_updated_date() -> date:
    if GAME_CSV.exists():
        existing_game_df = pd.read_csv(GAME_CSV)
        existing_game_df = _canonicalise(existing_game_df)
        if not existing_game_df.empty and "game_date" in existing_game_df.columns:
            return existing_game_df["game_date"].max().date()
    return date(2023, 1, 1) # Default to a reasonable date if no data or game_date column

def _get_fetch_dates(start_date_str: Optional[str], end_date_str: Optional[str]) -> Tuple[date, date]:
    """
    Determines the effective start and end dates for data fetching.

    Args:
        start_date_str (str, optional): User-provided start date string.
        end_date_str (str, optional): User-provided end date string.

    Returns:
        Tuple[date, date]: A tuple containing the effective start and end date objects.
    """
    last_updated_date = _get_last_updated_date()

    if start_date_str:
        fetch_from = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    else:
        fetch_from = last_updated_date + timedelta(days=1)

    if end_date_str:
        fetch_until = datetime.strptime(end_date_str, '%Y-%m-%d').date()
    else:
        fetch_until = date.today()

    return fetch_from, fetch_until


def _log_fetch_window(df: pd.DataFrame):
    df = _canonicalise(df)
    if df is None or df.empty:
        LOGGER.info("Fetched 0 rows.")
        return
    if "game_date" in df.columns:
        LOGGER.info(f"Fetched {len(df)} new rows covering {df['game_date'].min()} \u2192 {df['game_date'].max()}")
    else:
        LOGGER.info(f"Fetched {len(df)} new rows (no game_date column present).")


@dataclass(slots=True)
class DailyUpdateResult:
    """Summary describing a ``daily`` update."""

    output_path: Path
    rows_written: int
    appended: bool
    final_row_count: int

    def to_dict(self) -> dict[str, object]:  # pragma: no cover - helper for scripts/CLI
        return {
            "output_path": str(self.output_path),
            "rows_written": self.rows_written,
            "appended": self.appended,
            "final_row_count": self.final_row_count,
        }





def _canonicalise(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        return frame

    canonical = frame.copy()
    canonical.columns = canonical.columns.str.lower()
    if "game_date" in canonical.columns:
        canonical["game_date"] = pd.to_datetime(canonical["game_date"], errors="coerce")
    if "season_type" not in canonical.columns:
        canonical["season_type"] = DEFAULT_SEASON_TYPE
    else:
        canonical["season_type"] = (
            canonical["season_type"].fillna(DEFAULT_SEASON_TYPE).replace("", DEFAULT_SEASON_TYPE)
        )
    for column in ("season_id", "game_id", "team_id"):
        if column not in canonical.columns:
            continue
        series = canonical[column].astype(str).str.strip()
        if column in {"game_id", "team_id"}:
            series = series.str.lstrip("0").replace({"": "0"})
        canonical[column] = series
    return canonical
















def daily(
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DailyUpdateResult:
    """Fetch and append the latest NBA games into ``data/raw/game.csv``."""

    # read existing and canonicalise before using it
    if GAME_CSV.exists():
        existing_game_df = pd.read_csv(GAME_CSV)
        existing_game_df = _canonicalise(existing_game_df)
    else:
        existing_game_df = pd.DataFrame()


    fetch_from, fetch_until = _get_fetch_dates(start_date, end_date)

    LOGGER.info(f"Fetching data from {fetch_from} to {fetch_until}")
    new_rows = extract.get_league_game_log_from_date(fetch_from.strftime('%Y-%m-%d'), fetch_until.strftime('%Y-%m-%d'))
    new_rows = _canonicalise(new_rows)
    if new_rows is None or new_rows.empty:
        LOGGER.info("No new games found. Exiting...")
        return DailyUpdateResult(GAME_CSV, 0, appended=True, final_row_count=len(existing_game_df))

    _log_fetch_window(new_rows)

    # merge, dedup on lowercase keys, then atomic write
    if existing_game_df is not None and not existing_game_df.empty:
        combined_df = pd.concat([existing_game_df, new_rows], ignore_index=True)
    else:
        combined_df = new_rows

    # ensure season_type exists (Kaggle sometimes lacks it)
    if "season_type" not in combined_df.columns:
        combined_df["season_type"] = "Regular Season"

    combined_df.drop_duplicates(subset=["game_id", "team_id", "season_type"], keep="first", inplace=True)

    _atomic_write_csv(combined_df, GAME_CSV)
    final_count = len(combined_df)
    appended_rows = max(final_count - len(existing_game_df), 0)

    LOGGER.info(f"Wrote {final_count} total rows to {GAME_CSV}")

    return DailyUpdateResult(GAME_CSV, appended_rows, appended=True, final_row_count=final_count)


__all__ = ["DailyUpdateResult", "daily"]
