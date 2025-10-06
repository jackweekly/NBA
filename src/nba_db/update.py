"""Incremental game log updater built around the Kaggle bootstrap seed."""
from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta
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














def _log_fetch_window(new_rows: pd.DataFrame) -> None:
    if "game_date" not in new_rows.columns:
        LOGGER.info("Fetched %s new rows (game_date column missing)", len(new_rows))
        return

    valid_dates = new_rows["game_date"].dropna()
    if valid_dates.empty:
        LOGGER.info("Fetched %s new rows with unknown game_date values", len(new_rows))
        return

    window = f"{valid_dates.min().date()} → {valid_dates.max().date()}"
    LOGGER.info("Fetched %s new rows covering %s", len(new_rows), window)

    for column in ("season_id", "game_id", "team_id"):
        if column not in canonical.columns:
            continue
        series = canonical[column].astype(str).str.strip()
        if column in {"game_id", "team_id"}:
            series = series.str.lstrip("0").replace({"": "0"})
        canonical[column] = series

def daily(
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> DailyUpdateResult:
    """Fetch and append the latest NBA games into ``data/raw/game.csv``."""

    today_local = date.today()

    # read existing and canonicalise before using it
    if GAME_CSV.exists():
        existing_game_df = pd.read_csv(GAME_CSV)
        existing_game_df = _canonicalise(existing_game_df)
        if not existing_game_df.empty:
            fetch_from_date = existing_game_df["game_date"].max()
            # bump +1 day only for normal daily (no explicit backfill)
            if not start_date:
                fetch_from_date = (pd.to_datetime(fetch_from_date) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        existing_game_df = pd.DataFrame()

    if start_date:
        fetch_from = start_date
    elif not existing_game_df.empty:
        fetch_from = (pd.to_datetime(existing_game_df["game_date"].max()) + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    else:
        # If no existing data and no start_date, start from a reasonable default, e.g., 2023-01-01
        fetch_from = "2023-01-01" # Or some other sensible default

    if end_date:
        fetch_until = end_date
    else:
        fetch_until = today_local.strftime("%Y-%m-%d")

    # ... after you obtain new_games_df ...
    new_rows = extract.get_league_game_log_from_date(fetch_from, fetch_until)
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
