"""Incremental game log updater built around the Kaggle bootstrap seed."""
from __future__ import annotations

import logging
import os
import tempfile
from dataclasses import dataclass
from datetime import date, timedelta, datetime
from pandas.tseries.offsets import MonthEnd
from pandas import Timestamp
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
        existing_game_df = pd.read_csv(GAME_CSV, usecols=["game_date", "game_id", "season_id", "season_type"], dtype_backend="pyarrow")
        existing_game_df = _canonicalise(existing_game_df)
        if not existing_game_df.empty and "game_date" in existing_game_df.columns:
            return existing_game_df["game_date"].max().date()
    raise FileNotFoundError(
        f"Game CSV not found at {GAME_CSV}. Please run `run_init.py` to initialize the database."
    )




def _log_fetch_window(df: pd.DataFrame):
    df = _canonicalise(df)
    if df is None or df.empty:
        LOGGER.info("Fetched 0 rows.")
        return
    if "game_date" in df.columns:
        min_date = df['game_date'].min()
        max_date = df['game_date'].max()
        LOGGER.info(f"Fetched {len(df)} new rows covering {min_date} \u2192 {max_date}")
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
        existing_game_df = pd.read_csv(GAME_CSV, dtype_backend="pyarrow")
        existing_game_df = _canonicalise(existing_game_df)
    else:
        existing_game_df = pd.DataFrame()


    if existing_game_df.empty or "game_date" not in existing_game_df.columns:
        fetch_from = pd.Timestamp("2020-01-01")  # or first date you want
    else:
        last = existing_game_df["game_date"].max()
        if pd.isna(last):
            fetch_from = pd.Timestamp("2020-01-01")
        else:
            fetch_from = (last + pd.Timedelta(days=1)).normalize()

    today = pd.Timestamp.today().normalize()
    fetch_until = min(today, pd.to_datetime(end_date).normalize()) if end_date else today

    if start_date:
        fetch_from = pd.to_datetime(start_date).normalize()

    if fetch_from > fetch_until:
        LOGGER.info("No new games today. Exiting...")
        return DailyUpdateResult(GAME_CSV, 0, appended=True, final_row_count=len(existing_game_df))

    LOGGER.info(f"Resume window computed as {fetch_from.date()} → {fetch_until.date()}")

    cursor = fetch_from
    end    = fetch_until

    collected = []
    while cursor <= end:
        month_end = (cursor + pd.offsets.MonthEnd(0))
        window_end = min(month_end, end)

        LOGGER.info(f"Fetching data from {cursor.date()} to {window_end.date()}")

        # Single discovery call (or minimal per-season_type)
        df_win = extract.get_league_game_log_from_date(
            cursor.strftime("%Y-%m-%d"),
            window_end.strftime("%Y-%m-%d"),
        )

        if df_win is not None and not df_win.empty:
            collected.append(df_win)

        # advance to first day of next month
        cursor = (month_end + pd.Timedelta(days=1)).normalize()

    # After the loop:
    if not collected:
        LOGGER.info("No new games found across the entire requested window. Exiting...")
        return DailyUpdateResult(GAME_CSV, 0, appended=True, final_row_count=len(existing_game_df))

    new_rows = pd.concat(collected, ignore_index=True)
    new_rows = _canonicalise(new_rows)

    # ensure season_type exists (Kaggle sometimes lacks it)
    if "season_type" not in new_rows.columns:
        new_rows["season_type"] = "Regular Season"

    if GAME_CSV.exists():
        new_rows.to_csv(GAME_CSV, mode="a", index=False, header=False)
        appended = True
    else:
        new_rows.to_csv(GAME_CSV, index=False)
        appended = False

    LOGGER.info(f"Daily update wrote {len(new_rows)} new rows to {GAME_CSV} (appended={appended})")

    return DailyUpdateResult(GAME_CSV, len(new_rows), appended=appended, final_row_count=len(existing_game_df) + len(new_rows) if appended else len(new_rows))


__all__ = ["DailyUpdateResult", "daily"]
