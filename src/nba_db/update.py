"""Wrapper utilities that delegate to :mod:`nbapredictor.nbadb_sync`."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from nbapredictor import nbadb_sync


@dataclass
class DailyUpdateResult:
    """Mirror the structure returned by ``nba_db.update.daily`` in upstream repo."""

    summary: nbadb_sync.UpdateSummary


def _normalise_start_date(
    *, fetch_all_history: bool, start_date: Optional[str]
) -> Optional[str]:
    if fetch_all_history:
        return nbadb_sync.HISTORICAL_START_DATE.isoformat()
    return start_date


def daily(
    *,
    fetch_all_history: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Path | str = Path("data/raw"),
    bootstrap_kaggle: bool = False,
    force: bool = False,
) -> DailyUpdateResult:
    """Delegate a daily update run to :func:`update_raw_data`."""

    effective_start = _normalise_start_date(
        fetch_all_history=fetch_all_history, start_date=start_date
    )
    summary = nbadb_sync.update_raw_data(
        output_dir=output_dir,
        start_date=effective_start,
        end_date=end_date,
        bootstrap_kaggle=bootstrap_kaggle,
        force=force,
        fetch_all_history=fetch_all_history,
    )
    return DailyUpdateResult(summary=summary)


def init(output_dir: Path | str = Path("data/raw")) -> None:
    """Run the one-off bootstrap stage by pulling the Kaggle dump."""

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    nbadb_sync.bootstrap_kaggle_dump(destination)


__all__ = ["DailyUpdateResult", "daily", "init"]
