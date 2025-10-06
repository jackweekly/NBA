"""Utilities for synchronising data with the wyattowalsh/nbadb project."""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests
from dateutil import parser as date_parser
from nba_api.stats.endpoints import leaguegamelog

LOGGER = logging.getLogger(__name__)

NBADB_DATASET_METADATA_URL = (
    "https://raw.githubusercontent.com/wyattowalsh/nbadb/main/dataset-metadata.json"
)
KAGGLE_DATASET_ID = "wyattowalsh/basketball"
MANIFEST_FILENAME = "manifest.json"
DATE_FORMAT = "%Y-%m-%d"
SEASON_TYPES: tuple[str, ...] = (
    "Regular Season",
    "Playoffs",
    "PlayIn",
    "Pre Season",
)


@dataclass
class UpdateSummary:
    """Container summarising an update run."""

    processed_dates: List[date]
    downloaded_files: List[Path]
    skipped_dates: List[date]
    empty_dates: List[date]

    def to_dict(self) -> Dict[str, object]:
        return {
            "processed_dates": [d.isoformat() for d in self.processed_dates],
            "downloaded_files": [str(p) for p in self.downloaded_files],
            "skipped_dates": [d.isoformat() for d in self.skipped_dates],
            "empty_dates": [d.isoformat() for d in self.empty_dates],
        }


def fetch_dataset_metadata(session: Optional[requests.Session] = None) -> Dict[str, object]:
    """Retrieve the metadata file published in the ``nbadb`` repository."""

    session = session or requests.Session()
    response = session.get(NBADB_DATASET_METADATA_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def _manifest_path(output_dir: Path) -> Path:
    return output_dir / MANIFEST_FILENAME


def _load_manifest(output_dir: Path) -> Dict[str, object]:
    manifest_file = _manifest_path(output_dir)
    if not manifest_file.exists():
        return {}
    with manifest_file.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def _write_manifest(output_dir: Path, payload: Dict[str, object]) -> None:
    manifest_file = _manifest_path(output_dir)
    with manifest_file.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)


def _default_start_date(today: Optional[date] = None) -> date:
    today = today or date.today()
    season_start_year = today.year if today.month >= 7 else today.year - 1
    return date(season_start_year, 10, 1)


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date_parser.parse(value).date()


def _season_for_date(target_date: date) -> str:
    season_start_year = target_date.year if target_date.month >= 7 else target_date.year - 1
    return f"{season_start_year}-{str(season_start_year + 1)[-2:]}"


def _fetch_game_logs_for_date(target_date: date, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    season = _season_for_date(target_date)
    date_str = target_date.strftime("%m/%d/%Y")
    frames: List[pd.DataFrame] = []
    for season_type in SEASON_TYPES:
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt < retries:
            attempt += 1
            try:
                LOGGER.debug(
                    "Fetching league game log for %s (%s, attempt %s)",
                    date_str,
                    season_type,
                    attempt,
                )
                endpoint = leaguegamelog.LeagueGameLog(
                    league_id="00",
                    season=season,
                    season_type_all_star=season_type,
                    date_from_nullable=date_str,
                    date_to_nullable=date_str,
                )
                frame = endpoint.get_data_frames()[0]
                if not frame.empty:
                    frame.insert(0, "SEASON_TYPE", season_type)
                    frames.append(frame)
                break
            except Exception as exc:  # noqa: BLE001 - third-party exceptions are opaque
                last_error = exc
                LOGGER.warning(
                    "Attempt %s/%s failed for %s (%s): %s",
                    attempt,
                    retries,
                    date_str,
                    season_type,
                    exc,
                )
                time.sleep(pause * attempt)
        else:
            LOGGER.error(
                "Unable to download data for %s (%s) after %s attempts.",
                date_str,
                season_type,
                retries,
            )
            if last_error:
                raise RuntimeError("Failed to download daily stats") from last_error
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _date_range(start: date, end: date) -> Iterable[date]:
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def bootstrap_kaggle_dump(output_dir: Path) -> None:
    """Download the Kaggle dataset backing the ``nbadb`` project.

    The Kaggle CLI must be installed and authenticated. The dataset is unpacked
    into ``output_dir / 'nbadb'``.
    """

    destination = output_dir / "nbadb"
    destination.mkdir(parents=True, exist_ok=True)
    if shutil.which("kaggle") is None:
        raise FileNotFoundError(
            "The 'kaggle' CLI is required to bootstrap the upstream dataset."
        )
    LOGGER.info("Downloading Kaggle dataset %s to %s", KAGGLE_DATASET_ID, destination)
    subprocess.run(
        [
            "kaggle",
            "datasets",
            "download",
            "--unzip",
            "-p",
            str(destination),
            "-d",
            KAGGLE_DATASET_ID,
        ],
        check=True,
    )


def update_raw_data(
    output_dir: Path | str = Path("data/raw"),
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bootstrap_kaggle: bool = False,
    force: bool = False,
    session: Optional[requests.Session] = None,
) -> UpdateSummary:
    """Update raw CSV dumps with daily statistics.

    Args:
        output_dir: Directory where raw CSV files should be written.
        start_date: Optional ISO date string overriding the computed start date.
        end_date: Optional ISO date string to stop updates (inclusive). Defaults to
            ``yesterday``.
        bootstrap_kaggle: Whether to fetch the Kaggle dataset before processing.
        force: If ``True``, CSV files are regenerated even when they already exist.
        session: Optional :class:`requests.Session` reused for metadata lookups.

    Returns:
        :class:`UpdateSummary` describing what happened during the run.
    """

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if bootstrap_kaggle:
        bootstrap_kaggle_dump(output_dir)

    manifest = _load_manifest(output_dir)
    metadata = fetch_dataset_metadata(session=session)
    LOGGER.info("Fetched upstream metadata: %s", metadata.get("title", "unknown"))

    start = _parse_date(start_date)
    if start is None and manifest.get("last_updated"):
        start = _parse_date(manifest["last_updated"]) + timedelta(days=1)
    if start is None:
        start = _default_start_date()

    stop = _parse_date(end_date)
    if stop is None:
        stop = date.today() - timedelta(days=1)

    if start > stop:
        LOGGER.info("No updates required: start=%s stop=%s", start, stop)
        return UpdateSummary([], [], [], [])

    processed: List[date] = []
    downloaded: List[Path] = []
    skipped: List[date] = []
    empty: List[date] = []

    for target_date in _date_range(start, stop):
        processed.append(target_date)
        filename = f"{target_date.strftime(DATE_FORMAT)}_leaguegamelog.csv"
        destination = output_dir / filename
        if destination.exists() and not force:
            LOGGER.debug("Skipping %s; file already exists", destination)
            skipped.append(target_date)
            manifest["last_updated"] = target_date.strftime(DATE_FORMAT)
            continue
        LOGGER.info("Fetching stats for %s", target_date)
        frame = _fetch_game_logs_for_date(target_date)
        if frame.empty:
            LOGGER.info("No games found on %s", target_date)
            empty.append(target_date)
        else:
            frame.to_csv(destination, index=False)
            downloaded.append(destination)
            LOGGER.info("Saved %s rows to %s", len(frame), destination)
        manifest["last_updated"] = target_date.strftime(DATE_FORMAT)
        _write_manifest(output_dir, manifest)

    return UpdateSummary(processed, downloaded, skipped, empty)


__all__ = [
    "UpdateSummary",
    "bootstrap_kaggle_dump",
    "fetch_dataset_metadata",
    "update_raw_data",
]
