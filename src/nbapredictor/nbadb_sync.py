"""Utilities for synchronising data with the wyattowalsh/nbadb project."""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:  # pragma: no cover - optional dependency for lightweight environments
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - defer error until used
    pd = None  # type: ignore[assignment]
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
HISTORICAL_START_DATE = date(1946, 11, 1)
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
    processed_seasons: List[str] = field(default_factory=list)
    skipped_seasons: List[str] = field(default_factory=list)
    empty_seasons: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, object]:
        return {
            "processed_dates": [d.isoformat() for d in self.processed_dates],
            "downloaded_files": [str(p) for p in self.downloaded_files],
            "skipped_dates": [d.isoformat() for d in self.skipped_dates],
            "empty_dates": [d.isoformat() for d in self.empty_dates],
            "processed_seasons": list(self.processed_seasons),
            "skipped_seasons": list(self.skipped_seasons),
            "empty_seasons": list(self.empty_seasons),
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
    """Return the default start date for data ingestion.

    Unless the caller or manifest specifies an alternative, updates should begin
    from the first day of the inaugural BAA season (1 November 1946). The
    ``today`` argument is accepted for backwards compatibility with callers that
    may have relied on the previous behaviour in tests.
    """

    _ = today  # ``today`` is unused now but kept for compatibility.
    return HISTORICAL_START_DATE


def _parse_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return date_parser.parse(value).date()


def _season_for_date(target_date: date) -> str:
    season_start_year = target_date.year if target_date.month >= 7 else target_date.year - 1
    return f"{season_start_year}-{str(season_start_year + 1)[-2:]}"


def _season_start_year(target_date: date) -> int:
    return target_date.year if target_date.month >= 7 else target_date.year - 1


def _season_end_date(season_start_year: int) -> date:
    return date(season_start_year + 1, 6, 30)


def _historical_season_range(start: date, end: date) -> Iterable[tuple[int, str]]:
    start_year = _season_start_year(start)
    end_year = _season_start_year(end)
    for season_year in range(start_year, end_year + 1):
        yield season_year, f"{season_year}-{str(season_year + 1)[-2:]}"


def _fetch_game_logs_for_date(target_date: date, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    if pd is None:  # pragma: no cover - dependency is optional in unit tests
        raise ModuleNotFoundError("pandas is required to fetch game logs")
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


def _fetch_game_logs_for_season(season: str, retries: int = 3, pause: float = 1.5) -> pd.DataFrame:
    if pd is None:  # pragma: no cover - dependency is optional in unit tests
        raise ModuleNotFoundError("pandas is required to fetch game logs")

    frames: List[pd.DataFrame] = []
    for season_type in SEASON_TYPES:
        attempt = 0
        last_error: Optional[Exception] = None
        while attempt < retries:
            attempt += 1
            try:
                LOGGER.debug(
                    "Fetching league game log for season %s (%s, attempt %s)",
                    season,
                    season_type,
                    attempt,
                )
                endpoint = leaguegamelog.LeagueGameLog(
                    league_id="00",
                    season=season,
                    season_type_all_star=season_type,
                )
                frame = endpoint.get_data_frames()[0]
                if not frame.empty:
                    frame.insert(0, "SEASON_TYPE", season_type)
                    frames.append(frame)
                break
            except Exception as exc:  # noqa: BLE001 - third-party exceptions are opaque
                last_error = exc
                LOGGER.warning(
                    "Attempt %s/%s failed for season %s (%s): %s",
                    attempt,
                    retries,
                    season,
                    season_type,
                    exc,
                )
                time.sleep(pause * attempt)
        else:
            LOGGER.error(
                "Unable to download data for season %s (%s) after %s attempts.",
                season,
                season_type,
                retries,
            )
            if last_error:
                raise RuntimeError("Failed to download season stats") from last_error

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
    fetch_all_history: bool = False,
) -> UpdateSummary:
    """Update raw CSV dumps with daily statistics.

    Args:
        output_dir: Directory where raw CSV files should be written.
        start_date: Optional ISO date string overriding the computed start date.
            Defaults to ``1946-11-01`` when the manifest has no history.
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

    stop = _parse_date(end_date)
    if stop is None:
        stop = date.today() - timedelta(days=1)

    if fetch_all_history:
        start = HISTORICAL_START_DATE
    else:
        start = _parse_date(start_date)
        if start is None and manifest.get("last_updated"):
            start = _parse_date(manifest["last_updated"]) + timedelta(days=1)
        if start is None:
            start = _default_start_date()

    if start > stop:
        LOGGER.info("No updates required: start=%s stop=%s", start, stop)
        return UpdateSummary([], [], [], [])

    if fetch_all_history:
        processed_seasons: List[str] = []
        skipped_seasons: List[str] = []
        empty_seasons: List[str] = []
        downloaded: List[Path] = []

        for season_year, season_label in _historical_season_range(start, stop):
            season_dir = output_dir / "leaguegamelog" / f"season={season_label}"
            destination = season_dir / "part-000.csv"
            if destination.exists() and not force:
                LOGGER.debug("Skipping season %s; file already exists", season_label)
                skipped_seasons.append(season_label)
                manifest["last_updated"] = min(
                    stop, _season_end_date(season_year)
                ).strftime(DATE_FORMAT)
                _write_manifest(output_dir, manifest)
                continue

            LOGGER.info("Fetching stats for season %s", season_label)
            frame = _fetch_game_logs_for_season(season_label)
            if frame.empty:
                LOGGER.info("No games found in season %s", season_label)
                empty_seasons.append(season_label)
            else:
                season_dir.mkdir(parents=True, exist_ok=True)
                frame.to_csv(destination, index=False)
                downloaded.append(destination)
                LOGGER.info("Saved %s rows to %s", len(frame), destination)

            processed_seasons.append(season_label)
            manifest["last_updated"] = min(
                stop, _season_end_date(season_year)
            ).strftime(DATE_FORMAT)
            manifest.setdefault("historical_seasons", [])
            if season_label not in manifest["historical_seasons"]:
                manifest["historical_seasons"].append(season_label)
            _write_manifest(output_dir, manifest)

        return UpdateSummary(
            [],
            downloaded,
            [],
            [],
            processed_seasons=processed_seasons,
            skipped_seasons=skipped_seasons,
            empty_seasons=empty_seasons,
        )

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
