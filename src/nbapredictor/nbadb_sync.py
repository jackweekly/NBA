"""Utilities for synchronising data with the wyattowalsh/nbadb project."""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
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

NBA_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
}

DEFAULT_TIMEOUT = 15


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


@dataclass
class KaggleBootstrapResult:
    """Details about the outcome of a Kaggle bootstrap."""

    dataset_dir: Path
    downloaded_files: List[Path]
    processed_seasons: List[str]
    skipped_seasons: List[str]
    last_game_date: Optional[date]


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
                    headers=NBA_API_HEADERS,
                    timeout=DEFAULT_TIMEOUT,
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
                    headers=NBA_API_HEADERS,
                    timeout=DEFAULT_TIMEOUT,
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


def bootstrap_kaggle_dump(destination: Path) -> Path:
    """Download the Kaggle dataset backing the ``nbadb`` project.

    Args:
        destination: Directory where the Kaggle dataset should be extracted.

    Returns:
        The directory containing the downloaded dataset.
    """

    destination = Path(destination)
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
    return destination


def _bootstrap_from_kaggle(
    output_dir: Path, *, force: bool = False, dataset_dir: Optional[Path] = None
) -> KaggleBootstrapResult:
    """Download and ingest the upstream Kaggle dump into ``output_dir``."""

    if pd is None:  # pragma: no cover - dependency is optional in unit tests
        raise ModuleNotFoundError("pandas is required to bootstrap the Kaggle dump")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    staging_root = (
        Path(dataset_dir)
        if dataset_dir is not None
        else output_dir.parent / "external" / "wyatt"
    )
    staging_root.mkdir(parents=True, exist_ok=True)

    dataset_location = bootstrap_kaggle_dump(staging_root)

    manifest = _load_manifest(output_dir)

    game_candidates = sorted(dataset_location.rglob("game.csv"))
    if not game_candidates:
        raise FileNotFoundError(
            "Unable to locate 'game.csv' inside the Kaggle dataset."
        )

    LOGGER.info("Loading Kaggle league game log from %s", game_candidates[0])
    game_frame = pd.read_csv(game_candidates[0])
    if game_frame.empty:
        LOGGER.warning("The Kaggle league game log is empty; nothing to import")
        manifest.setdefault("historical_seasons", [])
        manifest["bootstrap"] = {
            "source": "kaggle",
            "dataset": KAGGLE_DATASET_ID,
            "dataset_dir": str(dataset_location),
            "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "rows": 0,
        }
        _write_manifest(output_dir, manifest)
        return KaggleBootstrapResult(
            dataset_dir=dataset_location,
            downloaded_files=[],
            processed_seasons=[],
            skipped_seasons=[],
            last_game_date=None,
        )

    date_column: Optional[str] = None
    for candidate in ("GAME_DATE", "GAME_DATE_EST", "GAME_DATE_TIME_EST"):
        if candidate in game_frame.columns:
            date_column = candidate
            break
    if date_column is None:
        raise KeyError(
            "The Kaggle game log is missing a GAME_DATE column; cannot infer seasons."
        )

    game_dates = pd.to_datetime(game_frame[date_column], errors="coerce")
    if hasattr(game_dates.dt, "tz") and game_dates.dt.tz is not None:
        game_dates = game_dates.dt.tz_convert(None)
    if hasattr(game_dates.dt, "tz_localize"):
        try:
            game_dates = game_dates.dt.tz_localize(None)
        except TypeError:
            # pandas < 2.2 raises TypeError when localising timezone-aware series
            pass
    game_frame = game_frame.assign(_INGEST_GAME_DATE=game_dates.dt.date)
    game_frame = game_frame[game_frame["_INGEST_GAME_DATE"].notna()].copy()
    if game_frame.empty:
        raise ValueError("Kaggle game log does not contain valid GAME_DATE values")

    game_frame["_INGEST_SEASON"] = [
        _season_for_date(game_date) for game_date in game_frame["_INGEST_GAME_DATE"]
    ]

    downloaded: List[Path] = []
    processed_seasons: List[str] = []
    skipped_seasons: List[str] = []

    seasons_in_dataset = sorted(game_frame["_INGEST_SEASON"].unique())
    for season_label in seasons_in_dataset:
        season_frame = game_frame[game_frame["_INGEST_SEASON"] == season_label].drop(
            columns=["_INGEST_GAME_DATE", "_INGEST_SEASON"]
        )
        season_dir = output_dir / "leaguegamelog" / f"season={season_label}"
        destination = season_dir / "part-000.csv"
        if destination.exists() and not force:
            LOGGER.debug(
                "Skipping Kaggle import for season %s; destination exists", season_label
            )
            skipped_seasons.append(season_label)
            continue
        season_dir.mkdir(parents=True, exist_ok=True)
        season_frame.to_csv(destination, index=False)
        downloaded.append(destination)
        processed_seasons.append(season_label)

    last_game_date = max(game_frame["_INGEST_GAME_DATE"])

    historical_seasons = set(manifest.get("historical_seasons", []))
    historical_seasons.update(seasons_in_dataset)
    manifest["historical_seasons"] = sorted(historical_seasons)
    manifest["last_updated"] = last_game_date.strftime(DATE_FORMAT)
    manifest["bootstrap"] = {
        "source": "kaggle",
        "dataset": KAGGLE_DATASET_ID,
        "dataset_dir": str(dataset_location),
        "imported_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rows": int(len(game_frame)),
    }
    _write_manifest(output_dir, manifest)

    return KaggleBootstrapResult(
        dataset_dir=dataset_location,
        downloaded_files=downloaded,
        processed_seasons=processed_seasons,
        skipped_seasons=skipped_seasons,
        last_game_date=last_game_date,
    )


def bootstrap_from_kaggle(
    output_dir: Path | str = Path("data/raw"),
    *,
    force: bool = False,
    dataset_dir: Optional[Path] = None,
) -> KaggleBootstrapResult:
    """Public wrapper around :func:`_bootstrap_from_kaggle`."""

    return _bootstrap_from_kaggle(
        Path(output_dir), force=force, dataset_dir=dataset_dir
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

    downloaded: List[Path] = []
    processed_seasons: List[str] = []
    skipped_seasons: List[str] = []
    empty_seasons: List[str] = []

    if bootstrap_kaggle:
        bootstrap_result = _bootstrap_from_kaggle(output_dir, force=force)
        downloaded.extend(bootstrap_result.downloaded_files)
        processed_seasons.extend(bootstrap_result.processed_seasons)
        skipped_seasons.extend(bootstrap_result.skipped_seasons)
        manifest = _load_manifest(output_dir)
    else:
        manifest = _load_manifest(output_dir)
    metadata = fetch_dataset_metadata(session=session)
    LOGGER.info("Fetched upstream metadata: %s", metadata.get("title", "unknown"))

    stop = _parse_date(end_date)
    if stop is None:
        stop = date.today()

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
        return UpdateSummary(
            [],
            downloaded,
            [],
            [],
            processed_seasons=processed_seasons,
            skipped_seasons=skipped_seasons,
            empty_seasons=empty_seasons,
        )

    if fetch_all_history:
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

    return UpdateSummary(
        processed,
        downloaded,
        skipped,
        empty,
        processed_seasons=processed_seasons,
        skipped_seasons=skipped_seasons,
        empty_seasons=empty_seasons,
    )


__all__ = [
    "UpdateSummary",
    "KaggleBootstrapResult",
    "bootstrap_kaggle_dump",
    "bootstrap_from_kaggle",
    "fetch_dataset_metadata",
    "update_raw_data",
]
