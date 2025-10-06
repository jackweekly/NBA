"""Data ingestion helpers emulating the ``wyattowalsh/nbadb`` project."""
from __future__ import annotations

import logging
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

import pandas as pd
from requests import exceptions as requests_exceptions
from nba_api.stats.static import players as static_players
from nba_api.stats.static import teams as static_teams
from nba_api.stats.endpoints import (
    boxscoresummaryv2,
    commonplayerinfo,
    leaguegamelog,
    teaminfocommon,
)

from .paths import game_log_path, load_config, raw_data_dir

SEASON_TYPES: tuple[str, ...] = (
    "Regular Season",
    "Playoffs",
    "Pre Season",
    "In Season Tournament",
)


GAME_LOG_PRIMARY_KEY: tuple[str, str, str] = ("game_id", "team_id", "season_type")
NUMERIC_KEY_COLUMNS: frozenset[str] = frozenset({"game_id", "team_id"})
DEFAULT_SEASON_TYPE = "Regular Season"
REQUEST_SLEEP_SECONDS = 0.5


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

DEFAULT_TIMEOUT = 20
SLOW_ENDPOINT_TIMEOUTS: tuple[int, ...] = (
    DEFAULT_TIMEOUT,
    DEFAULT_TIMEOUT * 2,
    DEFAULT_TIMEOUT * 4,
    DEFAULT_TIMEOUT * 4,
    DEFAULT_TIMEOUT * 4,
)
MAX_REQUEST_RETRIES = 5
MAX_BACKOFF_SECONDS = 16


LOGGER = logging.getLogger(__name__)


@dataclass
class WriteOutcome:
    """Result of persisting a DataFrame to disk."""

    appended_rows: int
    final_row_count: int


@dataclass
class DailyUpdateResult:
    """Summary describing a ``daily`` update."""

    output_path: Path
    rows_written: int
    appended: bool
    final_row_count: int

    def to_dict(self) -> dict[str, object]:
        return {
            "output_path": str(self.output_path),
            "rows_written": self.rows_written,
            "appended": self.appended,
            "final_row_count": self.final_row_count,
        }


@dataclass
class InitResult:
    """Outcome of the initial bootstrap."""

    player_path: Path
    team_path: Path
    game_path: Path
    game_summary_path: Path
    row_counts: dict[str, int]


def _season_start(year: int) -> date:
    return date(year, 7, 1)


def _season_end(year: int) -> date:
    return date(year + 1, 6, 30)


def _season_label(year: int) -> str:
    return f"{year}-{str(year + 1)[-2:]}"


def _canonicalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``frame`` with lower-case column names."""

    renamed = frame.copy()
    renamed.columns = [col.lower() for col in frame.columns]
    return renamed


def _standardise_game_log(frame: pd.DataFrame, *, season_type: Optional[str] = None) -> pd.DataFrame:
    """Apply canonical casing and dtype handling for game log data."""

    canonical = _canonicalize_columns(frame)
    if "game_date" in canonical.columns:
        canonical["game_date"] = pd.to_datetime(canonical["game_date"], errors="coerce")
    if season_type is not None:
        canonical["season_type"] = season_type
    elif "season_type" in canonical.columns:
        canonical["season_type"] = (
            canonical["season_type"].fillna(DEFAULT_SEASON_TYPE).replace("", DEFAULT_SEASON_TYPE)
        )
    else:
        canonical["season_type"] = DEFAULT_SEASON_TYPE
    return canonical


def _normalise_key_columns(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    """Normalise key columns in-place for deduplication comparisons."""

    for column in columns:
        if column not in frame.columns:
            continue
        series = frame[column].astype(str).str.strip()
        if column in NUMERIC_KEY_COLUMNS:
            series = series.str.lstrip("0").replace({"": "0"})
        frame[column] = series


def _deduplicate_game_log(frame: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows from a canonical game log frame."""

    canonical = _canonicalize_columns(frame)
    available_subset = [column for column in GAME_LOG_PRIMARY_KEY if column in canonical.columns]
    if not available_subset:
        return canonical.drop_duplicates(ignore_index=True)
    return canonical.drop_duplicates(subset=available_subset, keep="last", ignore_index=True)


def _read_game_log(path: Path) -> pd.DataFrame:
    """Load the consolidated game log, applying the canonical schema."""

    if not path.exists():
        return pd.DataFrame()
    frame = pd.read_csv(path)
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        return frame
    return _standardise_game_log(frame)


def _canonicalize_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of ``frame`` with lower-case column names."""

    if frame.empty:
        renamed = frame.copy()
        renamed.columns = [col.lower() for col in frame.columns]
        return renamed
    return frame.rename(columns=str.lower)


def _normalise_key_columns(frame: pd.DataFrame, columns: Sequence[str]) -> None:
    """Normalise key columns in-place for deduplication comparisons."""

    for column in columns:
        if column not in frame.columns:
            continue
        series = frame[column].astype(str).str.strip()
        if column in NUMERIC_KEY_COLUMNS:
            series = series.str.lstrip("0").replace({"": "0"})
        frame[column] = series


def _deduplicate_game_log(frame: pd.DataFrame) -> pd.DataFrame:
    """Drop duplicate rows from a canonical game log frame."""

    canonical = _canonicalize_columns(frame)
    available_subset = [column for column in GAME_LOG_PRIMARY_KEY if column in canonical.columns]
    if not available_subset:
        return canonical.drop_duplicates(ignore_index=True)
    return canonical.drop_duplicates(subset=available_subset, keep="last", ignore_index=True)


def _season_range(start: date, end: date) -> Iterable[int]:
    start_year = start.year if start.month >= 7 else start.year - 1
    end_year = end.year if end.month >= 7 else end.year - 1
    for season_year in range(start_year, end_year + 1):
        yield season_year


def _format_for_api(value: date) -> str:
    return value.strftime("%m/%d/%Y")


def _fetch_season_frame(
    season: str,
    *,
    date_from: Optional[date] = None,
    date_to: Optional[date] = None,
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    from_str = _format_for_api(date_from) if date_from else None
    to_str = _format_for_api(date_to) if date_to else None
    for season_type in SEASON_TYPES:
        frame = _call_with_retry(
            f"league game log {season} ({season_type})",
            lambda timeout, st=season_type: leaguegamelog.LeagueGameLog(
                league_id="00",
                season=season,
                season_type_all_star=st,
                date_from_nullable=from_str,
                date_to_nullable=to_str,
                headers=NBA_API_HEADERS,
                timeout=timeout,
            ).get_data_frames()[0],
        )
        if not frame.empty:
            frames.append(_standardise_game_log(frame, season_type=season_type))
        time.sleep(REQUEST_SLEEP_SECONDS)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return _standardise_game_log(combined)


def get_league_game_log_all() -> pd.DataFrame:
    today = date.today()
    start = date(1946, 11, 1)
    frames: list[pd.DataFrame] = []
    for season_year in _season_range(start, today):
        season = _season_label(season_year)
        season_frame = _fetch_season_frame(season)
        if not season_frame.empty:
            frames.append(season_frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return _deduplicate_game_log(combined)


def get_league_game_log_from_date(
    start_date: date,
    end_date: Optional[date] = None,
) -> pd.DataFrame:
    end_date = end_date or date.today()
    frames: list[pd.DataFrame] = []
    for season_year in _season_range(start_date, end_date):
        season = _season_label(season_year)
        season_start = max(start_date, _season_start(season_year))
        season_end = min(end_date, _season_end(season_year))
        season_frame = _fetch_season_frame(
            season,
            date_from=season_start,
            date_to=season_end,
        )
        if not season_frame.empty:
            frames.append(season_frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return _deduplicate_game_log(combined)


def _infer_game_date(frame: pd.DataFrame) -> pd.Series:
    canonical = _canonicalize_columns(frame)
    for column in ("game_date", "game_date_est", "game_date_time_est"):
        if column in canonical:
            return pd.to_datetime(canonical[column], errors="coerce")
    raise KeyError("Game log does not contain a date column")


def _next_start_date(game_csv_path: Path) -> date:
    if not game_csv_path.exists():
        raise FileNotFoundError(
            "Cannot infer start date because game.csv does not exist."
        )
    frame = _read_game_log(game_csv_path)
    if frame.empty:
        raise ValueError("Existing game.csv is empty")
    dates = _infer_game_date(frame)
    last = dates.dropna().max()
    if pd.isna(last):
        raise ValueError("Unable to infer last game date from game.csv")
    return (last.date() + timedelta(days=1))


def _atomic_write_csv(path: Path, frame: pd.DataFrame) -> None:
    """Write ``frame`` to ``path`` using an atomic replace."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        newline="",
        delete=False,
        dir=path.parent,
        prefix=f".{path.name}.",
        suffix=".tmp",
    ) as handle:
        temp_path = Path(handle.name)
        frame.to_csv(handle, index=False)
    os.replace(temp_path, path)


def _write_dataframe(
    path: Path,
    frame: pd.DataFrame,
    *,
    append: bool,
    deduplicate_subset: Optional[Sequence[str]] = None,
) -> WriteOutcome:
    """Persist ``frame`` to ``path`` while optionally removing duplicates."""

    subset = [column.lower() for column in (deduplicate_subset or [])]
    if subset:
        existing = _read_game_log(path) if append else pd.DataFrame()
        frame_to_write = _standardise_game_log(frame)
    else:
        existing = pd.read_csv(path) if append and path.exists() else pd.DataFrame()
        frame_to_write = frame.copy()

    existing_count = len(existing)

    if not append and frame_to_write.empty:
        return WriteOutcome(appended_rows=0, final_row_count=0)
    if append and frame_to_write.empty:
        return WriteOutcome(appended_rows=0, final_row_count=existing_count)

    if append and not existing.empty:
        combined = pd.concat([existing, frame_to_write], ignore_index=True)
    else:
        combined = frame_to_write.copy()

    if subset:
        available_subset = [column for column in subset if column in combined.columns]
        if available_subset:
            _normalise_key_columns(combined, available_subset)
            combined = combined.drop_duplicates(subset=available_subset, keep="first", ignore_index=True)
        else:
            combined = combined.drop_duplicates(ignore_index=True)
    else:
        combined = combined.drop_duplicates(ignore_index=True)

    final_row_count = len(combined)
    appended_rows = final_row_count - existing_count

    _atomic_write_csv(path, combined)
    return WriteOutcome(appended_rows=appended_rows, final_row_count=final_row_count)


def _call_with_retry(
    description: str,
    func: Callable[[int | float | tuple[int, int]], pd.DataFrame],
    *,
    timeouts: Optional[Sequence[int | float | tuple[int, int]]] = None,
) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    delay = 1
    timeout_plan: Sequence[int | float | tuple[int, int]]
    if timeouts is None:
        timeout_plan = (DEFAULT_TIMEOUT,) * MAX_REQUEST_RETRIES
    elif not timeouts:
        timeout_plan = (DEFAULT_TIMEOUT,) * MAX_REQUEST_RETRIES
    else:
        timeout_plan = timeouts

    for attempt in range(1, MAX_REQUEST_RETRIES + 1):
        timeout_value = timeout_plan[min(attempt - 1, len(timeout_plan) - 1)]
        try:
            return func(timeout_value)
        except requests_exceptions.RequestException as exc:  # pragma: no cover - network
            last_error = exc
            if attempt == MAX_REQUEST_RETRIES:
                break
            time.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)
    if last_error is not None:  # pragma: no cover - network
        raise RuntimeError(f"Failed to fetch {description}") from last_error
    raise RuntimeError(f"Failed to fetch {description}")


def _fetch_all_players() -> pd.DataFrame:
    players = static_players.get_players()
    frames: list[pd.DataFrame] = []
    for meta in players:
        frame = _call_with_retry(
            f"player {meta['id']}",
            lambda timeout, meta_id=meta["id"]: commonplayerinfo.CommonPlayerInfo(
                player_id=meta_id,
                headers=NBA_API_HEADERS,
                timeout=timeout,
            ).get_data_frames()[0],
            timeouts=SLOW_ENDPOINT_TIMEOUTS,
        )
        if not frame.empty:
            frames.append(frame)
        time.sleep(REQUEST_SLEEP_SECONDS)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_all_teams() -> pd.DataFrame:
    teams = static_teams.get_teams()
    frames: list[pd.DataFrame] = []
    for meta in teams:
        frame = _call_with_retry(
            f"team {meta['id']}",
            lambda timeout, meta_id=meta["id"]: teaminfocommon.TeamInfoCommon(
                team_id=meta_id,
                headers=NBA_API_HEADERS,
                timeout=timeout,
            ).get_data_frames()[0],
            timeouts=SLOW_ENDPOINT_TIMEOUTS,
        )
        if not frame.empty:
            frames.append(frame)
        time.sleep(REQUEST_SLEEP_SECONDS)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_game_summaries(game_ids: Sequence[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for game_id in game_ids:
        frame = _call_with_retry(
            f"game summary {game_id}",
            lambda timeout, gid=game_id: boxscoresummaryv2.BoxScoreSummaryV2(
                game_id=gid,
                headers=NBA_API_HEADERS,
                timeout=timeout,
            ).get_data_frames()[0],
            timeouts=SLOW_ENDPOINT_TIMEOUTS,
        )
        if not frame.empty:
            frames.append(frame)
        time.sleep(REQUEST_SLEEP_SECONDS)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def daily(
    *,
    fetch_all_history: bool = False,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    output_dir: Optional[Path | str] = None,
) -> DailyUpdateResult:
    config = load_config()
    raw_dir = raw_data_dir(config=config, override=output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    game_csv_path = game_log_path(config=config, override=output_dir)

    today_local = date.today()

    if fetch_all_history:
        LOGGER.info("Starting full-history refresh into %s", game_csv_path)
        frame = get_league_game_log_all()
        outcome = _write_dataframe(
            game_csv_path,
            frame,
            append=False,
            deduplicate_subset=GAME_LOG_PRIMARY_KEY,
        )
        LOGGER.info(
            "Full-history refresh complete; final row count %s",
            outcome.final_row_count,
        )
        return DailyUpdateResult(
            game_csv_path,
            outcome.appended_rows,
            appended=False,
            final_row_count=outcome.final_row_count,
        )

    if not game_csv_path.exists():
        raise FileNotFoundError("Cannot run incremental update before bootstrap is written")

    start = date.fromisoformat(start_date) if start_date else _next_start_date(game_csv_path)
    stop = date.fromisoformat(end_date) if end_date else None

    if start >= today_local:
        LOGGER.info("No new games to fetch; latest recorded date is %s", (start - timedelta(days=1)).isoformat())
        existing = _read_game_log(game_csv_path)
        return DailyUpdateResult(
            game_csv_path,
            0,
            appended=True,
            final_row_count=len(existing),
        )

    if stop is not None:
        stop = min(stop, today_local - timedelta(days=1))
        if stop < start:
            LOGGER.info("Requested update window %s-%s is empty", start.isoformat(), stop.isoformat())
            existing = _read_game_log(game_csv_path)
            return DailyUpdateResult(
                game_csv_path,
                0,
                appended=True,
                final_row_count=len(existing),
            )

    frame = get_league_game_log_from_date(start, stop)
    frame = _standardise_game_log(frame)
    if frame.empty:
        LOGGER.info(
            "No games returned between %s and %s",
            start.isoformat(),
            (stop or today_local - timedelta(days=1)).isoformat(),
        )
        existing = _read_game_log(game_csv_path)
        return DailyUpdateResult(
            game_csv_path,
            0,
            appended=True,
            final_row_count=len(existing),
        )

    valid_dates = frame["game_date"].dropna()
    if not valid_dates.empty:
        LOGGER.info(
            "Fetched %s rows covering %s → %s",
            len(frame),
            valid_dates.min().date().isoformat(),
            valid_dates.max().date().isoformat(),
        )
    else:
        LOGGER.info("Fetched %s rows with no valid game_date values", len(frame))

    existed_before = game_csv_path.exists()
    outcome = _write_dataframe(
        game_csv_path,
        frame,
        append=existed_before,
        deduplicate_subset=GAME_LOG_PRIMARY_KEY,
    )
    LOGGER.info("Final game log row count: %s", outcome.final_row_count)
    return DailyUpdateResult(
        game_csv_path,
        outcome.appended_rows,
        appended=existed_before,
        final_row_count=outcome.final_row_count,
    )


def init(output_dir: Optional[Path | str] = None) -> InitResult:
    config = load_config()
    raw_dir = raw_data_dir(config=config, override=output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)

    player_path = raw_dir / "common_player_info.csv"
    team_path = raw_dir / "team_info_common.csv"
    game_path = raw_dir / "game.csv"
    game_summary_path = raw_dir / "game_summary.csv"

    players_frame = _fetch_all_players()
    teams_frame = _fetch_all_teams()
    games_frame = get_league_game_log_all()
    summaries_frame = _fetch_game_summaries(games_frame["GAME_ID"].tolist()) if not games_frame.empty else pd.DataFrame()

    row_counts = {
        "players": _write_dataframe(player_path, players_frame, append=False).final_row_count if not players_frame.empty else 0,
        "teams": _write_dataframe(team_path, teams_frame, append=False).final_row_count if not teams_frame.empty else 0,
        "games": _write_dataframe(
            game_path,
            games_frame,
            append=False,
            deduplicate_subset=GAME_LOG_PRIMARY_KEY,
        ).final_row_count
        if not games_frame.empty
        else 0,
        "game_summaries": _write_dataframe(game_summary_path, summaries_frame, append=False).final_row_count if not summaries_frame.empty else 0,
    }

    return InitResult(
        player_path=player_path,
        team_path=team_path,
        game_path=game_path,
        game_summary_path=game_summary_path,
        row_counts=row_counts,
    )


__all__ = [
    "DailyUpdateResult",
    "InitResult",
    "WriteOutcome",
    "daily",
    "get_league_game_log_all",
    "get_league_game_log_from_date",
    "init",
]
