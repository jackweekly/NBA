"""NBA Stats API helpers for incremental game log updates."""
from __future__ import annotations

import logging
import random
import time
from datetime import date
from typing import Optional

import pandas as pd
import requests
from requests import HTTPError, exceptions as requests_exceptions
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from .utils import get_proxies


LOGGER = logging.getLogger(__name__)

SEASON_TYPES = [
    "Regular Season",
    "Pre Season",
    "PlayIn",
    "Playoffs",
    "In-Season Tournament",
]

SEASON_TYPE_CANON = {
    "regular season": "Regular Season",
    "pre season": "Pre Season",
    "preseason": "Pre Season",
    "playin": "PlayIn",
    "play-in": "PlayIn",
    "playoffs": "Playoffs",
    "post season": "Playoffs",
    "in season tournament": "In-Season Tournament",
    "in-season tournament": "In-Season Tournament",
}

NBA_API_URL = "https://stats.nba.com/stats/leaguegamelog"
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

REQUEST_SLEEP_SECONDS = 1.0
DEFAULT_TIMEOUT = 10
MAX_RETRIES = 5
MAX_BACKOFF_SECONDS = 10


def _season_year_for_date(value: date) -> int:
    return value.year if value.month >= 7 else value.year - 1


def _season_label(year: int) -> str:
    return f"{year}-{str(year + 1)[-2:]}"


def _latest_started_season_year(today: date) -> int:
    season_start = date(today.year, 10, 1)
    return today.year if today >= season_start else today.year - 1


def _format_for_api(value: Optional[date]) -> Optional[str]:
    return value.strftime("%m/%d/%Y") if value else None


def _mmddyyyy(value: pd.Timestamp) -> str:
    return value.strftime("%m/%d/%Y")



def season_types_for_window(start: pd.Timestamp, end: pd.Timestamp) -> list[str]:
    """Return plausible season types for a batched window."""

    types: set[str] = set()
    if start.month <= 10 <= end.month or start.month == 9 or end.month == 9:
        types.add("Pre Season")
    types.add("Regular Season")
    if end.month >= 11:
        types.add("In-Season Tournament")
    if end.month >= 4:
        types.update({"PlayIn", "Playoffs"})
    return sorted(types)


def _canon_season_type(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return SEASON_TYPE_CANON.get(value.strip().lower(), value)


def _call_with_retry(description: str, func):
    delay = 4
    attempts = 0
    while True:
        try:
            return func()
        except requests_exceptions.RequestException as exc:
            attempts += 1
            if attempts >= MAX_RETRIES:
                raise RuntimeError(f"Failed to fetch {description}") from exc
            sleep_for = min(delay, MAX_BACKOFF_SECONDS)
            LOGGER.warning(
                "%s failed (%s); retrying in %ss", description, exc.__class__.__name__, sleep_for
            )
            time.sleep(sleep_for)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)


def _normalise_frame(frame: pd.DataFrame, season_type: str) -> pd.DataFrame:
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        frame["season_type"] = season_type
        return frame

    normalised = frame.copy()
    normalised.columns = normalised.columns.str.lower()
    if "game_date" in normalised.columns:
        normalised["game_date"] = pd.to_datetime(normalised["game_date"], errors="coerce")
    normalised["season_type"] = season_type
    return normalised


def _fetch_leaguegamelog(
    *,
    season: str,
    season_type: Optional[str],
    formatted_from: str,
    formatted_to: str,
    timeout: int,
    proxy: Optional[str],
) -> pd.DataFrame:
    params = {
        "Counter": 0,
        "Direction": "ASC",
        "LeagueID": "00",
        "PlayerOrTeam": "T",
        "Season": season,
        "SeasonType": _canon_season_type(season_type) if season_type else "",
        "Sorter": "DATE",
        "DateFrom": formatted_from,
        "DateTo": formatted_to,
    }
    proxies = {"http": proxy, "https": proxy} if proxy else None

    response = requests.get(
        NBA_API_URL,
        params=params,
        headers=NBA_API_HEADERS,
        timeout=timeout,
        proxies=proxies,
    )
    try:
        response.raise_for_status()
    except HTTPError as exc:  # pragma: no cover - non-retriable errors handled upstream
        status = getattr(exc.response, "status_code", None)
        if status == 400:
            return pd.DataFrame()
        raise
    payload = response.json()

    datasets = payload.get("resultSets") or payload.get("resultSet")
    if isinstance(datasets, dict):
        target = datasets
    elif isinstance(datasets, list) and datasets:
        target = next((item for item in datasets if item.get("name") == "LeagueGameLog"), datasets[0])
    else:
        target = {}

    headers = target.get("headers", [])
    rows = target.get("rowSet", [])
    frame = pd.DataFrame(rows, columns=headers)
    frame.columns = frame.columns.str.lower()
    return frame


def get_league_game_log_from_date(
    start: date,
    end: Optional[date] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch league game logs between ``start`` and ``end`` (inclusive)."""

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end if end else date.today())

    if start_ts > end_ts:
        return pd.DataFrame()

    proxies = get_proxies()
    frames: list[pd.DataFrame] = []

    current = start_ts.normalize()
    final = end_ts.normalize()

    while current <= final:
        month_end = (current + pd.offsets.MonthEnd(0)).normalize()
        window_end = min(month_end, final)
        season_label = _season_label(_season_year_for_date(current.date()))

        for season_type in season_types_for_window(current, window_end):
            proxy = random.choice(proxies) if proxies else None

            def _fetch() -> pd.DataFrame:
                return _fetch_leaguegamelog(
                    season=season_label,
                    season_type=season_type,
                    formatted_from=_mmddyyyy(current),
                    formatted_to=_mmddyyyy(window_end),
                    timeout=timeout,
                    proxy=proxy,
                )

            frame = _call_with_retry(
                f"leaguegamelog {current.date()}→{window_end.date()} {season_type}",
                _fetch,
            )
            if frame.empty:
                continue
            if "game_date" in frame.columns:
                frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")
            if "season_type" not in frame.columns:
                frame["season_type"] = season_type
            frames.append(frame)
            time.sleep(REQUEST_SLEEP_SECONDS)

        current = (window_end + pd.Timedelta(days=1)).normalize()

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "game_date" in combined.columns:
        combined = combined[(combined["game_date"].dt.date >= start_ts.date()) & (combined["game_date"].dt.date <= end_ts.date())]
    subset = [col for col in ["game_id", "team_id", "season_type"] if col in combined.columns]
    if subset:
        combined.drop_duplicates(subset=subset, inplace=True)
    combined.reset_index(drop=True, inplace=True)
    return combined


__all__ = ["get_league_game_log_from_date", "SEASON_TYPES"]


BOX_SCORE_URL = "https://stats.nba.com/stats/boxscoretraditionalv2"
PLAY_BY_PLAY_URL = "https://stats.nba.com/stats/playbyplayv2"
DETAIL_SUCCESS_SLEEP_SECONDS = 0.25
PER_GAME_SLEEP_SECONDS = DETAIL_SUCCESS_SLEEP_SECONDS  # backwards compatibility for tests
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def _retryable_http(exc: Exception) -> bool:
    if isinstance(exc, HTTPError) and getattr(exc.response, "status_code", None) is not None:
        return exc.response.status_code in RETRYABLE_STATUS_CODES
    return isinstance(exc, requests_exceptions.RequestException)


@retry(
    wait=wait_exponential(multiplier=1, min=2, max=8),
    stop=stop_after_attempt(3),
    retry=retry_if_exception(_retryable_http),
    reraise=True,
)
def _fetch_json(endpoint: str, params: dict[str, object], *, timeout: int, proxy: str | None) -> dict:
    proxies = {"http": proxy, "https": proxy} if proxy else None
    response = requests.get(
        endpoint,
        params=params,
        headers=NBA_API_HEADERS,
        timeout=timeout,
        proxies=proxies,
    )
    response.raise_for_status()
    return response.json()


def _frame_from_result(payload: dict, name: str) -> pd.DataFrame:
    result_sets = payload.get("resultSets") or payload.get("resultSet")
    if isinstance(result_sets, dict):
        target = result_sets if result_sets.get("name") == name else {}
    elif isinstance(result_sets, list):
        target = next((item for item in result_sets if item.get("name") == name), {})
    else:
        target = {}
    headers = target.get("headers", [])
    rows = target.get("rowSet", [])
    if not headers or not rows:
        return pd.DataFrame(columns=headers)
    frame = pd.DataFrame(rows, columns=headers)
    frame.columns = frame.columns.str.lower()
    return frame


def get_box_score(game_id: str, *, timeout: int = DEFAULT_TIMEOUT) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return team and player box score frames for ``game_id``."""

    proxies = get_proxies()

    def _fetch():
        proxy = random.choice(proxies) if proxies else None
        return _fetch_json(
            BOX_SCORE_URL,
            {
                "GameID": game_id,
                "StartPeriod": 1,
                "EndPeriod": 10,
                "StartRange": 0,
                "EndRange": 0,
                "RangeType": 0,
            },
            timeout=timeout,
            proxy=proxy,
        )

    try:
        payload = _fetch()
    except HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == 400:
            LOGGER.warning("Skip %s: 400 (bad game_id)", game_id)
            return pd.DataFrame(), pd.DataFrame()
        raise
    except requests_exceptions.RequestException as exc:
        raise RuntimeError(f"Failed to fetch box score for {game_id}") from exc
    player = _frame_from_result(payload, "PlayerStats")
    team = _frame_from_result(payload, "TeamStats")
    if not player.empty:
        player["game_id"] = str(game_id)
    if not team.empty:
        team["game_id"] = str(game_id)
    time.sleep(DETAIL_SUCCESS_SLEEP_SECONDS)
    return team, player


def get_play_by_play(game_id: str, *, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """Return play-by-play events for ``game_id``."""

    proxies = get_proxies()

    def _fetch():
        proxy = random.choice(proxies) if proxies else None
        return _fetch_json(
            PLAY_BY_PLAY_URL,
            {
                "GameID": game_id,
                "StartPeriod": 1,
                "EndPeriod": 10,
            },
            timeout=timeout,
            proxy=proxy,
        )

    try:
        payload = _fetch()
    except HTTPError as exc:
        status = getattr(exc.response, "status_code", None)
        if status == 400:
            LOGGER.warning("Skip %s PBP: 400 (bad game_id)", game_id)
            return pd.DataFrame()
        raise
    except requests_exceptions.RequestException as exc:
        raise RuntimeError(f"Failed to fetch play-by-play for {game_id}") from exc
    frame = _frame_from_result(payload, "PlayByPlay")
    if not frame.empty:
        frame["game_id"] = str(game_id)
    time.sleep(DETAIL_SUCCESS_SLEEP_SECONDS)
    return frame
