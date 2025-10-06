"""NBA Stats API helpers for incremental game log updates."""
from __future__ import annotations

import logging
import random
import time
from datetime import date
from typing import Optional

import pandas as pd
import requests
from requests import exceptions as requests_exceptions

from .utils import get_proxies


LOGGER = logging.getLogger(__name__)

SEASON_TYPES = [
    "Regular Season",
    "Playoffs",
    "Pre Season",
    "In Season Tournament",
]

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
    season_type: str,
    formatted_from: Optional[str],
    formatted_to: Optional[str],
    timeout: int,
    proxy: Optional[str],
) -> pd.DataFrame:
    params = {
        "Counter": 0,
        "Direction": "ASC",
        "LeagueID": "00",
        "PlayerOrTeam": "T",
        "Season": season,
        "SeasonType": season_type,
        "Sorter": "DATE",
        "DateFrom": formatted_from or "",
        "DateTo": formatted_to or "",
    }
    proxies = {"http": proxy, "https": proxy} if proxy else None

    response = requests.get(
        NBA_API_URL,
        params=params,
        headers=NBA_API_HEADERS,
        timeout=timeout,
        proxies=proxies,
    )
    response.raise_for_status()
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
    return pd.DataFrame(rows, columns=headers)


def get_league_game_log_from_date(
    start: date,
    end: Optional[date] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch league game logs between ``start`` and ``end`` (inclusive)."""

    today = date.today()
    latest_season_year = _latest_started_season_year(today)
    effective_end = end or today
    effective_end_year = min(_season_year_for_date(effective_end), latest_season_year)
    start_year = _season_year_for_date(start)

    if start_year > latest_season_year:
        LOGGER.info("Start date %s is in a future season; returning empty frame", start)
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    proxies = get_proxies()

    for season_year in range(start_year, effective_end_year + 1):
        season = _season_label(season_year)
        season_start = date(season_year, 7, 1)
        season_end = date(season_year + 1, 6, 30)
        season_from = start if season_year == start_year else season_start
        season_to = effective_end if season_year == effective_end_year else season_end

        for season_type in SEASON_TYPES:
            formatted_from = _format_for_api(season_from if season_from >= season_start else season_start)
            formatted_to = _format_for_api(season_to if season_to <= season_end else season_end)

            proxy = random.choice(proxies) if proxies else None

            def _fetch() -> pd.DataFrame:
                try:
                    return _fetch_leaguegamelog(
                        season=season,
                        season_type=season_type,
                        formatted_from=formatted_from,
                        formatted_to=formatted_to,
                        timeout=timeout,
                        proxy=proxy,
                    )
                except requests_exceptions.HTTPError as exc:
                    status_code = getattr(exc.response, "status_code", None)
                    if status_code in {400, 404}:
                        LOGGER.info(
                            "No data for season=%s type=%s (HTTP %s); skipping",
                            season,
                            season_type,
                            status_code,
                        )
                        return pd.DataFrame()
                    raise

            frame = _call_with_retry(f"leaguegamelog {season} {season_type}", _fetch)
            if not frame.empty:
                normalised = _normalise_frame(frame, season_type)
                frames.append(normalised)
            time.sleep(REQUEST_SLEEP_SECONDS)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "game_date" in combined.columns:
        upper_bound = end or effective_end
        mask = combined["game_date"].isna() | (
            (combined["game_date"].dt.date >= start) &
            (combined["game_date"].dt.date <= upper_bound)
        )
        combined = combined[mask]
        combined.reset_index(drop=True, inplace=True)
    return combined


__all__ = ["get_league_game_log_from_date", "SEASON_TYPES"]


BOX_SCORE_URL = "https://stats.nba.com/stats/boxscoretraditionalv2"
PLAY_BY_PLAY_URL = "https://stats.nba.com/stats/playbyplayv2"
PER_GAME_SLEEP_SECONDS = 0.35


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
        payload = _fetch_json(
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
        return payload

    payload = _call_with_retry(f"boxscoretraditionalv2 {game_id}", _fetch)
    player = _frame_from_result(payload, "PlayerStats")
    team = _frame_from_result(payload, "TeamStats")
    if not player.empty:
        player["game_id"] = str(game_id)
    if not team.empty:
        team["game_id"] = str(game_id)
    return team, player


def get_play_by_play(game_id: str, *, timeout: int = DEFAULT_TIMEOUT) -> pd.DataFrame:
    """Return play-by-play events for ``game_id``."""

    proxies = get_proxies()

    def _fetch():
        proxy = random.choice(proxies) if proxies else None
        payload = _fetch_json(
            PLAY_BY_PLAY_URL,
            {
                "GameID": game_id,
                "StartPeriod": 1,
                "EndPeriod": 10,
            },
            timeout=timeout,
            proxy=proxy,
        )
        return payload

    payload = _call_with_retry(f"playbyplayv2 {game_id}", _fetch)
    frame = _frame_from_result(payload, "PlayByPlay")
    if not frame.empty:
        frame["game_id"] = str(game_id)
    return frame
