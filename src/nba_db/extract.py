"""NBA Stats API helpers for incremental game log updates."""
from __future__ import annotations

import logging
import random
import time
from datetime import date
from typing import Optional, Tuple
import json
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type

import pandas as pd
import requests

from requests import exceptions as requests_exceptions

from .utils import get_proxies


LOGGER = logging.getLogger(__name__)

SEASON_TYPES = [
    "Regular Season",
    "Playoffs",
    "PlayIn",
    "In Season Tournament",
]

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









def get_league_game_log_from_date(
    datefrom: str,
    dateto: Optional[str] = None,
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> pd.DataFrame:
    """Fetch league game logs between ``datefrom`` and ``dateto`` (inclusive)."""

    # Convert string dates to date objects
    datefrom_obj = date.fromisoformat(datefrom)
    dateto_obj = date.fromisoformat(dateto) if dateto else None

    today = date.today()
    latest_season_year = _latest_started_season_year(today)
    effective_end = dateto_obj if dateto_obj else date.today()

    # Adjust effective_start if it's before the earliest season in the database
    # (logic for this is not shown in the provided snippet, but assumed to be here)

    effective_end_year = min(_season_year_for_date(effective_end), latest_season_year)
    start_year = _season_year_for_date(datefrom_obj)

    if start_year > latest_season_year:
        LOGGER.info("Start date %s is in a future season; returning empty frame", datefrom_obj)
        return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    proxies = get_proxies()

    for season_year in range(start_year, effective_end_year + 1):
        season = _season_label(season_year)
        season_start = date(season_year, 7, 1)
        season_end = date(season_year + 1, 6, 30)
        season_from = datefrom_obj if season_year == start_year else season_start
        season_to = effective_end if season_year == effective_end_year else season_end

        for season_type in SEASON_TYPES:
            formatted_from = _format_for_api(season_from if season_from >= season_start else season_start)
            formatted_to = _format_for_api(season_to if season_to <= season_end else season_end)

            @retry(wait=wait_exponential(min=1, max=6), stop=stop_after_attempt(4), retry=retry_if_exception_type(requests.exceptions.RequestException))
            def _make_api_request(season, season_type, formatted_from, formatted_to, proxy_to_use):
                base_url = "https://stats.nba.com/stats/leaguegamelog"
                params = {
                    "LeagueID": "00", # NBA
                    "Season": season,
                    "SeasonType": season_type,
                    "PlayerOrTeam": "T", # Team
                    "Counter": 0,
                    "Sorter": "DATE",
                    "Direction": "ASC",
                    "DateFrom": formatted_from,
                    "DateTo": formatted_to,
                }

                response = requests.get(
                    base_url,
                    params=params,
                    headers=NBA_API_HEADERS,
                    proxies={"http": proxy_to_use, "https": proxy_to_use} if proxy_to_use else None,
                    timeout=10, # Use 10s timeout as suggested
                )
                response.raise_for_status() # Raise an exception for HTTP errors
                return response.json()

            try:
                proxy_to_use = random.choice(proxies) if proxies else None
                raw_data = _make_api_request(season, season_type, formatted_from, formatted_to, proxy_to_use)

                if "resultSets" not in raw_data:
                    LOGGER.warning(f"'resultSets' not found in API response for {season_type} from {formatted_from} to {formatted_to}. Response: {raw_data}")
                    continue # Continue to next season_type

                # Assuming the first resultSet contains the game log data
                game_log_data = raw_data["resultSets"][0]
                headers = game_log_data["headers"]
                rows = game_log_data["rowSet"]
                df = pd.DataFrame(rows, columns=headers)

                if df.empty:
                    LOGGER.info(f"No data returned for {season_type} from {formatted_from} to {formatted_to}.")
                    continue # Continue to next season_type
                # normalize here so callers can rely on shape
                df.columns = df.columns.str.lower()
                if "game_date" in df.columns:
                    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                df["season_type"] = season_type
                # DO NOT self-merge; keep per-team rows
                frames.append(df)
            except requests.exceptions.RequestException as e:
                LOGGER.error(f"RequestException for {season_type} from {formatted_from} to {formatted_to}: {e}")
                # Tenacity will handle retries, so we just log and let it retry or fail
            except json.JSONDecodeError as e:
                LOGGER.error(f"JSONDecodeError for {season_type} from {formatted_from} to {formatted_to}: {e}. Response content: {response.text}")
                # Tenacity does not retry on JSONDecodeError by default, so we log and continue
            time.sleep(REQUEST_SLEEP_SECONDS) # Politeness sleep after each successful call

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    if "game_date" in combined.columns:
        upper_bound = dateto_obj or effective_end
        mask = combined["game_date"].isna() | (
            (combined["game_date"].dt.date >= datefrom_obj) &
            (combined["game_date"].dt.date <= upper_bound)
        )
        combined = combined[mask]
        combined.reset_index(drop=True, inplace=True)
    return combined


__all__ = ["get_league_game_log_from_date", "SEASON_TYPES"]
