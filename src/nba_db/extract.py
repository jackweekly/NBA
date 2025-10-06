"""NBA Stats API helpers for incremental game log updates."""
from __future__ import annotations

import logging
import random
import time
from datetime import date
from typing import Optional

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog
from requests import exceptions as requests_exceptions

from .utils import get_proxies


LOGGER = logging.getLogger(__name__)

SEASON_TYPES = [
    "Regular Season",
    "Playoffs",
    "Pre Season",
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
    effective_end = dateto_obj or today
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

            while True:
                try:
                    proxy_to_use = random.choice(proxies) if proxies else None
                    endpoint = leaguegamelog.LeagueGameLog(
                        season=season,
                        season_type_all_star=season_type,
                        date_from_nullable=formatted_from,
                        date_to_nullable=formatted_to,
                        timeout=10, # <-- was DEFAULT_TIMEOUT, make it less flaky
                        headers=NBA_API_HEADERS,
                        proxy=proxy_to_use,
                    )
                    dfs = endpoint.get_data_frames()
                    if not dfs:
                        LOGGER.info(f"No data returned for {season_type} from {formatted_from} to {formatted_to}.")
                        break
                    df = dfs[0]
                    # normalize here so callers can rely on shape
                    df.columns = df.columns.str.lower()
                    if "game_date" in df.columns:
                        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
                    df["season_type"] = season_type
                    # DO NOT self-merge; keep per-team rows
                    frames.append(df)
                    break
                except requests_exceptions.RequestException as e:
                    LOGGER.error(f"RequestException for {season_type} from {formatted_from} to {formatted_to}: {e}")
                    time.sleep(5) # Sleep for 5 seconds before retrying
                    continue
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
