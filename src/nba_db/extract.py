"""NBA Stats API helpers for incremental game log updates."""
from __future__ import annotations

import logging
import random
import time
from datetime import date
from typing import Optional, Tuple
import json
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception

import pandas as pd
import requests
from requests import exceptions as requests_exceptions, HTTPError

from .utils import get_proxies


LOGGER = logging.getLogger(__name__)

DAILY_SEASON_TYPES = ["Regular Season", "Playoffs", "PlayIn", "In-Season Tournament"]

SEASON_TYPE_MAP = {
    "regular season": "Regular Season",
    "playoffs": "Playoffs",
    "playin": "PlayIn",
    "play-in": "PlayIn",
    "preseason": "Pre Season",
    "pre season": "Pre Season",
    "in season tournament": "In-Season Tournament",  # MUST be hyphenated
    "in-season tournament": "In-Season Tournament",
}

def _canon_season_type(s):
    return SEASON_TYPE_MAP.get((s or "").strip().lower()) or s

def season_types_for_window(start: date, end: date) -> list[str]:
    # Cheap guards to reduce empty calls
    types = set()
    # Regular season runs Oct–Apr
    if 10 <= start.month <= 12 or 1 <= end.month <= 4:
        types.add("Regular Season")
    # Preseason late Sep–Oct
    if start.month in {9,10} or end.month in {9,10}:
        types.add("Pre Season")
    # In-Season Tournament in Nov–Dec
    if 11 <= start.month <= 12 or 11 <= end.month <= 12:
        types.add("In-Season Tournament")
    # Play-In and Playoffs mid-Apr–Jun
    if end.month >= 4:
        types.add("PlayIn")
        types.add("Playoffs")
    return sorted(types)

def _is_retryable_http(e: Exception) -> bool:
    if isinstance(e, HTTPError) and e.response is not None:
        return e.response.status_code in {429, 500, 502, 503, 504}
    return isinstance(e, (TimeoutError, ConnectionError))

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

@retry(
    wait=wait_exponential(multiplier=1, min=2, max=8),
    stop=stop_after_attempt(4),
    retry=retry_if_exception(_is_retryable_http),
    reraise=True,
)
def _call_leaguegamelog(season, season_type_raw, formatted_from, formatted_to, proxy_to_use):
    season_type = _canon_season_type(season_type_raw) # Canonicalize season type
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









def get_league_game_log_from_date(date_from: str, date_to: str, proxies=None) -> pd.DataFrame:
    start = pd.to_datetime(date_from)
    end   = pd.to_datetime(date_to)

    dfs = []
    for st in season_types_for_window(start, end):
        st_canonical = _canon_season_type(st) # Use canonical season type
        # one request per valid season type
        try:
            proxy_to_use = random.choice(proxies) if proxies else None
            raw_data = _call_leaguegamelog(start.year, st_canonical, date_from, date_to, proxy_to_use) # Pass canonical season type
            if "resultSets" not in raw_data:
                LOGGER.warning(f"'resultSets' not found in API response for {st_canonical} from {date_from} to {date_to}. Response: {raw_data}")
                continue # Continue to next season type

            # Assuming the first resultSet contains the game log data
            game_log_data = raw_data["resultSets"][0]
            headers = game_log_data["headers"]
            rows = game_log_data["rowSet"]
            df = pd.DataFrame(rows, columns=headers)

            if df is not None and not df.empty:
                dfs.append(df)
        except HTTPError as e:
            code = e.response.status_code if e.response is not None else None
            if code == 400:
                LOGGER.warning(f"Skipping season_type={st_canonical} for {start.year}: server says 400 (likely invalid SeasonType/date combo or label).")
                continue # Continue to next season type
            raise # Re-raise other HTTP errors

    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True)
    # normalize once here
    out.columns = out.columns.str.lower()
    if "game_date" in out.columns:
        out["game_date"] = pd.to_datetime(out["game_date"], errors="coerce")
    if "season_type" not in out.columns:
        out["season_type"] = st_canonical  # best-effort; usually present from API
    return out
