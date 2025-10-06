"""Data ingestion helpers emulating the ``wyattowalsh/nbadb`` project."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Callable, Iterable, Optional, Sequence

import pandas as pd
import time
from requests import exceptions as requests_exceptions
try:  # pragma: no cover - optional dependency in the lightweight environment
    import yaml
except ModuleNotFoundError:  # pragma: no cover
    yaml = None  # type: ignore[assignment]
from nba_api.stats.static import players as static_players
from nba_api.stats.static import teams as static_teams
from nba_api.stats.endpoints import (
    boxscoresummaryv2,
    commonplayerinfo,
    leaguegamelog,
    teaminfocommon,
)

CONFIG_FILENAME = "config.yaml"
SEASON_TYPES: tuple[str, ...] = (
    "Regular Season",
    "Playoffs",
    "Pre Season",
    "PlayIn",
)


NBA_API_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "x-nba-stats-origin": "stats",
    "Referer": "https://stats.nba.com/",
}

DEFAULT_TIMEOUT = 10


@dataclass
class DailyUpdateResult:
    """Summary describing a ``daily`` update."""

    output_path: Path
    rows_written: int
    appended: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "output_path": str(self.output_path),
            "rows_written": self.rows_written,
            "appended": self.appended,
        }


@dataclass
class InitResult:
    """Outcome of the initial bootstrap."""

    player_path: Path
    team_path: Path
    game_path: Path
    game_summary_path: Path
    row_counts: dict[str, int]


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _parse_yaml_fallback(text: str) -> dict[str, object]:
    config: dict[str, object] = {}
    stack: list[tuple[int, dict[str, object]]] = [(-1, config)]
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip())
        line = raw_line.strip()
        if ":" not in line:
            raise ValueError(f"Invalid config line: {raw_line!r}")
        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        current = stack[-1][1]
        if not value:
            nested: dict[str, object] = {}
            current[key] = nested
            stack.append((indent, nested))
            continue
        if value[0] in {'"', "'"} and value[-1] == value[0]:
            value = value[1:-1]
        current[key] = value
    return config


def _load_config(config_path: Optional[Path] = None) -> dict[str, object]:
    path = config_path or _project_root() / CONFIG_FILENAME
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")
    with path.open("r", encoding="utf-8") as handle:
        text = handle.read()
    if yaml is not None:
        config = yaml.safe_load(text) or {}
    else:
        config = _parse_yaml_fallback(text)
    return config


def _resolve_raw_dir(config: dict[str, object], override: Optional[Path | str] = None) -> Path:
    if override is not None:
        raw_dir = Path(override)
        return raw_dir if raw_dir.is_absolute() else _project_root() / raw_dir

    raw_section = config.get("raw") if isinstance(config, dict) else None
    if not isinstance(raw_section, dict):
        raise KeyError("Configuration is missing 'raw' section")
    raw_dir_value = raw_section.get("raw_dir")
    if raw_dir_value is None:
        raise KeyError("Configuration is missing 'raw.raw_dir'")
    raw_dir = Path(raw_dir_value)
    return raw_dir if raw_dir.is_absolute() else _project_root() / raw_dir


def _season_start(year: int) -> date:
    return date(year, 7, 1)


def _season_end(year: int) -> date:
    return date(year + 1, 6, 30)


def _season_label(year: int) -> str:
    return f"{year}-{str(year + 1)[-2:]}"


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
            lambda st=season_type: leaguegamelog.LeagueGameLog(
                league_id="00",
                season=season,
                season_type_all_star=st,
                date_from_nullable=from_str,
                date_to_nullable=to_str,
                headers=NBA_API_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            ).get_data_frames()[0],
        )
        if not frame.empty:
            frame.insert(0, "SEASON_TYPE", season_type)
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


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
    return pd.concat(frames, ignore_index=True)


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
    return pd.concat(frames, ignore_index=True)


def _infer_game_date(series: pd.Series) -> pd.Series:
    for column in ("GAME_DATE", "GAME_DATE_EST", "GAME_DATE_TIME_EST"):
        if column in series:
            return pd.to_datetime(series[column], errors="coerce")
    raise KeyError("Game log does not contain a date column")


def _next_start_date(game_csv_path: Path) -> date:
    if not game_csv_path.exists():
        raise FileNotFoundError(
            "Cannot infer start date because game.csv does not exist."
        )
    frame = pd.read_csv(game_csv_path)
    if frame.empty:
        raise ValueError("Existing game.csv is empty")
    dates = _infer_game_date(frame)
    last = dates.dropna().max()
    if pd.isna(last):
        raise ValueError("Unable to infer last game date from game.csv")
    return (last.date() + timedelta(days=1))


def _write_dataframe(path: Path, frame: pd.DataFrame, *, append: bool) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    if append and path.exists():
        frame.to_csv(path, mode="a", header=False, index=False)
    else:
        frame.to_csv(path, index=False)
    return len(frame)


def _call_with_retry(description: str, func: Callable[[], pd.DataFrame]) -> pd.DataFrame:
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            return func()
        except requests_exceptions.RequestException as exc:  # pragma: no cover - network
            last_error = exc
            time.sleep(2 ** attempt)
    if last_error is not None:  # pragma: no cover - network
        raise RuntimeError(f"Failed to fetch {description}") from last_error
    raise RuntimeError(f"Failed to fetch {description}")


def _fetch_all_players() -> pd.DataFrame:
    players = static_players.get_players()
    frames: list[pd.DataFrame] = []
    for meta in players:
        frame = _call_with_retry(
            f"player {meta['id']}",
            lambda meta_id=meta["id"]: commonplayerinfo.CommonPlayerInfo(
                player_id=meta_id,
                headers=NBA_API_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            ).get_data_frames()[0],
        )
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_all_teams() -> pd.DataFrame:
    teams = static_teams.get_teams()
    frames: list[pd.DataFrame] = []
    for meta in teams:
        frame = _call_with_retry(
            f"team {meta['id']}",
            lambda meta_id=meta["id"]: teaminfocommon.TeamInfoCommon(
                team_id=meta_id,
                headers=NBA_API_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            ).get_data_frames()[0],
        )
        if not frame.empty:
            frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _fetch_game_summaries(game_ids: Sequence[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for game_id in game_ids:
        frame = _call_with_retry(
            f"game summary {game_id}",
            lambda gid=game_id: boxscoresummaryv2.BoxScoreSummaryV2(
                game_id=gid,
                headers=NBA_API_HEADERS,
                timeout=DEFAULT_TIMEOUT,
            ).get_data_frames()[0],
        )
        if not frame.empty:
            frames.append(frame)
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
    config = _load_config()
    raw_dir = _resolve_raw_dir(config, override=output_dir)
    raw_dir.mkdir(parents=True, exist_ok=True)
    game_csv_path = raw_dir / "game.csv"

    if fetch_all_history:
        frame = get_league_game_log_all()
        rows = _write_dataframe(game_csv_path, frame, append=False)
        return DailyUpdateResult(game_csv_path, rows, appended=False)

    start = date.fromisoformat(start_date) if start_date else _next_start_date(game_csv_path)
    stop = date.fromisoformat(end_date) if end_date else None
    frame = get_league_game_log_from_date(start, stop)
    if frame.empty:
        return DailyUpdateResult(game_csv_path, 0, appended=game_csv_path.exists())
    rows = _write_dataframe(game_csv_path, frame, append=game_csv_path.exists())
    return DailyUpdateResult(game_csv_path, rows, appended=game_csv_path.exists())


def init(output_dir: Optional[Path | str] = None) -> InitResult:
    config = _load_config()
    raw_dir = _resolve_raw_dir(config, override=output_dir)
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
        "players": _write_dataframe(player_path, players_frame, append=False) if not players_frame.empty else 0,
        "teams": _write_dataframe(team_path, teams_frame, append=False) if not teams_frame.empty else 0,
        "games": _write_dataframe(game_path, games_frame, append=False) if not games_frame.empty else 0,
        "game_summaries": _write_dataframe(game_summary_path, summaries_frame, append=False) if not summaries_frame.empty else 0,
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
    "daily",
    "get_league_game_log_all",
    "get_league_game_log_from_date",
    "init",
]
