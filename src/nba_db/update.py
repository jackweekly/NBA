"""Incremental game log updater built around the Kaggle bootstrap seed."""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence, Tuple

import duckdb
import pandas as pd

from . import extract
from .paths import DUCKDB_PATH, GAME_CSV, RAW_DIR


LOGGER = logging.getLogger(__name__)

GAME_LOG_PRIMARY_KEY = ("game_id", "team_id", "season_type")
DEFAULT_SEASON_TYPE = "Regular Season"
HISTORICAL_START_DATE = date(1946, 11, 1)


@dataclass(slots=True)
class DailyUpdateResult:
    """Summary describing a ``daily`` update."""

    output_path: Path
    rows_written: int
    appended: bool
    final_row_count: int

    def to_dict(self) -> dict[str, object]:  # pragma: no cover - helper for scripts/CLI
        return {
            "output_path": str(self.output_path),
            "rows_written": self.rows_written,
            "appended": self.appended,
            "final_row_count": self.final_row_count,
        }


def _ensure_raw_dir() -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)


def _ensure_output_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _canonicalise(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        return frame

    canonical = frame.copy()
    canonical.columns = canonical.columns.str.lower()
    if "game_date" in canonical.columns:
        canonical["game_date"] = pd.to_datetime(canonical["game_date"], errors="coerce")
    if "season_type" not in canonical.columns:
        canonical["season_type"] = DEFAULT_SEASON_TYPE
    else:
        canonical["season_type"] = (
            canonical["season_type"].fillna(DEFAULT_SEASON_TYPE).replace("", DEFAULT_SEASON_TYPE)
        )
    for column in ("season_id", "game_id", "team_id"):
        if column not in canonical.columns:
            continue
        series = canonical[column].astype(str).str.strip()
        if column in {"game_id", "team_id"}:
            series = series.str.lstrip("0").replace({"": "0"})
        canonical[column] = series
    return canonical


def _duckdb_table_exists(table: str) -> bool:
    if not DUCKDB_PATH.exists():
        return False
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        tables = con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name IS NULL AND schema_name = 'main'"
        ).fetchall()
        return any(name == table for (name,) in tables)
    finally:
        con.close()


def _read_duckdb_game_log() -> pd.DataFrame:
    if not _duckdb_table_exists("bronze_game_log_team"):
        raise FileNotFoundError("bronze_game_log_team not found in DuckDB warehouse")
    con = duckdb.connect(str(DUCKDB_PATH))
    try:
        frame = con.execute("SELECT * FROM bronze_game_log_team").fetch_df()
    finally:
        con.close()
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        return frame
    return _canonicalise(frame)


def _read_existing(path: Path) -> pd.DataFrame:
    try:
        return _read_duckdb_game_log()
    except FileNotFoundError:
        pass

    if not path.exists():
        raise FileNotFoundError(
            f"{path} not found and DuckDB warehouse is missing bronze_game_log_team"
        )

    frame = pd.read_csv(path)
    if frame.empty:
        frame.columns = [col.lower() for col in frame.columns]
        return frame
    return _canonicalise(frame)


def _latest_game_date(frame: pd.DataFrame) -> Optional[date]:
    if "game_date" not in frame.columns:
        return None
    valid = frame["game_date"].dropna()
    if valid.empty:
        return None
    return valid.max().date()


def _deduplicate(frame: pd.DataFrame) -> pd.DataFrame:
    subset = [column for column in GAME_LOG_PRIMARY_KEY if column in frame.columns]
    if not subset:
        return frame.drop_duplicates(ignore_index=True)
    return frame.drop_duplicates(subset=subset, keep="first", ignore_index=True)


def _atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    tmp_path = path.with_suffix(".tmp")
    frame.to_csv(tmp_path, index=False)
    os.replace(tmp_path, path)


def _upsert_duckdb(frame: pd.DataFrame, *, replace: bool = False) -> None:
    if frame.empty:
        return

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("PRAGMA threads=4;")
    try:
        con.register("src", frame)
        con.execute("CREATE TABLE IF NOT EXISTS bronze_game_log_team AS SELECT * FROM src WHERE 0=1")
        if replace:
            LOGGER.info("Replacing bronze_game_log_team with %s rows", len(frame))
            con.execute("DELETE FROM bronze_game_log_team")
            con.execute("INSERT INTO bronze_game_log_team SELECT * FROM src")
        else:
            LOGGER.info("Merging %s rows into bronze_game_log_team", len(frame))
            con.execute(
                """
                MERGE INTO bronze_game_log_team AS tgt
                USING src AS src
                ON tgt.game_id = src.game_id
                 AND tgt.team_id = src.team_id
                 AND COALESCE(LOWER(tgt.season_type),'') = COALESCE(LOWER(src.season_type),'')
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
    finally:
        try:
            con.unregister("src")
        except duckdb.Error:  # pragma: no cover - safe cleanup
            pass
        con.close()


def _replace_duckdb_table(table: str, frame: pd.DataFrame, *, delete_key: str = "game_id") -> None:
    if frame.empty:
        return

    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("PRAGMA threads=4;")
    try:
        con.register("src", frame)
        con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM src WHERE 0=1")
        if delete_key in frame.columns:
            keys = frame[delete_key].dropna().unique().tolist()
            if keys:
                placeholders = ",".join("?" for _ in keys)
                con.execute(f"DELETE FROM {table} WHERE {delete_key} IN ({placeholders})", keys)
        con.execute(f"INSERT INTO {table} SELECT * FROM src")
    finally:
        try:
            con.unregister("src")
        except duckdb.Error:
            pass
        con.close()


def _normalise_id_columns(frame: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
    for column in columns:
        if column in frame.columns:
            frame[column] = frame[column].astype(str).str.strip()
    return frame


def _fetch_boxscores(game_ids: Sequence[str]) -> Tuple[pd.DataFrame, pd.DataFrame]:
    team_frames: list[pd.DataFrame] = []
    player_frames: list[pd.DataFrame] = []
    for index, game_id in enumerate(game_ids, 1):
        try:
            team_frame, player_frame = extract.get_box_score(game_id)
        except RuntimeError as exc:  # noqa: BLE001 - network errors already logged
            LOGGER.warning("Box score fetch failed for %s: %s", game_id, exc)
            continue
        if not team_frame.empty:
            team_frame = _normalise_id_columns(team_frame, ["game_id", "team_id"])
            team_frames.append(team_frame)
        if not player_frame.empty:
            player_frame = _normalise_id_columns(player_frame, ["game_id", "team_id", "player_id"])
            player_frames.append(player_frame)
        if index % 100 == 0:
            LOGGER.info("Fetched box scores for %s/%s games", index, len(game_ids))
        time.sleep(extract.PER_GAME_SLEEP_SECONDS)
    team_df = pd.concat(team_frames, ignore_index=True) if team_frames else pd.DataFrame()
    player_df = pd.concat(player_frames, ignore_index=True) if player_frames else pd.DataFrame()
    return team_df, player_df


def _fetch_play_by_play_games(game_ids: Sequence[str]) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for index, game_id in enumerate(game_ids, 1):
        try:
            frame = extract.get_play_by_play(game_id)
        except RuntimeError as exc:  # noqa: BLE001
            LOGGER.warning("Play-by-play fetch failed for %s: %s", game_id, exc)
            continue
        if frame.empty:
            continue
        frame = _normalise_id_columns(frame, ["game_id", "team_id"])
        if "eventnum" in frame.columns:
            frame["eventnum"] = pd.to_numeric(frame["eventnum"], errors="coerce")
        frames.append(frame)
        if index % 100 == 0:
            LOGGER.info("Fetched play-by-play for %s/%s games", index, len(game_ids))
        time.sleep(extract.PER_GAME_SLEEP_SECONDS)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _update_additional_tables(game_ids: Sequence[str]) -> None:
    if not game_ids:
        return

    LOGGER.info("Fetching detailed stats for %s games", len(game_ids))
    team_box, player_box = _fetch_boxscores(game_ids)
    if not team_box.empty:
        _replace_duckdb_table("bronze_box_score_team", team_box)
        LOGGER.info("Upserted team box scores (%s rows)", len(team_box))
    if not player_box.empty:
        _replace_duckdb_table("bronze_box_score_player", player_box)
        LOGGER.info("Upserted player box scores (%s rows)", len(player_box))
    pbp = _fetch_play_by_play_games(game_ids)
    if not pbp.empty:
        _replace_duckdb_table("bronze_play_by_play", pbp)
        LOGGER.info("Upserted play-by-play events (%s rows)", len(pbp))


def _log_fetch_window(new_rows: pd.DataFrame) -> None:
    if "game_date" not in new_rows.columns:
        LOGGER.info("Fetched %s new rows (game_date column missing)", len(new_rows))
        return

    valid_dates = new_rows["game_date"].dropna()
    if valid_dates.empty:
        LOGGER.info("Fetched %s new rows with unknown game_date values", len(new_rows))
        return

    window = f"{valid_dates.min().date()} → {valid_dates.max().date()}"
    LOGGER.info("Fetched %s new rows covering %s", len(new_rows), window)


def daily(
    *,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fetch_all_history: bool = False,
    output_path: Optional[Path] = None,
) -> DailyUpdateResult:
    """Fetch and append the latest NBA games into ``data/raw/game.csv``."""

    target_path = Path(output_path) if output_path is not None else GAME_CSV

    _ensure_raw_dir()
    _ensure_output_dir(target_path)

    fetch_until = date.fromisoformat(end_date) if end_date else None

    if fetch_all_history:
        fetch_from = date.fromisoformat(start_date) if start_date else HISTORICAL_START_DATE
        LOGGER.info(
            "Fetching full historical league game log from %s%s",
            fetch_from.isoformat(),
            f" through {fetch_until.isoformat()}" if fetch_until else "",
        )
        new_rows = extract.get_league_game_log_from_date(fetch_from, fetch_until)
        new_rows = _canonicalise(new_rows)
        if new_rows.empty:
            LOGGER.info("NBA API returned no rows for the requested historical range")
            _atomic_write_csv(new_rows, target_path)
            return DailyUpdateResult(target_path, 0, appended=False, final_row_count=0)

        new_rows = _deduplicate(new_rows)
        game_ids = sorted(new_rows.get("game_id", pd.Series(dtype=str)).dropna().unique().tolist())
        _log_fetch_window(new_rows)
        _atomic_write_csv(new_rows, target_path)
        _upsert_duckdb(new_rows, replace=True)
        _update_additional_tables(game_ids)
        final_count = len(new_rows)
        LOGGER.info("Wrote complete historical game log with %s rows to %s", final_count, target_path)
        return DailyUpdateResult(target_path, final_count, appended=False, final_row_count=final_count)

    existing = _read_existing(target_path)

    today_local = date.today()

    if start_date:
        fetch_from = date.fromisoformat(start_date)
    else:
        latest = _latest_game_date(existing)
        if latest is None:
            raise RuntimeError(
                "Existing game.csv does not contain any valid game_date values; cannot resume update"
            )
        fetch_from = latest + timedelta(days=1)

    if fetch_until and fetch_from > fetch_until:
        LOGGER.info(
            "Start date %s is after requested end date %s; nothing to fetch",
            fetch_from.isoformat(),
            fetch_until.isoformat(),
        )
        return DailyUpdateResult(target_path, 0, appended=True, final_row_count=len(existing))

    if fetch_until is None and fetch_from >= today_local:
        LOGGER.info("No new games to fetch; latest recorded date is %s", (fetch_from - timedelta(days=1)))
        return DailyUpdateResult(target_path, 0, appended=True, final_row_count=len(existing))

    new_rows = extract.get_league_game_log_from_date(fetch_from, fetch_until)
    new_rows = _canonicalise(new_rows)

    if new_rows.empty:
        LOGGER.info("NBA API returned no games for %s onward", fetch_from.isoformat())
        return DailyUpdateResult(target_path, 0, appended=True, final_row_count=len(existing))

    game_ids = sorted(new_rows.get("game_id", pd.Series(dtype=str)).dropna().unique().tolist())

    _log_fetch_window(new_rows)

    combined = pd.concat([existing, new_rows], ignore_index=True)
    combined = _deduplicate(combined)

    _atomic_write_csv(combined, target_path)
    _upsert_duckdb(new_rows)
    _update_additional_tables(game_ids)
    final_count = len(combined)
    appended_rows = max(final_count - len(existing), 0)

    LOGGER.info("Final game.csv row count: %s", final_count)

    return DailyUpdateResult(target_path, appended_rows, appended=True, final_row_count=final_count)


__all__ = ["DailyUpdateResult", "daily"]
