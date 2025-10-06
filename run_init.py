#!/usr/bin/env python3
"""Bootstrap the wyattowalsh/basketball Kaggle dataset into local storage."""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd

from nba_db.paths import (
    GAME_CSV,
    LOG_DIR,
    RAW_BOOTSTRAP_DIR,
    RAW_BOOTSTRAP_LEAGUELOG_DIR,
    RAW_DIR,
    REPORTS_DIR,
    ROOT,
    WATERMARK_PATH,
    WYATT_DATASET_DIR,
)

DEFAULT_DATASET_ID = "wyattowalsh/basketball"
MIN_FREE_BYTES = 15 * 1024**3

_NUMERIC_ENV_DEFAULTS: Dict[str, str] = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OPENBLAS_CORETYPE": "HASWELL",
}


@dataclass(frozen=True)
class TableSpec:
    """Description of a bootstrap table we expect to extract."""

    name: str
    csv_candidates: tuple[str, ...]
    sqlite_candidates: tuple[str, ...]
    optional: bool = False


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="game",
        csv_candidates=("game.csv", "games.csv"),
        sqlite_candidates=("game",),
    ),
    TableSpec(
        name="team",
        csv_candidates=("team.csv", "teams.csv"),
        sqlite_candidates=("team",),
    ),
    TableSpec(
        name="player",
        csv_candidates=("player.csv", "players.csv"),
        sqlite_candidates=("player",),
    ),
    TableSpec(
        name="box_score",
        csv_candidates=("box_score.csv", "boxscore.csv", "line_score.csv"),
        sqlite_candidates=("box_score", "boxscore", "line_score"),
        optional=False,
    ),
    TableSpec(
        name="play_by_play",
        csv_candidates=("play_by_play.csv", "playbyplay.csv"),
        sqlite_candidates=("play_by_play", "playbyplay"),
    ),
)


def _configure_numeric_environment() -> None:
    for key, value in _NUMERIC_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and prepare Kaggle bootstrap data")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download and overwrite existing bootstrap artefacts",
    )
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help="Kaggle dataset identifier to download (default: wyattowalsh/basketball)",
    )
    return parser.parse_args(argv)


def _ensure_dirs() -> None:
    for directory in (
        RAW_DIR,
        RAW_BOOTSTRAP_DIR,
        RAW_BOOTSTRAP_LEAGUELOG_DIR,
        WYATT_DATASET_DIR,
        LOG_DIR,
        REPORTS_DIR,
        GAME_CSV.parent,
    ):
        directory.mkdir(parents=True, exist_ok=True)


def _ensure_disk_space(directory: Path, required_bytes: int = MIN_FREE_BYTES) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    usage = shutil.disk_usage(directory)
    if usage.free < required_bytes:
        raise SystemExit(
            f"At least {required_bytes / (1024**3):.0f} GiB of free space is required before staging data; "
            f"only {usage.free / (1024**3):.2f} GiB available in {directory}"
        )


def _ensure_kaggle_cli() -> None:
    if shutil.which("kaggle") is not None:
        return
    raise SystemExit(
        "Kaggle CLI not found. Install the kaggle package and configure credentials via "
        "~/.kaggle/kaggle.json before running run_init.py"
    )


def _dataset_present() -> bool:
    csv_dir = WYATT_DATASET_DIR / "csv"
    if csv_dir.exists() and any(csv_dir.glob("*.csv")):
        return True
    return any(WYATT_DATASET_DIR.glob("*.sqlite"))


def _download_dataset(dataset_id: str, *, force: bool) -> None:
    WYATT_DATASET_DIR.mkdir(parents=True, exist_ok=True)
    if not force and _dataset_present():
        logging.info(
            "Existing Kaggle dataset detected at %s; skipping download (use --force to re-download)",
            WYATT_DATASET_DIR,
        )
        return

    logging.info("Downloading %s into %s", dataset_id, WYATT_DATASET_DIR)
    cmd = [
        "kaggle",
        "datasets",
        "download",
        "-d",
        dataset_id,
        "-p",
        str(WYATT_DATASET_DIR),
        "--unzip",
        "-w",
    ]
    if force:
        cmd.append("--force")
    try:
        subprocess.run(
            cmd,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - network side effect
        stdout = exc.stdout.decode() if exc.stdout else ""
        stderr = exc.stderr.decode() if exc.stderr else ""
        logging.error("Kaggle download failed (stdout=%s, stderr=%s)", stdout.strip(), stderr.strip())
        raise SystemExit(1) from exc

    # Some Kaggle CLI versions place artefacts alongside the working directory
    # instead of the requested ``-p`` destination. Move them into place when that
    # happens so the subsequent import logic can rely on a consistent layout.
    stray_csv = ROOT / "csv"
    if stray_csv.exists() and not (WYATT_DATASET_DIR / "csv").exists():
        logging.info("Relocating stray csv directory from %s to %s", stray_csv, WYATT_DATASET_DIR / "csv")
        target = WYATT_DATASET_DIR / "csv"
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(stray_csv), str(target))
    stray_sqlite = ROOT / "nba.sqlite"
    if stray_sqlite.exists() and not (WYATT_DATASET_DIR / stray_sqlite.name).exists():
        logging.info("Relocating stray SQLite database from %s to %s", stray_sqlite, WYATT_DATASET_DIR)
        shutil.move(str(stray_sqlite), str(WYATT_DATASET_DIR / stray_sqlite.name))


def _find_csv_path(candidates: Iterable[str]) -> Optional[Path]:
    lowered = {candidate.lower() for candidate in candidates}
    for path in (WYATT_DATASET_DIR / "csv").rglob("*.csv"):
        if path.name.lower() in lowered:
            return path
    for path in WYATT_DATASET_DIR.rglob("*.csv"):
        if path.name.lower() in lowered:
            return path
    return None


def _find_sqlite_database() -> Optional[Path]:
    candidates = sorted(WYATT_DATASET_DIR.rglob("*.sqlite"))
    return candidates[0] if candidates else None


def _resolve_sqlite_table(sqlite_path: Path, candidates: Iterable[str]) -> Optional[str]:
    with sqlite3.connect(sqlite_path) as conn:
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    lookup = {name.lower(): name for (name,) in rows}
    for candidate in candidates:
        match = lookup.get(candidate.lower())
        if match:
            return match
    return None


def _export_table_from_sqlite(
    sqlite_path: Path, table_name: str, destination: Path, *, force: bool
) -> Path:
    if destination.exists() and not force:
        logging.info("Skipping SQLite export for %s; %s already exists", table_name, destination)
        return destination

    logging.info("Exporting %s from %s -> %s", table_name, sqlite_path.name, destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")
    with sqlite3.connect(sqlite_path) as conn:
        first_chunk = True
        for chunk in pd.read_sql_query(f"SELECT * FROM {table_name}", conn, chunksize=50_000):
            chunk.to_csv(
                tmp_path,
                index=False,
                mode="w" if first_chunk else "a",
                header=first_chunk,
            )
            first_chunk = False
    os.replace(tmp_path, destination)
    return destination


def _copy_table_from_csv(source: Path, destination: Path, *, force: bool) -> Path:
    if destination.exists() and not force:
        logging.info("Skipping copy; %s already exists", destination)
        return destination

    logging.info("Copying %s -> %s", source, destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")
    shutil.copy2(source, tmp_path)
    os.replace(tmp_path, destination)
    return destination


def _import_bootstrap_tables(*, force: bool) -> dict[str, Path]:
    outputs: dict[str, Path] = {}
    sqlite_path = _find_sqlite_database()

    for spec in TABLE_SPECS:
        destination = RAW_BOOTSTRAP_DIR / f"{spec.name}.csv"
        csv_path = _find_csv_path(spec.csv_candidates)
        if csv_path is not None:
            outputs[spec.name] = _copy_table_from_csv(csv_path, destination, force=force)
            continue

        if sqlite_path is None:
            if spec.optional:
                logging.warning("No source found for optional table %s; skipping", spec.name)
                continue
            raise SystemExit(f"Unable to locate a CSV or SQLite source for required table '{spec.name}'.")

        table_name = _resolve_sqlite_table(sqlite_path, spec.sqlite_candidates)
        if table_name is None:
            if spec.optional:
                logging.warning(
                    "SQLite database %s does not contain %s; skipping optional table",
                    sqlite_path,
                    spec.sqlite_candidates,
                )
                continue
            raise SystemExit(
                f"SQLite database {sqlite_path} does not contain any of the expected tables for '{spec.name}'."
            )

        outputs[spec.name] = _export_table_from_sqlite(sqlite_path, table_name, destination, force=force)

    return outputs


def _transform_game_table(game_table_path: Path) -> pd.DataFrame:
    frame = pd.read_csv(game_table_path, low_memory=False)
    if frame.empty:
        logging.warning("Kaggle game table is empty; league game log cannot be derived")
        return frame

    if "game_date" not in frame.columns:
        raise SystemExit("game.csv is missing the 'game_date' column required for watermark generation")

    frame["game_date"] = pd.to_datetime(frame["game_date"], errors="coerce")

    base_columns = [column for column in ("season_id", "game_id", "game_date", "season_type", "min") if column in frame.columns]
    home_columns = [column for column in frame.columns if column.endswith("_home")]
    away_columns = [column for column in frame.columns if column.endswith("_away")]

    if not home_columns or not away_columns:
        raise SystemExit("Unable to identify home/away splits in Kaggle game.csv; cannot derive per-team logs")

    def _reshape(tag: str, columns: list[str]) -> pd.DataFrame:
        data = frame[base_columns + columns].copy()
        suffix = f"_{tag}"
        rename_map = {column: column[: -len(suffix)] for column in columns}
        data.rename(columns=rename_map, inplace=True)
        return data

    home = _reshape("home", home_columns)
    away = _reshape("away", away_columns)

    combined = pd.concat([home, away], ignore_index=True)

    required_columns = [
        "season_id",
        "team_id",
        "team_abbreviation",
        "team_name",
        "game_id",
        "game_date",
        "matchup",
        "wl",
        "min",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "plus_minus",
        "video_available",
        "season_type",
    ]

    for column in required_columns:
        if column not in combined.columns:
            if column == "video_available":
                combined[column] = 0
            else:
                combined[column] = pd.NA

    combined = combined[required_columns]

    combined["season_id"] = combined["season_id"].astype(str).str.strip()
    combined["team_id"] = combined["team_id"].astype(str).str.strip()
    combined["game_id"] = combined["game_id"].astype(str).str.strip()
    combined["wl"] = combined["wl"].astype(str).str.upper()
    combined["season_type"] = combined["season_type"].fillna("Regular Season").astype(str)
    combined["video_available"] = pd.to_numeric(combined["video_available"], errors="coerce").fillna(0).astype(int)

    numeric_columns = [
        "min",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "pts",
        "plus_minus",
    ]
    for column in numeric_columns:
        combined[column] = pd.to_numeric(combined[column], errors="coerce")

    combined["game_date"] = pd.to_datetime(combined["game_date"], errors="coerce")
    combined.dropna(subset=["game_date", "team_id", "game_id"], inplace=True)
    combined.sort_values(["game_date", "game_id", "team_id"], inplace=True)
    combined.reset_index(drop=True, inplace=True)
    combined.columns = combined.columns.str.lower()
    combined["game_date"] = combined["game_date"].dt.date
    return combined


def _write_csv(frame: pd.DataFrame, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = destination.with_suffix(destination.suffix + ".tmp")
    frame.to_csv(tmp_path, index=False)
    os.replace(tmp_path, destination)


def _write_watermark(frame: pd.DataFrame) -> Optional[str]:
    if "game_date" not in frame.columns:
        return None
    valid_dates = frame["game_date"].dropna()
    if valid_dates.empty:
        return None
    watermark_date = max(valid_dates)
    WATERMARK_PATH.parent.mkdir(parents=True, exist_ok=True)
    WATERMARK_PATH.write_text(watermark_date.isoformat())
    return watermark_date.isoformat()


def _log_bootstrap_summary(tables: dict[str, Path], league_log: pd.DataFrame, watermark: Optional[str]) -> None:
    logging.info("Imported Kaggle tables: %s", ", ".join(sorted(tables)))
    if not league_log.empty:
        logging.info("Derived league game log with %s rows", len(league_log))
    if watermark:
        logging.info("Bootstrap watermark set to %s", watermark)
    logging.info("Bootstrap artefacts stored under %s", RAW_BOOTSTRAP_DIR)
    logging.info("Primary game log stored at %s", GAME_CSV)


def main(argv: list[str] | None = None) -> int:
    project_root = ROOT
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    _configure_numeric_environment()
    _configure_logging()

    args = _parse_args(argv or sys.argv[1:])

    logging.info("Starting Kaggle bootstrap at %s", datetime.now().isoformat(timespec="seconds"))

    _ensure_dirs()
    _ensure_disk_space(WYATT_DATASET_DIR)
    _ensure_kaggle_cli()
    _download_dataset(args.dataset_id, force=args.force)

    tables = _import_bootstrap_tables(force=args.force)
    game_table_path = tables.get("game")
    if game_table_path is None:
        raise SystemExit("Failed to import the required game.csv table from the Kaggle dataset")

    league_log = _transform_game_table(game_table_path)
    if not league_log.empty:
        _write_csv(league_log, RAW_BOOTSTRAP_LEAGUELOG_DIR / "leaguegamelog.csv")
        _write_csv(league_log, GAME_CSV)
    watermark = _write_watermark(league_log)

    _log_bootstrap_summary(tables, league_log, watermark)
    logging.info("Finished Kaggle bootstrap")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
