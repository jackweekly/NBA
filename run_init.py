#!/usr/bin/env python3
"""Bootstrap Kaggle data into the local ``data/raw`` directory."""
from __future__ import annotations

import argparse
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime

import gzip, shutil, sqlite3, sys
from pathlib import Path
import pandas as pd

from nba_db.paths import GAME_CSV, RAW_DIR, ROOT


def _first_existing(*candidates: list[Path]) -> Path | None:
  for p in candidates:
    if p and p.exists():
      return p
  return None

def find_or_materialize_game_csv() -> Path | None:
  # 1) direct or nested CSVs (case-insensitive)
  csvs = list(RAW_DIR.rglob("game.csv")) + list(RAW_DIR.rglob("Game.csv"))
  if not csvs:
    csvs = list(RAW_DIR.rglob("games.csv")) + list(RAW_DIR.rglob("Games.csv"))
  if csvs:
    src = csvs[0]
    src = src.resolve()
    if src.suffix == ".csv":
      return src

  # 2) gzipped CSV
  gz = _first_existing(*(RAW_DIR.rglob("game.csv.gz")))
  if gz:
    tmp = gz.with_suffix("")  # strip .gz
    with gzip.open(gz, "rb") as f_in, open(tmp, "wb") as f_out:
      shutil.copyfileobj(f_in, f_out)
    return tmp

  # 3) SQLite fallback → dump table 'game'
  sqlite_files = list(RAW_DIR.rglob("*.sqlite")) + list(RAW_DIR.rglob("*.db"))
  if sqlite_files:
    db = sqlite_files[0]
    with sqlite3.connect(db) as conn:
      try:
        df = pd.read_sql("SELECT * FROM game", conn)
      except Exception as e:
        print(f"[ERROR] Could not read 'game' table from {db}: {e}", file=sys.stderr)
        return None
    out = RAW_DIR / "game.csv"
    df.to_csv(out, index=False)
    return out

  return None

def normalize_and_place_game_csv(src_csv: Path) -> None:
  RAW_DIR.mkdir(parents=True, exist_ok=True)
  df = pd.read_csv(src_csv)
  df.columns = df.columns.str.lower()
  if "game_date" in df.columns:
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
  if "season_type" not in df.columns:
    df["season_type"] = "Regular Season"
  tmp = GAME_CSV.with_suffix(".csv.tmp")
  df.to_csv(tmp, index=False)
  tmp.replace(GAME_CSV)
  print(f"[OK] Seeded {GAME_CSV} rows={len(df)}, window={df['game_date'].min()}→{df['game_date'].max()}")


DEFAULT_DATASET_ID = "wyattowalsh/basketball"
DEFAULT_SEASON_TYPE = "Regular Season"


_NUMERIC_ENV_DEFAULTS = {
    "OMP_NUM_THREADS": "1",
    "OPENBLAS_NUM_THREADS": "1",
    "MKL_NUM_THREADS": "1",
    "NUMEXPR_NUM_THREADS": "1",
    "OPENBLAS_CORETYPE": "HASWELL",
}


def _configure_numeric_environment() -> None:
    for key, value in _NUMERIC_ENV_DEFAULTS.items():
        os.environ.setdefault(key, value)


def _configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download and normalise Kaggle bootstrap data")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download the Kaggle dataset even if local files already exist",
    )
    parser.add_argument(
        "--dataset-id",
        default=DEFAULT_DATASET_ID,
        help="Kaggle dataset identifier to download",
    )
    return parser.parse_args(argv)


def _ensure_kaggle_cli() -> None:
    if shutil.which("kaggle") is not None:
        return
    message = (
        "Kaggle CLI not found. Install kaggle and configure credentials via "
        "~/.kaggle/kaggle.json or KAGGLE_USERNAME/KAGGLE_KEY before running run_init.py"
    )
    raise SystemExit(message)


def _clear_raw_dir() -> None:
    if not RAW_DIR.exists():
        return
    for entry in RAW_DIR.iterdir():
        if entry.is_file() or entry.is_symlink():
            entry.unlink()
        elif entry.is_dir():
            shutil.rmtree(entry)


def _download_dataset(dataset_id: str, *, force: bool) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if not force and any(RAW_DIR.glob("*.csv")):
        logging.info("Existing Kaggle exports detected; skipping download (use --force to re-download)")
        return

    if force:
        logging.info("--force supplied; clearing %s before download", RAW_DIR)
        _clear_raw_dir()

    logging.info("Downloading %s into %s", dataset_id, RAW_DIR)
    try:
        subprocess.run(
            [
                "kaggle",
                "datasets",
                "download",
                "-d",
                dataset_id,
                "-p",
                str(RAW_DIR),
                "--unzip",
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.CalledProcessError as exc:  # pragma: no cover - requires kaggle CLI
        logging.error("Kaggle download failed: %s", exc.stderr.decode() if exc.stderr else exc)
        raise SystemExit(1) from exc





def _log_summary() -> None:
    frame = pd.read_csv(GAME_CSV)
    row_count = len(frame)
    if "game_date" in frame.columns and not frame["game_date"].dropna().empty:
        min_date = frame["game_date"].min()
        max_date = frame["game_date"].max()
        window = f"{min_date.date()} → {max_date.date()}"
    else:
        window = "unknown"

    logging.info("Normalised game.csv with %s rows covering %s", row_count, window)

    csv_paths = sorted(RAW_DIR.glob("*.csv"))
    if not csv_paths:
        logging.warning("No CSV files detected in %s after download", RAW_DIR)
        return

    logging.info("Kaggle file sizes:")
    for path in csv_paths:
        size_mb = path.stat().st_size / (1024 * 1024)
        logging.info("  %s: %.2f MB", path.relative_to(ROOT), size_mb)


def main(argv: list[str] | None = None) -> int:
    project_root = ROOT
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    _configure_numeric_environment()
    _configure_logging()

    args = _parse_args(argv or sys.argv[1:])

    logging.info("Starting Kaggle bootstrap at %s", datetime.now().isoformat(timespec="seconds"))

    _ensure_kaggle_cli()
    _download_dataset(args.dataset_id, force=args.force)

    src = find_or_materialize_game_csv()
    if not src:
      # Debugging aid: show what *was* downloaded
      print(f"[ERROR] Could not locate game.csv. Files under {RAW_DIR}:", file=sys.stderr)
      for p in RAW_DIR.rglob("*"):
        print(" -", p.relative_to(RAW_DIR), file=sys.stderr)
      sys.exit(1)

    normalize_and_place_game_csv(src)
    _log_summary()

    logging.info("Finished Kaggle bootstrap")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
