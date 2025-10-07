#!/usr/bin/env python3
"""Fetch and store home/away overrides for ambiguous games."""
from __future__ import annotations

import argparse
import concurrent.futures
import logging
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List, Sequence

import duckdb
import requests

from nba_db.paths import DUCKDB_PATH

NBA_API_URL = "https://stats.nba.com/stats/boxscoresummaryv2"
NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}
MAX_ATTEMPTS = 4
BACKOFF_SECONDS = 1.5
MAX_WORKERS = 4


@dataclass
class OverrideRecord:
    """Represent a resolved home/away mapping for a single team."""

    game_id: str
    team_id: str
    is_home: bool


def _ensure_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS silver.home_away_overrides (
          game_id VARCHAR,
          team_id VARCHAR,
          is_home BOOLEAN,
          PRIMARY KEY (game_id, team_id)
        )
        """
    )


def _discover_from_bronze(con: duckdb.DuckDBPyConnection) -> list[str]:
    """Fallback discovery using raw bronze tables when views are unavailable."""

    query = """
        WITH base AS (
          SELECT
            LPAD(CAST(game_id AS VARCHAR), 10, '0') AS game_id,
            COUNT(DISTINCT team_id) AS team_ct,
            COUNT(*) FILTER (WHERE POSITION('vs.' IN LOWER(COALESCE(matchup, ''))) > 0) AS vs_ct,
            COUNT(*) FILTER (WHERE POSITION('@' IN COALESCE(matchup, '')) > 0) AS at_ct
          FROM bronze_game_log_team
          GROUP BY 1
        )
        SELECT b.game_id
        FROM base b
        LEFT JOIN (
          SELECT DISTINCT game_id
          FROM silver.home_away_overrides
        ) o ON o.game_id = b.game_id
        WHERE o.game_id IS NULL
          AND b.team_ct = 2
          AND b.vs_ct = 0
          AND b.at_ct >= 2
        ORDER BY b.game_id
    """
    rows = con.execute(query).fetchall()
    return [row[0] for row in rows]


def _discover_games(con: duckdb.DuckDBPyConnection, explicit: Sequence[str]) -> list[str]:
    if explicit:
        return sorted({game_id.strip() for game_id in explicit if game_id.strip()})

    return _discover_from_bronze(con)


def _fetch_single(game_id: str) -> list[OverrideRecord]:
    params = {"GameID": game_id}
    attempt = 0
    while True:
        attempt += 1
        try:
            response = requests.get(
                NBA_API_URL,
                params=params,
                headers=NBA_HEADERS,
                timeout=15,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:  # pragma: no cover - network handling
            if attempt >= MAX_ATTEMPTS:
                raise RuntimeError(f"Failed to fetch {game_id} after {attempt} attempts") from exc
            sleep_for = BACKOFF_SECONDS * attempt
            logging.warning("Retrying %s (%s/%s): %s", game_id, attempt, MAX_ATTEMPTS, exc)
            time.sleep(sleep_for)
            continue

        result_sets = {rs["name"]: rs for rs in data.get("resultSets", [])}
        summary = result_sets.get("GameSummary")
        if not summary or not summary.get("rowSet"):
            raise RuntimeError(f"GameSummary missing for {game_id}")

        headers = summary["headers"]
        row = summary["rowSet"][0]
        try:
            home_idx = headers.index("HOME_TEAM_ID")
            away_idx = headers.index("VISITOR_TEAM_ID")
        except ValueError as exc:  # pragma: no cover - unexpected payload
            raise RuntimeError(f"Expected columns missing for {game_id}") from exc

        home_team = row[home_idx]
        away_team = row[away_idx]
        if home_team is None or away_team is None:
            raise RuntimeError(f"Incomplete team IDs for {game_id}")

        return [
            OverrideRecord(game_id=game_id, team_id=str(home_team), is_home=True),
            OverrideRecord(game_id=game_id, team_id=str(away_team), is_home=False),
        ]


def _fetch_overrides(game_ids: Sequence[str]) -> tuple[list[OverrideRecord], list[str]]:
    overrides: list[OverrideRecord] = []
    failures: list[str] = []
    if not game_ids:
        return overrides, failures

    workers = min(MAX_WORKERS, len(game_ids)) or 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_by_id = {
            executor.submit(_fetch_single, game_id): game_id for game_id in game_ids
        }
        for future in concurrent.futures.as_completed(future_by_id):
            game_id = future_by_id[future]
            try:
                overrides.extend(future.result())
            except Exception as exc:  # pragma: no cover - network failure path
                logging.error("Failed to resolve %s: %s", game_id, exc)
                failures.append(game_id)
    return overrides, failures


def _prepare_override_rows(records: Iterable[OverrideRecord]) -> list[tuple[str, str, bool]]:
    rows: list[tuple[str, str, bool]] = []
    for record in records:
        game_id = record.game_id.zfill(10)
        team_id = record.team_id.strip()
        rows.append((game_id, team_id, record.is_home))
    return rows


def _store_overrides(
    con: duckdb.DuckDBPyConnection,
    rows: Sequence[tuple[str, str, bool]],
    dry_run: bool,
) -> None:
    if not rows:
        return

    game_ids = sorted({row[0] for row in rows})
    logging.info("Updating overrides for %s games", len(game_ids))

    if dry_run:
        for game_id, team_id, is_home in rows:
            logging.info("DRY RUN: would set %s → %s home=%s", game_id, team_id, is_home)
        return

    delete_sql = "DELETE FROM silver.home_away_overrides WHERE game_id = ?"
    insert_sql = "INSERT OR REPLACE INTO silver.home_away_overrides (game_id, team_id, is_home) VALUES (?, ?, ?)"

    con.execute("BEGIN TRANSACTION")
    try:
        for game_id in game_ids:
            con.execute(delete_sql, [game_id])
        con.executemany(insert_sql, rows)
    finally:  # Ensure transaction closes even on failure
        con.execute("COMMIT")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "game_ids",
        nargs="*",
        help="Optional specific game IDs to refresh (defaults to auto-discovery)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned changes without modifying the overrides table",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable DEBUG logging",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    con = duckdb.connect(str(DUCKDB_PATH))
    con.execute("PRAGMA threads=4")

    _ensure_table(con)

    candidates = _discover_games(con, args.game_ids)
    if not candidates:
        logging.info("No ambiguous games detected; nothing to do")
        return 0

    logging.info("Resolving home/away for %s game(s)", len(candidates))
    overrides, failures = _fetch_overrides(candidates)

    rows = _prepare_override_rows(overrides)
    _store_overrides(con, rows, args.dry_run)

    if failures:
        logging.warning("Failed to resolve %s game(s): %s", len(failures), ", ".join(failures))

    logging.info("Stored overrides for %s team rows", len(rows))
    return 1 if failures and not args.dry_run else 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main())
