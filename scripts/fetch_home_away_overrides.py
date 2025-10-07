#!/usr/bin/env python3
"""Fetch and store home/away overrides for ambiguous games."""
from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import date, datetime
from typing import Iterable, Sequence

import duckdb
import requests

from nba_db.paths import DUCKDB_PATH

# New imports from user instructions
import random
import os
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# New NBA_HEADERS
NBA_HEADERS = {
    "User-Agent": os.getenv("NBA_UA", "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 "
                                      "(KHTML, like Gecko) Chrome/120 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token": "true",
    "Connection": "keep-alive",
}

# _has_cols from user
def _has_cols(con, table, cols):
    q = """
      SELECT LOWER(column_name)
      FROM information_schema.columns
      WHERE LOWER(table_name)=?
    """
    have = {r[0] for r in con.execute(q, [table.lower()]).fetchall()}
    return all(c.lower() in have for c in cols)

# _resolve_locally from user
def _resolve_locally(con):
    # A) If bronze_game carries home/visitor ids, use them directly
    if _has_cols(con, "bronze_game", {"home_team_id", "visitor_team_id"}):
        return con.execute("""
          SELECT
            game_id,
            ANY_VALUE(CAST(home_team_id AS INTEGER))   AS team_id_home,
            ANY_VALUE(CAST(visitor_team_id AS INTEGER)) AS team_id_away,
            TRUE  AS resolved
          FROM bronze_game
          WHERE game_id IS NOT NULL
          GROUP BY game_id
        """).fetchdf()

    # A.2) If bronze_box_score_team carries home/visitor ids, use them
    if _has_cols(con, "bronze_box_score_team", {"team_id_home", "team_id_away"}):
        return con.execute("""
          SELECT
            game_id,
            ANY_VALUE(CAST(team_id_home AS INTEGER)) AS team_id_home,
            ANY_VALUE(CAST(team_id_away AS INTEGER)) AS team_id_away,
            TRUE AS resolved
          FROM bronze_box_score_team
          WHERE game_id IS NOT NULL
            AND team_id_home IS NOT NULL
            AND team_id_away IS NOT NULL
          GROUP BY game_id
        """).fetchdf()

    # B) Else infer from team box scores using is_home
    if _has_cols(con, "bronze_box_score_team", {"game_id", "team_id", "is_home"}):
        return con.execute("""
          WITH by_game AS (
            SELECT
              game_id,
              MAX(CASE WHEN is_home THEN CAST(team_id AS INTEGER) END) AS team_id_home,
              MAX(CASE WHEN NOT is_home THEN CAST(team_id AS INTEGER) END) AS team_id_away
            FROM bronze_box_score_team
            GROUP BY 1
          )
          SELECT game_id, team_id_home, team_id_away, TRUE AS resolved
          FROM by_game
          WHERE game_id IS NOT NULL
        """).fetchdf()

    # C) Fallback: we only know game_id; mark unresolved so network can try
    return con.execute("""
      SELECT DISTINCT game_id, NULL::INT AS team_id_home, NULL::INT AS team_id_away, FALSE AS resolved
      FROM bronze_game
      WHERE game_id IS NOT NULL
    """).fetchdf()

# Hardened network fetcher from user
def _build_session(timeout=20, retries=5):
    s = requests.Session()
    r = Retry(
        total=retries,
        connect=retries,
        read=retries,
        status=retries,
        backoff_factor=0.7,  # exponential
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=False,  # retry all
        raise_on_status=False,
    )
    s.headers.update(NBA_HEADERS)
    s.mount("https://", HTTPAdapter(max_retries=r, pool_maxsize=10))
    s.timeout = timeout
    proxy = os.getenv("NBA_PROXY")  # optional: http://user:pass@host:port
    if proxy:
        s.proxies.update({"http": proxy, "https": proxy})
    return s

def _polite_sleep(base=0.9, jitter=0.6):
    time.sleep(max(0.4, random.random()*jitter + base))

def _fetch_game_summary(sess, game_id):
    # Example endpoint; adjust to what your code uses
    url = f"https://stats.nba.com/stats/boxscoresummaryv2?GameID={game_id}"
    resp = sess.get(url, timeout=sess.timeout)
    if resp.status_code == 200 and resp.content:
        return resp.json()
    # Bubble meaningful errors
    raise RuntimeError(f"HTTP {resp.status_code} for {game_id}")

# New main function structure
def _ensure_schemas(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS silver")

def _ensure_table(con):
    _ensure_schemas(con)
    con.execute("""
      CREATE TABLE IF NOT EXISTS silver.home_away_overrides (
        game_id        BIGINT PRIMARY KEY,
        date           DATE,
        season         INTEGER,
        team_id_home   INTEGER,
        team_id_away   INTEGER,
        home_override  BOOLEAN,
        away_override  BOOLEAN,
        source         VARCHAR,
        updated_at     TIMESTAMP DEFAULT now()
      )
    """)

def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    # The user did not specify what to do with game_ids, I will keep it.
    parser.add_argument(
        "game_ids",
        nargs="*",
        help="Optional specific game IDs to refresh (defaults to auto-discovery)",
    )
    parser.add_argument(
        "--offline-only",
        action="store_true",
        help="Run only local resolution steps; do not use the network.",
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
    _ensure_table(con)

    local = _resolve_locally(con)  # dataframe with resolved flag
    # Upsert all locally resolved rows immediately (no network):
    if not local.empty:
        con.execute("""
          CREATE OR REPLACE TEMP TABLE local_resolved AS
          SELECT game_id, team_id_home, team_id_away FROM local WHERE resolved
        """)
        con.execute("""
          MERGE INTO silver.home_away_overrides t
          USING local_resolved s
            ON t.game_id = s.game_id
          WHEN MATCHED THEN UPDATE SET
            team_id_home = COALESCE(s.team_id_home, t.team_id_home),
            team_id_away = COALESCE(s.team_id_away, t.team_id_away),
            source = COALESCE(t.source, 'local'),
            updated_at = now()
          WHEN NOT MATCHED THEN INSERT
            (game_id, date, season, team_id_home, team_id_away, home_override, away_override, source, updated_at)
          VALUES (s.game_id, NULL, NULL, s.team_id_home, s.team_id_away, NULL, NULL, 'local', now())
        """)

    if args.offline_only:
        logging.info("Offline-only mode: skipping network resolution.")
        return 0

    # Build network worklist: only those without teams after local pass
    need_net = local[local["resolved"] == False]["game_id"].dropna().astype("int64").tolist()
    
    # Resumability
    already = set(r[0] for r in con.execute("SELECT game_id FROM silver.home_away_overrides WHERE team_id_home IS NOT NULL").fetchall())
    work = [gid for gid in need_net if gid not in already]
    
    if args.game_ids:
        work = [int(gid) for gid in args.game_ids]

    if not work:
        logging.info("No games require network resolution.")
        return 0
        
    logging.info(f"Resolving {len(work)} games via network.")

    sess = _build_session()
    resolved_rows = []
    batch, B = [], 250
    for i, gid in enumerate(work, 1):
        try:
            data = _fetch_game_summary(sess, gid)
            
            result_sets = {rs["name"]: rs for rs in data.get("resultSets", [])}
            summary = result_sets.get("GameSummary")
            if not summary or not summary.get("rowSet"):
                raise RuntimeError(f"GameSummary missing for {gid}")

            headers = summary["headers"]
            row = summary["rowSet"][0]
            home_idx = headers.index("HOME_TEAM_ID")
            away_idx = headers.index("VISITOR_TEAM_ID")
            team_id_home = row[home_idx]
            team_id_away = row[away_idx]

            if team_id_home is None or team_id_away is None:
                raise RuntimeError(f"Incomplete team IDs for {gid}")

            batch.append((gid, team_id_home, team_id_away))
        except Exception as e:
            # Log & continue; don’t die the whole run
            logging.warning(f"[WARN] {gid} -> {e}")
        finally:
            _polite_sleep()

        if len(batch) >= B:
            logging.info(f"Flushing batch of {len(batch)} resolved games.")
            if batch:
                con.execute("CREATE OR REPLACE TEMP TABLE net_resolved(game_id BIGINT, team_id_home INT, team_id_away INT)")
                con.executemany("INSERT INTO net_resolved VALUES (?, ?, ?)", batch)
                con.execute("""
                  MERGE INTO silver.home_away_overrides t
                  USING net_resolved s
                    ON t.game_id = s.game_id
                  WHEN MATCHED THEN UPDATE SET
                    team_id_home = COALESCE(s.team_id_home, t.team_id_home),
                    team_id_away = COALESCE(s.team_id_away, t.team_id_away),
                    source = 'nba_api',
                    updated_at = now()
                  WHEN NOT MATCHED THEN INSERT
                    (game_id, date, season, team_id_home, team_id_away, home_override, away_override, source, updated_at)
                  VALUES (s.game_id, NULL, NULL, s.team_id_home, s.team_id_away, NULL, NULL, 'nba_api', now())
                """)
            batch.clear()
            time.sleep(2.0)

    # final flush
    if batch:
        logging.info(f"Flushing final batch of {len(batch)} resolved games.")
        con.execute("CREATE OR REPLACE TEMP TABLE net_resolved(game_id BIGINT, team_id_home INT, team_id_away INT)")
        con.executemany("INSERT INTO net_resolved VALUES (?, ?, ?)", batch)
        con.execute("""
          MERGE INTO silver.home_away_overrides t
          USING net_resolved s
            ON t.game_id = s.game_id
          WHEN MATCHED THEN UPDATE SET
            team_id_home = COALESCE(s.team_id_home, t.team_id_home),
            team_id_away = COALESCE(s.team_id_away, t.team_id_away),
            source = 'nba_api',
            updated_at = now()
          WHEN NOT MATCHED THEN INSERT
            (game_id, date, season, team_id_home, team_id_away, home_override, away_override, source, updated_at)
          VALUES (s.game_id, NULL, NULL, s.team_id_home, s.team_id_away, NULL, NULL, 'nba_api', now())
        """)

    return 0

if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main(sys.argv[1:]))