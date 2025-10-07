#!/usr/bin/env python3
"""Run warehouse quality checks and exit non-zero on failure."""
from __future__ import annotations

import sys

import duckdb

MODERN_START_YEAR = 2010
DB_PATH = "data/nba.duckdb"


def _query_scalar(con: duckdb.DuckDBPyConnection, sql: str, *params) -> int:
    return con.execute(sql, params).fetchone()[0]


def main() -> int:
    con = duckdb.connect(DB_PATH, read_only=True)

    con.execute(
        """
        CREATE OR REPLACE TEMP VIEW v_modern AS
        SELECT sgc.*, h.start_year
        FROM silver.team_game_cov AS sgc
        JOIN helper.helper_season_year AS h USING (season_id)
        WHERE h.start_year IS NOT NULL AND h.start_year >= ?
        """,
        [MODERN_START_YEAR],
    )

    failures: list[str] = []

    bad_wl = _query_scalar(
        con,
        """
        WITH k AS (
            SELECT game_id, season_type,
                   SUM(CASE WHEN wl = 'W' THEN 1 ELSE 0 END) AS w,
                   SUM(CASE WHEN wl = 'L' THEN 1 ELSE 0 END) AS l
            FROM v_modern
            GROUP BY 1, 2
        )
        SELECT COUNT(*) FROM k WHERE NOT (w = 1 AND l = 1)
        """,
    )
    if bad_wl:
        failures.append(f"WL imbalance games: {bad_wl}")

    bad_homeaway = _query_scalar(
        con,
        """
        WITH f AS (
            SELECT game_id, season_type,
                   SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS home_ct,
                   SUM(CASE WHEN is_home = FALSE THEN 1 ELSE 0 END) AS away_ct
            FROM v_modern
            GROUP BY 1, 2
        )
        SELECT COUNT(*) FROM f WHERE home_ct <> 1 OR away_ct <> 1
        """,
    )
    if bad_homeaway:
        failures.append(f"Home/Away imbalance games: {bad_homeaway}")

    bad_pct = con.execute(
        """
        SELECT
          SUM(CASE WHEN fga > 0 AND (fg_pct IS NULL OR fg_pct < 0 OR fg_pct > 1) THEN 1 ELSE 0 END) AS fg_bad,
          SUM(CASE WHEN fg3a > 0 AND (fg3_pct IS NULL OR fg3_pct < 0 OR fg3_pct > 1) THEN 1 ELSE 0 END) AS fg3_bad,
          SUM(CASE WHEN fta > 0 AND (ft_pct IS NULL OR ft_pct < 0 OR ft_pct > 1) THEN 1 ELSE 0 END) AS ft_bad
        FROM v_modern
        """,
    ).fetchone()
    if any(value and value > 0 for value in bad_pct):
        failures.append(
            "Out-of-range/null pct in modern seasons: "
            f"fg={bad_pct[0]}, fg3={bad_pct[1]}, ft={bad_pct[2]}"
        )

    bad_min = _query_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM v_modern
        WHERE min < 200 OR min > 330
        """,
    )
    if bad_min:
        failures.append(f"Implausible team minutes rows: {bad_min}")

    bad_pts = _query_scalar(
        con,
        """
        SELECT COUNT(*)
        FROM v_modern
        WHERE has_box_score_team
          AND pts IS NOT NULL
          AND ABS(pts - calc_pts) > 2
        """,
    )
    if bad_pts:
        failures.append(f"Points identity mismatches (modern + has box score): {bad_pts}")

    bad_team_dim = _query_scalar(
        con,
        """
        WITH t AS (
          SELECT DISTINCT team_id FROM silver.team_dim
        )
        SELECT COUNT(*)
        FROM (
          SELECT DISTINCT team_id FROM v_modern
        ) m
        LEFT JOIN t USING (team_id)
        WHERE t.team_id IS NULL
        """,
    )
    if bad_team_dim:
        failures.append(f"Unknown team_id in modern seasons: {bad_team_dim}")

    if failures:
        print("QUALITY CHECK FAILURES:")
        for failure in failures:
            print(" -", failure)
        return 1

    print("Quality checks passed.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
