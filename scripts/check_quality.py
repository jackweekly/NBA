#!/usr/bin/env python3
"""Run warehouse quality checks and exit non-zero on modern failures."""
from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import List, Sequence, Tuple

import duckdb

DB_PATH = "data/nba.duckdb"

MODERN_FILTER = "COALESCE(row_valid_modern, FALSE)"
LEGACY_FILTER = "COALESCE(row_valid_any, FALSE) AND NOT COALESCE(row_valid_modern, FALSE)"


@dataclass(frozen=True)
class QualityCheck:
    """Container for a quality check definition."""

    key: str
    description: str
    sql_template: str  # Must include a ``{filter}`` placeholder.


CHECKS: Tuple[QualityCheck, ...] = (
    QualityCheck(
        key="wl_imbalance",
        description="WL imbalance games",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            season_type,
            MAX(game_date) AS game_date,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(wl, '')) = 'W') AS wins,
            COUNT(*) FILTER (WHERE UPPER(COALESCE(wl, '')) = 'L') AS losses
          FROM silver.team_game_cov
          WHERE {filter}
          GROUP BY game_id, season_type
          HAVING wins <> 1 OR losses <> 1
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id
        LIMIT 20
        """,
    ),
    QualityCheck(
        key="home_away_imbalance",
        description="Home/Away imbalance games",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            season_type,
            MAX(game_date) AS game_date,
            COUNT(*) FILTER (WHERE is_home IS TRUE) AS home_ct,
            COUNT(*) FILTER (WHERE is_home IS FALSE) AS away_ct,
            COUNT(*) FILTER (WHERE is_home IS NULL) AS null_ct
          FROM silver.team_game_cov
          WHERE {filter}
          GROUP BY game_id, season_type
          HAVING home_ct <> 1 OR away_ct <> 1 OR null_ct > 0
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id
        LIMIT 20
        """,
    ),
    QualityCheck(
        key="pct_out_of_range",
        description="Out-of-range/null pct rows",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            team_id,
            team_name,
            season_type,
            game_date,
            fgm,
            fga,
            fg_pct_silver,
            fg_pct_raw,
            fg3m,
            fg3a,
            fg3_pct_silver,
            fg3_pct_raw,
            ftm,
            fta,
            ft_pct_silver,
            ft_pct_raw
          FROM silver.team_game_cov
          WHERE {filter}
            AND (
              (fga > 0 AND (fg_pct_silver IS NULL OR fg_pct_silver < 0 OR fg_pct_silver > 1)) OR
              (fg3a > 0 AND (fg3_pct_silver IS NULL OR fg3_pct_silver < 0 OR fg3_pct_silver > 1)) OR
              (fta > 0 AND (ft_pct_silver IS NULL OR ft_pct_silver < 0 OR ft_pct_silver > 1))
            )
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id, team_id
        LIMIT 20
        """,
    ),
    QualityCheck(
        key="minutes_implausible",
        description="Implausible team minutes rows",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            team_id,
            team_name,
            season_type,
            game_date,
            minutes_raw,
            min_silver,
            min_bad
          FROM silver.team_game_cov
          WHERE {filter} AND min_bad
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id, team_id
        LIMIT 20
        """,
    ),
    QualityCheck(
        key="points_mismatch",
        description="Points identity mismatches (has box score)",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            team_id,
            team_name,
            season_type,
            game_date,
            pts_original,
            pts_silver,
            pts_from_bx,
            pts_calc,
            pts_mismatch_flag
          FROM silver.team_game_cov
          WHERE {filter} AND has_box_score_team AND pts_mismatch_flag
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id, team_id
        LIMIT 20
        """,
    ),
    QualityCheck(
        key="team_unknown",
        description="Unknown team_id in seasons",
        sql_template="""
        WITH flagged AS (
          SELECT
            game_id,
            team_id,
            team_name,
            season_type,
            game_date,
            start_year,
            team_known
          FROM silver.team_game_cov
          WHERE {filter} AND NOT team_known
        )
        SELECT *, COUNT(*) OVER () AS total_rows
        FROM flagged
        ORDER BY game_date DESC NULLS LAST, game_id, team_id
        LIMIT 20
        """,
    ),
)


def _run_query(con: duckdb.DuckDBPyConnection, sql: str) -> tuple[list[str], list[tuple]]:
    rel = con.execute(sql)
    columns = [desc[0] for desc in rel.description]
    rows = rel.fetchall()
    return columns, rows


def _get_total(columns: Sequence[str], rows: Sequence[tuple]) -> int:
    if not rows:
        return 0
    idx = columns.index("total_rows")
    return int(rows[0][idx])


def _print_rows(label: str, columns: Sequence[str], rows: Sequence[tuple]) -> None:
    if not rows:
        return
    print(label)
    display_cols = [col for col in columns if col != "total_rows"]
    for row in rows:
        data = {col: value for col, value in zip(columns, row)}
        parts = [f"{col}={data[col]!r}" for col in display_cols]
        print(f"    {', '.join(parts)}")


def _collect(
    con: duckdb.DuckDBPyConnection,
    check: QualityCheck,
    filter_sql: str,
) -> tuple[int, list[str], list[tuple]]:
    columns, rows = _run_query(con, check.sql_template.format(filter=filter_sql))
    return _get_total(columns, rows), columns, rows


def main() -> int:
    con = duckdb.connect(DB_PATH, read_only=True)

    failures: List[str] = []
    legacy_payload: List[tuple[str, list[str], list[tuple]]] = []

    for check in CHECKS:
        modern_total, modern_cols, modern_rows = _collect(con, check, MODERN_FILTER)
        legacy_total, legacy_cols, legacy_rows = _collect(con, check, LEGACY_FILTER)

        if modern_total:
            failures.append(f"{check.description}: {modern_total}")
            _print_rows("  Modern sample:", modern_cols, modern_rows)

        if legacy_total:
            legacy_payload.append((f"{check.description}: {legacy_total}", legacy_cols, legacy_rows))

    if failures:
        print("QUALITY CHECK FAILURES:")
        for failure in failures:
            print(f" - {failure}")
        if legacy_payload:
            print("\nQUALITY WARNINGS (pre-2010 or partial rows):")
            for message, columns, rows in legacy_payload:
                print(f" - {message}")
                _print_rows("    Legacy sample:", columns, rows)
        return 1

    if legacy_payload:
        print("QUALITY WARNINGS (pre-2010 or partial rows):")
        for message, columns, rows in legacy_payload:
            print(f" - {message}")
            _print_rows("    Legacy sample:", columns, rows)

    print("Quality checks passed for modern seasons.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    sys.exit(main())
