import duckdb
from datetime import date

LEGACY_CUTOFF_DATE = "2010-01-01"  # anything older => warnings only

def main():
    con = duckdb.connect("data/nba.duckdb")

    # minutes comparison
    q = """
      WITH games AS (
        SELECT DISTINCT game_id FROM bronze_game_norm
      ),
      joined AS (
        SELECT
          tm.game_id,
          tm.team_id,
          ge.game_date,
          ge.season_type,
          tm.minutes_raw,
          ge.target_minutes_per_team AS minutes_target
        FROM silver.team_minutes tm
        JOIN games g USING (game_id)
        JOIN silver.game_enriched ge USING (game_id)
      )
      SELECT
        game_id, team_id, season_type, game_date,
        minutes_raw, minutes_target,
        CASE WHEN minutes_target IS NOT NULL AND ABS(minutes_raw - minutes_target) > 1.0
             THEN TRUE ELSE FALSE END AS min_bad
      FROM joined
      ORDER BY game_date DESC
      LIMIT 20000
    """
    rows = con.execute(q).fetchall()

    bad = [r for r in rows if r[-1] and r[3] >= date.fromisoformat(LEGACY_CUTOFF_DATE)]
    legacy = [r for r in rows if r[-1] and r[3] < date.fromisoformat(LEGACY_CUTOFF_DATE)]

    if bad:
        print("QUALITY CHECK FAILURES:")
        print(f" - Implausible team minutes rows (modern): {len(bad)}")
        for r in bad[:12]:
            print(f"    game_id='{r[0]}', team_id='{r[1]}', season_type='{r[2]}', "
                  f"game_date={r[3]}, minutes_raw={r[4]}, minutes_target={r[5]}, min_bad=True")
    if legacy:
        print("QUALITY WARNINGS (pre-2010 or partial rows):")
        print(f" - Implausible team minutes rows (legacy): {len(legacy)}")
        for r in legacy[:12]:
            print(f"    game_id='{r[0]}', season_type='{r[2]}', game_date={r[3]}, "
                  f"minutes_raw={r[4]}, minutes_target={r[5]}")

    # WL/home/away imbalance: treat as warnings if legacy
    q2 = """
      WITH games AS (SELECT DISTINCT game_id FROM bronze_game_norm),
      flags AS (
        SELECT
          g.game_id,
          ge.game_date,
          ge.season_type,
          CASE WHEN har.team_id_home IS NULL THEN 0 ELSE 1 END AS has_home,
          CASE WHEN har.team_id_away IS NULL THEN 0 ELSE 1 END AS has_away
        FROM games g
        LEFT JOIN silver.home_away_resolved har USING (game_id)
        JOIN silver.game_enriched ge USING (game_id)
      )
      SELECT game_id, season_type, game_date,
             SUM(has_home) AS home_ct,
             SUM(has_away) AS away_ct,
             (2 - SUM(has_home) - SUM(has_away)) AS null_ct  -- exactly two flags expected
      FROM flags
      GROUP BY 1,2,3
      HAVING (home_ct != 1 OR away_ct != 1)
    """
    rows2 = con.execute(q2).fetchall()
    modern_imb = [r for r in rows2 if r[2] >= date.fromisoformat(LEGACY_CUTOFF_DATE)]
    legacy_imb = [r for r in rows2 if r[2] < date.fromisoformat(LEGACY_CUTOFF_DATE)]

    if modern_imb:
        print("QUALITY CHECK FAILURES:")
        print(f" - Home/Away imbalance games (modern): {len(modern_imb)}")
        for r in modern_imb[:12]:
            print(f"    game_id='{r[0]}', season_type='{r[1]}', game_date={r[2]}, "
                  f"home_ct={r[3]}, away_ct={r[4]}, null_ct={r[5]}")
    if legacy_imb:
        print("QUALITY WARNINGS (legacy):")
        print(f" - Home/Away imbalance games: {len(legacy_imb)}")
        for r in legacy_imb[:12]:
            print(f"    game_id='{r[0]}', season_type='{r[1]}', game_date={r[2]}, "
                  f"home_ct={r[3]}, away_ct={r[4]}, null_ct={r[5]}")

    # Exit code policy: fail only if modern failures exist
    if bad or modern_imb:
        raise SystemExit(1)

if __name__ == "__main__":
    main()