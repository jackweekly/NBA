import duckdb

con = duckdb.connect("data/nba.duckdb")

def peek(title, q):
    print("\n==", title, "==")
    try:
        print(con.sql(q).df().head().to_string(index=False))
    except Exception as e:
        print("ERR:", e)

# Existence
peek("silver.team_rows_from_game (team rows)", "SELECT game_id, team_id, team_abbreviation, side FROM silver.team_rows_from_game LIMIT 6")
peek("silver.team_minutes (NULL minutes)", "SELECT * FROM silver.team_minutes LIMIT 6")
peek("silver.game_enriched (OT inference)", "SELECT game_id, season_type, ot_periods, target_minutes_per_team FROM silver.game_enriched ORDER BY game_date DESC LIMIT 6")

con.close()

