import duckdb
con = duckdb.connect("data/nba.duckdb")
print("bronze_game cols:", [r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE lower(table_name)='bronze_game' ORDER BY 1
""").fetchall()])
print("bronze_box_score_team cols:", [r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE lower(table_name)='bronze_box_score_team' ORDER BY 1
""").fetchall()])
print("overrides count pre:", con.execute("SELECT COUNT(*) FROM silver.home_away_overrides").fetchone()[0])