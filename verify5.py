import duckdb
con = duckdb.connect("data/nba.duckdb")
print("bronze_box_score columns:", [r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE lower(table_name)='bronze_box_score' ORDER BY 1
""").fetchall()])
print("bronze_box_score_team columns:", [r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE lower(table_name)='bronze_box_score_team' ORDER BY 1
""").fetchall()])
