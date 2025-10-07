import duckdb
con = duckdb.connect("data/nba.duckdb")
print("bronze_game cols:", [r[0] for r in con.execute("""
  SELECT column_name FROM information_schema.columns
  WHERE lower(table_name)='bronze_game' ORDER BY 1
""").fetchall()])
