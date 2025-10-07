import duckdb
con = duckdb.connect("data/nba.duckdb")
print(con.execute("SELECT * FROM silver.game_enriched LIMIT 5").fetchall())
