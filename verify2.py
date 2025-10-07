import duckdb
con = duckdb.connect("data/nba.duckdb")
print("overrides count post:", con.execute("SELECT COUNT(*) FROM silver.home_away_overrides").fetchone()[0])
