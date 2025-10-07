import duckdb
con = duckdb.connect("data/nba.duckdb")
print(con.execute("SELECT * FROM silver.team_minutes LIMIT 5").fetchall())
