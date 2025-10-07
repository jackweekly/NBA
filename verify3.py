import duckdb
con = duckdb.connect("data/nba.duckdb")
print(con.execute("DESCRIBE SELECT team_id_home FROM bronze_box_score_team").fetchall())
print(con.execute("DESCRIBE SELECT team_id_home FROM silver.home_away_overrides").fetchall())
