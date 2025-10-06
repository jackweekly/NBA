.PHONY: load-db build-schema db

load-db:
	python scripts/load_duckdb.py

build-schema:
	duckdb -c ".read sql/build_schema.sql"

db: load-db build-schema
