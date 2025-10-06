.PHONY: seed-db build-schema db

seed-db:
	python scripts/seed_duckdb.py

build-schema:
	python scripts/apply_schema.py

db: seed-db build-schema
