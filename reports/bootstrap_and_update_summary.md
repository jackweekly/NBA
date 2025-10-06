# Bootstrap and Update Summary

- Kaggle bootstrap watermark: 2023-06-12 (saved to `data/raw/bootstrap/.watermark`).
- Bootstrap league game log (`data/raw/bootstrap/leaguegamelog/leaguegamelog.csv`) rows: 131,396.
- Consolidated game log (`data/raw/game.csv`) rows after processing: 136,960 covering 1946-11-01 through 2025-10-05.
- Rows appended during the first incremental update: 5,564.
- Idempotency check: second incremental run appended 0 rows (row count remained 136,960).
- Notable external call behaviour: stats.nba.com returned HTTP 400 for "In Season Tournament" seasons without data; these were skipped automatically after brief retries. No other retries or timeouts were observed.
