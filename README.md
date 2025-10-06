# NBA Predictor Data Pipeline

This project provides a lightweight data ingestion layer for a future NBA prediction
system. The core component is a synchronisation script that downloads daily
NBA game statistics and keeps a local collection of raw CSV files aligned with
updates made to [wyattowalsh/nbadb](https://github.com/wyattowalsh/nbadb).

## Features

* Pulls daily NBA game logs using the official `nba_api` client.
* Mirrors the update cadence of the upstream `nbadb` project by tracking the
  last processed date in a manifest file.
* Supports optional bootstrapping of the upstream Kaggle dataset when the
  `kaggle` CLI and credentials are available, importing the league game log
  into season-partitioned CSV files and updating the manifest watermark.
* Produces one CSV file per day inside `data/raw` so downstream modelling
  code can perform incremental loading, while season-level backfills are
  partitioned by season for faster historical refreshes.

## Installation

```bash
pip install -e .
```

The package depends on `requests`, `pandas`, `nba_api`, and `python-dateutil`.

## Usage

The repository mirrors the interface provided by the
[`wyattowalsh/nbadb`](https://github.com/wyattowalsh/nbadb) utilities so you can
drive updates with lightweight scripts at the project root.

### One-time bootstrap (Kaggle)

1. Configure the Kaggle CLI (only needs to be done once):

   ```bash
   pip install kaggle
   mkdir -p ~/.kaggle
   printf '%s\n' '{"username":"<your_user>","key":"<your_api_key>"}' > ~/.kaggle/kaggle.json
   chmod 600 ~/.kaggle/kaggle.json
   ```

2. Run the bootstrap helper:

   ```bash
   python run_init.py
   ```

   This downloads the Kaggle dataset into `data/external/wyatt`, imports the
   league game log into `data/raw/leaguegamelog/season=*`, and records the last
   available game date in `data/raw/manifest.json`. The bootstrap script mirrors
   the numeric-environment guards used by the daily updater so the heavy
   ingestion runs on constrained CPUs without crashing native BLAS libraries.

### Daily updates

```bash
python run_daily_update.py --fetch-all-history
```

This performs a complete historical backfill by delegating to
`nba_db.update.daily(fetch_all_history=True)`. The historical branch pulls one
season at a time and writes the results to
`data/raw/leaguegamelog/season=<YEAR-YEAR>/part-000.csv`, which drastically
reduces the number of API calls compared to day-by-day loops. To backfill from a
specific date instead, provide the desired start date:

```bash
python run_daily_update.py 2010-10-01
```

Once you have bootstrapped from Kaggle, invoking the script without arguments
only fetches games that were not included in the dataset dump. The helper
adjusts `sys.path` so `from nba_db import update` resolves correctly when
executed directly. Both the CLI wrapper and the underlying library set
conservative defaults for the common OpenBLAS and MKL environment variables
(`OMP_NUM_THREADS=1`, `OPENBLAS_CORETYPE=HASWELL`, etc.) to avoid the "Floating
point exception" crashes that occasionally surface on virtualised CPUs.

Python callers can continue using `nbapredictor.update_raw_data()` directly if
they prefer working with the richer API exposed by this repository. The helper
`scripts/update_data.py` mirrors the same behaviour and accepts a
`--fetch-all-history` flag when you want to trigger the season-wise refresh
outside of the wrapper that emulates `nba_db`.

## Testing

The repository contains a small unit test suite. Execute it with `pytest`:

```bash
pytest
```
