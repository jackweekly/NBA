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
  `kaggle` CLI and credentials are available.
* Produces one CSV file per day inside `data/raw` so downstream modelling
  code can perform incremental loading.

## Installation

```bash
pip install -e .
```

The package depends on `requests`, `pandas`, `nba_api`, and `python-dateutil`.

## Usage

The repository mirrors the interface provided by the
[`wyattowalsh/nbadb`](https://github.com/wyattowalsh/nbadb) utilities so you can
drive updates with lightweight scripts at the project root.

### Daily updates

```bash
python run_daily_update.py --fetch-all-history
```

This performs a complete historical backfill by delegating to
`nba_db.update.daily(fetch_all_history=True)`. To backfill from a specific date
instead, provide the desired start date:

```bash
python run_daily_update.py 2010-10-01
```

Invoking the script without arguments only fetches new games since the last run.
The helper adjusts `sys.path` so `from nba_db import update` resolves correctly
when executed directly.

### One-time bootstrap

```bash
python run_init.py
```

This seeds the local data directory by downloading the upstream Kaggle dataset
via `nba_db.update.init()`. The Kaggle CLI must be installed and authenticated
through the standard `KAGGLE_USERNAME`/`KAGGLE_KEY` environment variables.

Python callers can continue using `nbapredictor.update_raw_data()` directly if
they prefer working with the richer API exposed by this repository.

## Testing

The repository contains a small unit test suite. Execute it with `pytest`:

```bash
pytest
```
