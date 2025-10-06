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

The `scripts/update_data.py` helper can be executed directly:

```bash
python scripts/update_data.py --start-date 2024-10-01
```

This command will fetch league game logs from 1 October 2024 until yesterday
and store them under `data/raw`. When neither `--start-date` nor a manifest
value is provided the updater will backfill every day from 1 November 1946 so
your local archive always reflects the full NBA/BAA history. To run from
Python, call `nbapredictor.update_raw_data()`.

If the [Kaggle dataset](https://www.kaggle.com/datasets/wyattowalsh/basketball)
should be downloaded before the incremental update, pass
`--bootstrap-kaggle` (or set `bootstrap_kaggle=True` when using the Python API).
This requires the `kaggle` CLI to be installed and authenticated via the
standard `KAGGLE_USERNAME` and `KAGGLE_KEY` environment variables.

## Testing

The repository contains a small unit test suite. Execute it with `pytest`:

```bash
pytest
```
