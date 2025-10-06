# AGENTS.md — Codex runbook for **NBA** repository

> Purpose: Give an autonomous coding agent (e.g., Codex CLI) clear, safe instructions to **set up**, **bootstrap data**, **run smoke tests**, and **update** the local database for this project. The agent should follow this in order, and open a PR with logs when done.

---

## 0) Operating rules (must follow)

* **Scope:** Only modify files inside this repo directory. Never touch system files outside `/workspace/NBA`.
* **Secrets:** Do **not** commit credentials. Read Kaggle creds from environment or write `~/.kaggle/kaggle.json` at runtime and set permissions to `600`.
* **Rate limits:** Be gentle with `stats.nba.com`. Use browser-like headers, 10–20s timeouts, and ≤16 concurrent workers.
* **Resume & idempotency:** If any step fails mid-way, rerun from the last checkpoint. Never re-download if artifacts exist and pass validation.
* **Disk checks:** Ensure ≥ 15 GB free before downloading Kaggle dataset; clean up temp archives after extracting.
* **Safety:** Never run `rm -rf` outside the repo. When deleting cache, restrict path (`data/external/wyatt/*`).

---

## 1) Environment setup

### 1.1 Create and activate a venv

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip wheel
```

### 1.2 Install Python deps

```bash
pip install -r requirements.txt || true
# Ensure core libs
pip install nba_api pandas pyarrow requests tenacity kaggle sqlite-utils
```

### 1.3 Mitigate native BLAS crashes (cloud CPU quirk)

Set conservative threading to avoid `Floating point exception`:

```bash
export OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1 MKL_NUM_THREADS=1 NUMEXPR_NUM_THREADS=1
# Optional on some CPUs
export OPENBLAS_CORETYPE=HASWELL
```

### 1.4 Configure runtime headers (stats.nba.com)

When calling `nba_api`, pass headers and timeouts. The codebase includes helpers; if missing, the agent should patch them to include:

```python
NBA_HEADERS = {
  "Accept": "application/json, text/plain, */*",
  "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
  "x-nba-stats-origin": "stats",
  "Referer": "https://stats.nba.com/",
}
```

And use `timeout=10` (or 20) with ≤16 workers.

---

## 2) Data bootstrap via Kaggle (Wyatt dataset)

**Goal:** Seed historical data quickly, then fill gaps with the updater.

### 2.1 Provide Kaggle credentials

Agent must support **either** environment variables **or** creating `~/.kaggle/kaggle.json`.

* **Env option:** Ensure `KAGGLE_USERNAME` and `KAGGLE_KEY` are set.
* **File option:**

```bash
mkdir -p ~/.kaggle && chmod 700 ~/.kaggle
printf '%s\n' '{"username":"$KAGGLE_USERNAME","key":"$KAGGLE_KEY"}' > ~/.kaggle/kaggle.json
chmod 600 ~/.kaggle/kaggle.json
```

### 2.2 Download & stage

```bash
mkdir -p data/external/wyatt
kaggle datasets download -d wyattowalsh/basketball -p data/external/wyatt --unzip
```

### 2.3 Import selected tables

Use CSV dumps (fastest). Write to `data/raw/bootstrap/` in Parquet or CSV with fixed schemas.

```bash
python - <<'PY'
import pandas as pd, pathlib as p
root = p.Path('data/external/wyatt')
out  = p.Path('data/raw/bootstrap'); out.mkdir(parents=True, exist_ok=True)
map_ = {
  'game.csv': 'leaguegamelog',
  'team.csv': 'teams',
  'player.csv': 'players',
  'box_score.csv': 'boxscore_traditional',
  'play_by_play.csv': 'playbyplay',
}
for k,v in map_.items():
    f = next(root.rglob(k))
    df = pd.read_csv(f)
    df.to_parquet(out / f"{v}.parquet", index=False)
print('Imported', list(map_.values()))
PY
```

### 2.4 Compute bootstrap watermark

```bash
python - <<'PY'
import pandas as pd, pathlib as p
f = p.Path('data/raw/bootstrap/leaguegamelog.parquet')
df = pd.read_parquet(f)
last = pd.to_datetime(df['GAME_DATE']).max().date()
(p.Path('data/raw/bootstrap/.watermark')).write_text(str(last))
print('WATERMARK', last)
PY
```

---

## 3) Updater to fill the gap (watermark → today)

### 3.1 Determine start date = watermark + 1 day

```bash
START=$(python - <<'PY'
from datetime import date, timedelta
print((date.fromisoformat(open('data/raw/bootstrap/.watermark').read().strip()) + timedelta(days=1)).isoformat())
PY
)
```

### 3.2 Run incremental updater

Prefer the CLI wrapper already in repo:

```bash
python run_daily_update.py "$START"
```

This should append only missing games (and related tables) from the watermark forward.

### 3.3 (Optional) Full-season bulk path

If `--fetch-all-history` is used, ensure the codepath does **season-at-a-time** fetches and writes to `data/raw/game.csv` (or Parquet), not day-by-day. Agent may patch absolute paths to repo-relative:

```python
from pathlib import Path
GAME_CSV = Path(__file__).resolve().parents[1] / 'data' / 'raw' / 'game.csv'
```

---

## 4) Smoke tests (must pass before PR)

1. **Import test:**

```bash
python - <<'PY'
from nba_api.stats.static import players
assert isinstance(players.get_players(), list)
print('OK: players import')
PY
```

2. **Endpoint test with headers:**

```bash
python - <<'PY'
from nba_api.stats.endpoints import commonplayerinfo
headers={
  'Accept':'application/json, text/plain, */*',
  'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
  'x-nba-stats-origin':'stats','Referer':'https://stats.nba.com/'
}
print(commonplayerinfo.CommonPlayerInfo(2544, headers=headers, timeout=10).get_data_frames()[0].head())
PY
```

3. **Bootstrap watermark exists:** `test -f data/raw/bootstrap/.watermark`
4. **Updater writes new rows:**

   * Run `python run_daily_update.py "$START"` then verify the `game` table or file grew.

---

## 5) Logging & artifacts

* Write logs to `logs/AGENT_RUN_YYYYMMDD_HHMMSS.log` and echo key lines to console.
* Save a summary `reports/bootstrap_summary.md` with:

  * Kaggle import row counts per table,
  * watermark date,
  * updater rows added per table,
  * duration and any retries.
* Attach these to the PR.

---

## 6) PR expectations

* Title: `agent: bootstrap via Kaggle + incremental update`
* Include:

  * changeset (if headers/paths were patched),
  * console log excerpt (timeouts/retries),
  * row counts and watermark,
  * follow-up TODOs.

---

## 7) Troubleshooting playbook

* **`ReadTimeout` from stats.nba.com`** → ensure headers set, increase `timeout` to 20, reduce workers to ≤16, add exponential backoff.
* **`Floating point exception`** → export BLAS vars (see §1.3), batch writes (concat once per season instead of per-game), upgrade NumPy/Pandas.
* **Kaggle 403/No auth** → ensure `~/.kaggle/kaggle.json` exists with `chmod 600`, or set `KAGGLE_USERNAME/KAGGLE_KEY`.
* **Disk full** → keep only the needed Kaggle tables; delete zip after unzip; consider writing Parquet (smaller) instead of CSV.

---

## 8) Data layout (expected)

```
NBA/
  data/
    external/wyatt/               # raw Kaggle dump
    raw/
      bootstrap/
        leaguegamelog.parquet
        players.parquet
        teams.parquet
        boxscore_traditional.parquet
        playbyplay.parquet
        .watermark
      # updater outputs
      game.csv or game.parquet
      ... other endpoint tables ...
  logs/
  reports/
```

---

## 9) Non-goals for the agent

* Do **not** scrape non-official sources.
* Do **not** publish large data files in the repository history; keep them in `data/` (gitignored) unless explicitly whitelisted.
* Do **not** change modeling code in this run; this task is data bootstrap + updater verification only.

---

## 10) Acceptance criteria

* Environment is set up and reproducible.
* Kaggle bootstrap imported and watermark recorded.
* Updater filled the gap with correct row counts.
* Smoke tests pass.
* A PR is opened with logs, counts, and a short summary.
