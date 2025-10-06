Purpose
Guide an autonomous coding agent to set up, download historical NBA data, run updates to the present day, and verify results. This file contains no code; all instructions are written in plain language. The agent must download the data so it knows what’s going on.

Scope and guardrails
• Only modify files inside this repository directory.
• Never commit credentials into version control other than what appears in this document at the user’s request.
• Keep all large datasets inside the data/ directory, which should be ignored by Git history unless explicitly allowed.
• Be gentle to stats.nba.com: use browser-like HTTP headers, reasonable timeouts, and at most sixteen concurrent workers.
• Every step must be resumable. If a step fails, continue from the last completed checkpoint.
• Ensure at least fifteen gigabytes of free disk space before staging external data.
• Do not delete anything outside this repository. When cleaning caches, restrict deletions to data/external/wyatt.

Kaggle credentials (required)
Create a Kaggle credentials file for API access. The exact file content must be the following single line of JSON:

{"username":"jweeklybupper","key":"b864a0201afff2ec528419c1f0410375"}

Write this exact text to the file located at the path “~/.kaggle/kaggle.json” on the machine where the agent is running. Make sure the parent folder exists and the file is readable by the agent. The agent must not print this key to logs. If this repository is public, be aware this credential is exposed by design per the user’s instruction in this runbook.

High-level workflow

Environment readiness
• Confirm a working Python environment and the ability to read and write within this repository.
• Confirm the ability to read and write Parquet and CSV files under data/.
• Confirm internet access for Kaggle and stats.nba.com.

Bootstrap historical data from Kaggle (Wyatt dataset)
• Use the Kaggle API with the credential above to download the dataset identified by the slug “wyattowalsh/basketball”.
• Unpack the dataset into the folder data/external/wyatt within this repository.
• From that dataset, locate the following tables: game.csv, team.csv, player.csv, box_score.csv, and play_by_play.csv.
• Import these tables into the project’s raw bootstrap area at data/raw/bootstrap/, preserving all columns.
• After import, determine the latest game date present in the imported game table; this becomes the “bootstrap watermark date”.
• Store the watermark date as a plain ISO date string in a small file named .watermark inside data/raw/bootstrap/.

Build or refresh the project’s bulk game log
• Ensure the repository’s bulk fetch routine produces a single large season-level game log file at data/raw/game.csv (or an equivalent file in the same folder).
• If such a file already exists and is consistent, reuse it; otherwise, produce or repair it so that it reflects the complete history.

Incremental update from watermark to today
• Use the bootstrap watermark date from data/raw/bootstrap/.watermark.
• Define the start date as the calendar day immediately after the watermark date.
• Perform an incremental update that fetches all missing games and related data from that start date up to and including “today” on the agent’s clock.
• Append newly fetched rows to the existing outputs using stable primary keys to avoid duplicates.
• Respect rate limits and reliability guidelines when calling stats.nba.com (browser-like headers, reasonable timeouts, and limited parallelism).

Idempotency check
• Run the exact same incremental update a second time without changing inputs.
• Confirm that the row counts of the updated outputs do not increase on the second run, indicating that duplicate insertion did not occur.

Health and quality checks
• Confirm that data/raw/bootstrap/leaguegamelog (or its equivalent) contains a large number of rows consistent with full historical coverage.
• Confirm that data/raw/game.csv exists and also contains a large number of rows.
• Confirm that in the league game log, the logical primary key behaves as expected. For team-level logs, a typical check is that each game identifier appears twice, once per team.
• Confirm that the most recent game date in the updated outputs is on or after the start date derived from the watermark.
• Confirm that all expected folders exist: data/external/wyatt, data/raw/bootstrap, data/raw, logs, and reports.

Summary report for humans
• Create a short human-readable summary file at reports/bootstrap_and_update_summary.md describing:
– The watermark date detected in the Kaggle import.
– The total number of rows in the main game log after processing.
– The number of rows added by the incremental update (which may be zero if the Kaggle dataset already covered the latest games).
– The result of the idempotency check.
– Any notable retries or timeouts that occurred during external calls.

Reliability guidelines for stats.nba.com
• Always send browser-like HTTP headers and a legitimate user agent string.
• Use timeouts that allow for slow responses.
• Limit concurrent workers to at most sixteen.
• If an external call fails transiently, retry a small number of times with exponential backoff.
• Avoid per-day crawling for full history; prefer season-level or bulk endpoints for initial backfills. Use per-day only for daily increments.

Data layout expectations
• The Kaggle dataset is staged under data/external/wyatt.
• The imported bootstrap tables live under data/raw/bootstrap/ alongside a file named .watermark containing a single ISO date.
• The project’s large, consolidated game log lives under data/raw/ as game.csv (or an equivalent file with the same meaning).
• Logs and human summaries live under logs/ and reports/ respectively.

Acceptance criteria
• The agent downloaded and staged the Kaggle dataset using the provided credential.
• The agent imported the required tables and recorded the bootstrap watermark date.
• The agent produced or refreshed the large consolidated game log in data/raw/.
• The agent ran an incremental update from the watermark to today.
• The agent reran the incremental update and observed no duplicate rows.
• The agent produced a short summary file for human review.

Non-goals
• Do not scrape unapproved sources.
• Do not change modeling code as part of this runbook.
• Do not publish large data files in Git history unless explicitly allowed.
