[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![TiDB](https://img.shields.io/badge/TiDB-Serverless-FF6633?style=for-the-badge&logo=mysql&logoColor=white)](https://tidbcloud.com)
[![GitHub Actions](https://img.shields.io/badge/GitHub_Actions-2088FF?style=for-the-badge&logo=github-actions&logoColor=white)](https://github.com/n81l95k7j8/admitad-to-tidb/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# admitad-to-tidb

Idempotent synchronizer for [Admitad](https://www.admitad.com/) product feeds into [TiDB Serverless](https://www.pingcap.com/tidb-serverless/) (MySQL-compatible).

Streams a CSV feed over HTTP, diffs it against the current `products` table, performs batched `UPSERT`s only for changed rows, and deletes stale entries (rows no longer present in the feed). Every operation is protected by a monthly write budget and daily row limit so the job stays within the TiDB Serverless free/paid quota.

---

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Requirements](#requirements)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database schema](#database-schema)
- [Running](#running)
- [Sync algorithm](#sync-algorithm)
- [Budgets and limits](#budgets-and-limits)
- [Feed sanity check](#feed-sanity-check)
- [Concurrent runs and locking](#concurrent-runs-and-locking)
- [Sync statuses](#sync-statuses)
- [Logging and observability](#logging-and-observability)
- [Operational scenarios](#operational-scenarios)
- [Smoke tests](#smoke-tests)
- [CI / GitHub Actions](#ci--github-actions)
- [Exit codes](#exit-codes)
- [License](#license)

---

## Features

- **Streaming import** — the feed is read in chunks (`requests.iter_lines`); no full in-memory buffering.
- **HTTP caching** — `ETag` / `If-Modified-Since` are persisted between runs; a `304 Not Modified` response skips the import entirely.
- **Diff-upsert** — rows are written only when `price` or `url` actually changes. Unchanged rows do not consume the write budget.
- **Idempotency** — re-running on the same feed produces no duplicates and wastes no budget.
- **Stale cleanup** — products that disappeared from the feed are deleted in batches.
- **Feed sanity check** — refuses to delete when the feed has shrunk suspiciously (protects against empty/truncated exports).
- **Monthly write budget** (`MONTHLY_WRITE_LIMIT`) — tracked atomically under `SELECT … FOR UPDATE`.
- **Daily row budget** (`MAX_TOTAL_ROWS_PER_DAY`) with a separate quota for critical feeds.
- **Critical priority mode** — after a failed run the next start gets the reserved critical row budget.
- **TTL-based lock** — two cron processes targeting the same `feed_source` cannot collide.
- **Early-exit scan** — once the monthly budget is exhausted the scan switches to ids-only mode (no SELECT/INSERT) to avoid burning time and the daily row budget.
- **SSL by default** — uses the bundled system CA or a local `isrgrootx1.pem` (TiDB Cloud).

---

## Architecture

```
┌────────────────────┐    HTTP/CSV (stream)     ┌──────────────────────┐
│  Admitad export    │ ───────────────────────▶ │   v6_direct_db.py    │
│  (export_adv_      │   ETag / If-Mod-Since    │  TiDBImporter        │
│   products)        │ ◀─── 304 / 200 ───────── │  - acquire lock      │
└────────────────────┘                          │  - scan + batch      │
                                                │  - diff vs DB        │
                                                │  - upsert changed    │
                                                │  - sanity check      │
                                                │  - delete stale      │
                                                │  - release lock      │
                                                └──────────┬───────────┘
                                                           │ MySQL/SSL (4000)
                                                           ▼
                                                ┌──────────────────────┐
                                                │       TiDB           │
                                                │  products            │
                                                │  import_usage_budget │
                                                │  import_feed_state   │
                                                │  import_daily_row_…  │
                                                └──────────────────────┘
```

One run = one feed = one `run_id` (UNIX timestamp). Multiple feeds are processed as parallel processes (cron / Kubernetes CronJob / Airflow / GitHub Actions matrix). Parallel runs against the same `feed_source` are serialized through a lock stored in `import_feed_state`.

---

## Requirements

- **Python** ≥ 3.9
- **TiDB** ≥ 6.x (or TiDB Serverless), reachable on the MySQL protocol at port `4000`
- Network access to `export.admitad.com` and the TiDB host
- A pre-created `products` table (schema below). Auxiliary tables are created automatically.

---

## Installation

```bash
git clone https://github.com/n81l95k7j8/admitad-to-tidb
cd admitad-to-tidb

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

TiDB Serverless uses TLS issued by Let's Encrypt. On modern OS images (Ubuntu 20.04+, Debian 11+, macOS, recent Windows) the ISRG Root X1 certificate is already in the system CA store and the script works out of the box — `isrgrootx1.pem` is optional.

If you run on an older image where the chain doesn't validate, drop the CA bundle next to the script as `isrgrootx1.pem`. Get it from one of:

- **TiDB Cloud console** → cluster → *Connect* → *CA certificate* (recommended, always matches what your cluster serves)
- Your distribution's `ca-certificates` package (the file is typically `/etc/ssl/certs/ISRG_Root_X1.pem`)

The script auto-detects the file: present → used and logged; absent → system CA store + warning.

---

## Configuration

All settings are passed via environment variables.

### Required

| Variable               | Description                                            |
| ---------------------- | ------------------------------------------------------ |
| `TIDB_HOST`            | TiDB host (e.g. `gateway01.eu-central-1.prod.aws.tidbcloud.com`) |
| `TIDB_USER`            | DB user                                                |
| `TIDB_PASSWORD`        | DB password                                            |
| `TIDB_DB_NAME`         | Database name                                          |
| `ADMITAD_USER_ID`      | Admitad webmaster `id`                                 |
| `ADMITAD_USER_TOKEN`   | Export token (`user=` in the URL)                      |
| `ADMITAD_FID_CODE`     | Export `code`                                          |
| `FID_ID`               | Specific feed ID (also used as `source_id` in tables)  |

### Network timeouts

| Variable               | Default | Description                       |
| ---------------------- | ------: | --------------------------------- |
| `DB_CONNECT_TIMEOUT`   | `60`    | TCP connect to TiDB, seconds       |
| `DB_READ_TIMEOUT`      | `600`   | MySQL protocol read timeout, sec  |
| `DB_WRITE_TIMEOUT`     | `600`   | MySQL protocol write timeout, sec |
| `HTTP_CONNECT_TIMEOUT` | `40`    | HTTP connect to Admitad, sec      |
| `HTTP_READ_TIMEOUT`    | `900`   | HTTP read, sec                    |

### Budgets and batches

| Variable                    | Default       | Description                                                                                                |
| --------------------------- | ------------: | ---------------------------------------------------------------------------------------------------------- |
| `BATCH_SIZE`                | `5000`        | Upsert batch size                                                                                          |
| `DELETE_BATCH_SIZE`         | `10000`       | Stale delete batch size                                                                                    |
| `FEED_SCAN_BATCH_SIZE`      | `20000`       | Page size when scanning the DB for stale ids                                                               |
| `MONTHLY_WRITE_LIMIT`       | `50_000_000`  | Cap on (upsert + delete) operations per UTC calendar month                                                 |
| `MAX_FEED_ROWS`             | `15_000_000`  | Hard stop on runaway feeds                                                                                 |
| `MAX_TOTAL_ROWS_PER_DAY`    | `5_000_000`   | Feed rows allowed to be processed per UTC day                                                              |
| `MAX_CRITICAL_ROWS_PER_DAY` | `500_000`     | Extra daily row reserve available only in critical priority mode (after a failed run)                      |
| `DAILY_ROW_RESERVE_CHUNK`   | `= BATCH_SIZE`| Reservation chunk size for the daily budget                                                                |
| `STRICT_STALE_DELETE`       | `1`           | `1` — delete stale rows even after the write budget is exhausted. `0` — skip remaining deletes and do NOT persist ETag. |

### Sanity and concurrency

| Variable                 | Default | Description                                                                                                |
| ------------------------ | ------: | ---------------------------------------------------------------------------------------------------------- |
| `MIN_FEED_ROWS_ABSOLUTE` | `1000`  | If the feed has fewer rows than this, the run aborts WITHOUT stale cleanup (guard against empty exports).  |
| `MIN_FEED_SIZE_RATIO`    | `0.5`   | If the feed has shrunk below this fraction of the previous successful run, the run aborts WITHOUT stale cleanup. |
| `FORCE_SYNC`             | `0`     | `1` — bypasses both checks above. Use only for confirmed legitimate catalog shrinkage.                     |
| `LOCK_TTL_SECONDS`       | `3600`  | TTL for the `feed_source` lock. After expiry another process may take over (recovery from `kill -9`/OOM).  |

---

## Database schema

### `products` table (created manually)

```sql
CREATE TABLE products (
  id            BIGINT       NOT NULL,
  feed_source   VARCHAR(50)  NOT NULL,
  price         DECIMAL(18,4) NOT NULL,
  url           TEXT         NULL,
  run_id        BIGINT       NOT NULL,
  PRIMARY KEY (feed_source, id)
);
```

> The composite PK `(feed_source, id)` lets one table serve multiple feeds without `id` collisions. `run_id` is a diagnostic column; no index is needed (the script never queries by it).

### Auxiliary tables (created automatically)

#### `import_usage_budget`

| Column        | Type           | Purpose                                  |
| ------------- | -------------- | ---------------------------------------- |
| `period_ym`   | `CHAR(7)` PK   | `YYYY-MM` (UTC)                          |
| `used_writes` | `BIGINT`       | Write counter for the current month       |
| `updated_at`  | `TIMESTAMP`    | Auto                                     |

#### `import_feed_state`

| Column                 | Type             | Purpose                                                              |
| ---------------------- | ---------------- | -------------------------------------------------------------------- |
| `feed_source`          | `VARCHAR(50)` PK | Feed ID                                                              |
| `etag`                 | `VARCHAR(512)`   | Last `ETag` from Admitad                                             |
| `last_modified`        | `VARCHAR(255)`   | Last `Last-Modified`                                                 |
| `last_status`          | `VARCHAR(16)`    | `success` / `not_modified` / `budget_exhausted` / `failed`           |
| `consecutive_failures` | `INT`            | Consecutive failed runs (reset on any OK status)                     |
| `last_total_seen`      | `BIGINT`         | Feed size at the last successful run (used by the sanity check)      |
| `lock_token`           | `VARCHAR(64)`    | Token of the current lock owner                                      |
| `lock_expires_at`      | `TIMESTAMP`      | Lock validity deadline                                               |
| `last_run_utc`         | `TIMESTAMP`      | Auto                                                                 |

#### `import_daily_row_budget`

| Column               | Type           | Purpose                                                     |
| -------------------- | -------------- | ----------------------------------------------------------- |
| `day_utc`            | `CHAR(10)` PK  | `YYYY-MM-DD` (UTC)                                          |
| `used_rows`          | `BIGINT`       | Regular feed rows processed today                           |
| `used_critical_rows` | `BIGINT`       | Rows beyond the normal limit consumed in critical mode      |
| `updated_at`         | `TIMESTAMP`    | Auto                                                        |

---

## Running

### One-shot

```bash
export TIDB_HOST=...
export TIDB_USER=...
export TIDB_PASSWORD=...
export TIDB_DB_NAME=admitad
export ADMITAD_USER_ID=...
export ADMITAD_USER_TOKEN=...
export ADMITAD_FID_CODE=...
export FID_ID=123456

python v6_direct_db.py
```

### Cron (hourly)

```cron
17 * * * * cd /opt/admitad-to-tidb && /opt/admitad-to-tidb/.venv/bin/python v6_direct_db.py >> /var/log/admitad-sync.log 2>&1
```

### Multiple feeds

Run the process with different `FID_ID` values (and `ADMITAD_FID_CODE` if needed) — state is isolated per `feed_source` in every auxiliary table, and the lock is taken on `feed_source`.

---

## Sync algorithm

1. **Setup** — `_connect` → create/migrate auxiliary tables.
2. **Lock** — `_acquire_lock` atomically claims `lock_token` in `import_feed_state`. If the lock is held by a fresh process, the run exits quietly (return, exit 0).
3. **Priority detection** — if the previous `last_status` is not in `{success, not_modified, budget_exhausted}`, the run is marked **critical** and gets access to the reserved daily row budget.
4. **Conditional HTTP GET** — `If-None-Match` / `If-Modified-Since`. A `304` response → `mark_feed_status('not_modified')` and early return without spending budget.
5. **Streaming CSV read** (`;` delimiter, first line is a header). Parsed fields: `id` (col 0), `url` (col 2), `price` (col 8).
6. **Daily budget reservation** in chunks of `DAILY_ROW_RESERVE_CHUNK`.
7. **batch_map accumulation** up to `BATCH_SIZE`, then:
   - `SELECT id, price, url FROM products WHERE feed_source=? AND id IN (...)`
   - compare with the feed, keep only changed rows,
   - reserve monthly budget for actual change count,
   - `INSERT ... ON DUPLICATE KEY UPDATE`.
8. **On monthly budget exhaustion** the scan switches to ids-only mode: no more SELECT/INSERT/daily reservation, but the feed is read to completion (required for correct stale cleanup).
9. **Sanity check** — `_check_feed_sanity(total_seen)`:
   - `total_seen < MIN_FEED_ROWS_ABSOLUTE` → error;
   - `total_seen < last_total_seen * MIN_FEED_SIZE_RATIO` → error.

   On error, stale cleanup is skipped, status becomes `failed`, ETag is not persisted. `FORCE_SYNC=1` downgrades the error to a warning.
10. **Stale id collection** — DB scanned page by page (`WHERE feed_source=? AND id > last_id ORDER BY id LIMIT ...`).
11. **Stale deletion** — batches of `DELETE_BATCH_SIZE`. If the monthly budget is exhausted:
    - `STRICT_STALE_DELETE=1` — deletion proceeds without reservation, ETag is persisted, status is `success` or `budget_exhausted`;
    - `STRICT_STALE_DELETE=0` — deletion halts without raising, ETag is NOT persisted, status is `budget_exhausted` (the next run will refetch the feed and retry cleanup).
12. **State commit** — `etag`, `last_modified`, `last_total_seen` are persisted ONLY when cleanup completed fully.
13. **Release lock** in `finally`.

---

## Budgets and limits

### Monthly write budget

- `import_usage_budget.used_writes` is incremented atomically under `SELECT … FOR UPDATE` before every write.
- Exceeding the limit is **not** an error: upserts silently skip changes and the scan switches to ids-only mode.
- Reset is implicit: a new `period_ym = YYYY-MM` row is created on the first run of the month.

### Daily row budget

- `MAX_TOTAL_ROWS_PER_DAY` — total feed-row cap per UTC day.
- `MAX_CRITICAL_ROWS_PER_DAY` — extra reserve available only in **critical** mode.
- When both are exhausted the feed scan aborts with `RuntimeError`.

### Hard stop

`MAX_FEED_ROWS` — protection against a runaway feed: if more than this many rows are processed in one session, a `RuntimeError` is raised.

---

## Feed sanity check

Protection against the "Admitad returned an empty/truncated CSV → entire catalog wiped" scenario.

| Check                                                  | Behaviour                                                             |
| ------------------------------------------------------ | --------------------------------------------------------------------- |
| `total_seen < MIN_FEED_ROWS_ABSOLUTE`                  | `RuntimeError`, stale cleanup skipped, status = `failed`              |
| `total_seen < last_total_seen × MIN_FEED_SIZE_RATIO`   | same                                                                  |
| `FORCE_SYNC=1`                                         | Both checks become warnings, execution continues                      |

`last_total_seen` is updated ONLY on a fully successful run — it is the "trusted" baseline.

For a legitimate catalog shrinkage (end of sale, vendor leaving) run once with `FORCE_SYNC=1` — the baseline will be reset after success.

---

## Concurrent runs and locking

`_acquire_lock` atomically claims a row in `import_feed_state` via a single `UPDATE … WHERE lock_token IS NULL OR lock_expires_at < UTC_TIMESTAMP()`:

- Only one process can obtain `rowcount=1`.
- If the lock is held by a fresh process, the current run exits with a warning and exit 0 (cron-friendly).
- If `lock_expires_at` is in the past (the previous process was killed by `kill -9` or OOM), the lock is taken over by the new process.
- In `finally` the lock is released via `UPDATE … WHERE lock_token = my_token` (will not clear someone else's claim).

Tune `LOCK_TTL_SECONDS` to the maximum realistic sync duration (default 1 hour — usually generous).

---

## Sync statuses

`import_feed_state.last_status` values:

| Status             | Meaning                                                                                          | Resets `consecutive_failures`? | Triggers critical? |
| ------------------ | ------------------------------------------------------------------------------------------------ | :-: | :-: |
| `success`          | Fully successful sync                                                                            | ✅  | ❌  |
| `not_modified`     | HTTP 304, nothing to do                                                                          | ✅  | ❌  |
| `budget_exhausted` | Monthly write budget exhausted; data partially applied, stale cleanup may be incomplete          | ✅  | ❌  |
| `failed`           | Any error: network, DB, sanity check, hard stop                                                  | ❌  | ✅  |

`is_critical = True` when `last_status` exists and is not in `{success, not_modified, budget_exhausted}` — i.e. only after `failed`.

---

## Logging and observability

- Logger `TiDB_Importer_Budgeted`, level `INFO`, format `timestamp - level - message`.
- Progress is logged every 250 000 rows: `scanned`, `upserted`, `unique_ids`, `invalid`.
- Final line: `DONE [status]: upserted=…, deleted=…, budget_used=N/LIMIT, budget_blocked_*=…`.
- Suggested alerting metrics (scrape from logs):
  - `last_status='failed'` or `consecutive_failures > 0` in `import_feed_state` → stuck feed.
  - `last_status='budget_exhausted'` for several runs → monthly limit reached.
  - `Another process holds the feed lock` → overlapping crons (increase interval or speed up sync).
  - `Feed sanity check FAILED` → suspicious feed, needs manual review.
  - `deleted == 0` when catalog rotation is expected → the source may be returning a partial feed.

---

## Operational scenarios

### "Feed didn't change"

`HTTP 304` → log: `Feed not modified. Skipping import.`, status `not_modified`. ETag was saved earlier; no budget spent.

### "Import crashed mid-run"

`ETag` / `Last-Modified` / `last_total_seen` are not persisted until full success. The next run re-reads the feed from scratch; `last_status='failed'` raises the priority to **critical**.

### "Force a manual resync"

```sql
UPDATE import_feed_state
SET etag = NULL, last_modified = NULL
WHERE feed_source = '<FID_ID>';
```

The next run performs a full pass without conditional headers.

### "Sanity check tripped on a real catalog shrinkage"

```bash
FORCE_SYNC=1 python v6_direct_db.py
```

After success `last_total_seen` is updated and subsequent runs work normally without `FORCE_SYNC`.

### "Monthly budget exhausted but trash needs cleaning"

Keep `STRICT_STALE_DELETE=1` (default) — deletions continue without reservation. Status will be `success` or `budget_exhausted` (if cleanup couldn't complete).

### "Raise/lower the monthly limit or reset the counter"

```sql
UPDATE import_usage_budget SET used_writes = 0 WHERE period_ym = '2026-06';
```

Or override `MONTHLY_WRITE_LIMIT` in the environment.

### "Manually clear a stuck lock"

```sql
UPDATE import_feed_state
SET lock_token = NULL, lock_expires_at = NULL
WHERE feed_source = '<FID_ID>';
```

Rarely needed — locks auto-expire on TTL.

---

## Smoke tests

The repo root ships `smoke_test.py` — 16 tests with no dependency on a real DB or network.

```bash
python smoke_test.py
```

Uses `unittest.mock` to stub `mysql.connector` and `requests`. Covers:

- sanity check (absolute threshold, ratio threshold, passing case);
- lock (acquire, release, contention, TTL takeover);
- scan (HTTP 304, CSV parsing + diff-upsert, ids-only switch on budget exhausted);
- stale delete (strict overrides budget, non-strict halts);
- end-to-end `run_feed` (full success, 304, sanity failure preserves data, `FORCE_SYNC=1`, lock held → skip).

No extra dependencies beyond `requirements.txt`. Runs in ~0.1 s.

---

## CI / GitHub Actions

`.github/workflows/main.yml` defines two jobs:

| Job          | Triggers                                    | Purpose                                                                |
| ------------ | ------------------------------------------- | ---------------------------------------------------------------------- |
| `smoke-test` | `push` / `pull_request` / `schedule` / `workflow_dispatch` | Runs `smoke_test.py` (gate for the sync job)             |
| `run-sync`   | `schedule` (`0 2 * * *` UTC) / `workflow_dispatch`        | Matrix sync of 6 feeds, max 2 in parallel; needs `smoke-test` green |

`run-sync` writes per-feed logs via `tee tidb_import_${FID_ID}.log` (`set -o pipefail` preserves the exit code), uploads them as artifacts for 14 days, and posts a Slack message on failure (`SLACK_WEBHOOK` secret required).

Concurrency group `tidb-import` prevents overlapping workflow runs at the GitHub Actions level; the DB lock prevents overlapping process-level runs.

---

## Exit codes

| Code | Condition                                                                  |
| ---: | -------------------------------------------------------------------------- |
| `0`  | Successful sync, `not_modified`, `budget_exhausted`, or lock already held  |
| `1`  | At least one required environment variable is missing                      |
| `1`  | Any unhandled exception (sanity failure, DB, network, hard stop)           |

---

## License

[MIT](LICENSE) © 2026 Nikolas Rhys.
