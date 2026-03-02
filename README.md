# ca-image-import

Attaches images to existing CollectiveAccess objects from a CSV manifest. Images must already be on the CA server (uploaded via SFTP separately); this script links them as representations via the CA JSON API.

## Prerequisites

- Node.js 18+
- `npm install`
- A `.env` file (copy `.env.example` and fill in credentials)

## Setup

```bash
cp .env.example .env
# edit .env with your CA credentials
npm install
```

### `.env`

```
CA_BASE_URL=https://ca.israelalbum.org.il/service.php/json/
CA_USER=claude
CA_PASSWORD=your_password_here
CA_LOCALE=he_IL
REL_TYPE_ID=135
```

## Usage

```bash
node import.js --csv <file.csv> [options]
```

### Options

| Flag | Description |
|------|-------------|
| `--csv PATH` | CSV file to import **(required)** |
| `--start N` | Skip the first N rows |
| `--limit N` | Process at most N rows |
| `--concurrency N` | Starting concurrency (default: 2) |
| `--max-concurrency N` | Concurrency ceiling for adaptive control (default: 5) |
| `--domain HOST` | CA hostname — overrides `CA_BASE_URL` in `.env` |
| `--resume` | Skip rows already in `import.progress.json` |
| `-v` / `--verbose` | Log every API request and response |

### Examples

```bash
# Smoke test — first 3 rows
node import.js --csv ../image_import_v2/feb.csv --limit 3

# Start from row 50
node import.js --csv ../image_import_v2/feb.csv --start 50

# Resume an interrupted run
node import.js --csv ../image_import_v2/feb.csv --resume

# Retry only failed rows
node import.js --csv import.failed.csv

# Full run
node import.js --csv ../image_import_v2/feb.csv
```

## CSV format

```
title,path,image_id,item_id
YBZ.0807.339.jpg,/var/www/html/import/photos_to_connect_YBZ/YBZ.0807.339.jpg,YBZ.0807.339,YBZ.0807.339
```

| Column | Description |
|--------|-------------|
| `title` | Preferred label for the representation |
| `path` | Server-local file path on the CA host |
| `image_id` | Representation idno |
| `item_id` | CA object idno to attach the image to |

The `path` column must be a path on the **CA server's filesystem** — the image must already be there before running this script. The script does not upload files.

## Skip logic

A row is skipped (not an error) if:
- The `item_id` does not exist in CA → logged as `skipped / object not found`
- The object already has one or more representations → logged as `skipped / already has N image(s)`

## Output files

All output files are written next to the input CSV.

| File | Contents |
|------|----------|
| `import.log.csv` | Every row: `item_id, image_id, status, reason, rep_id` |
| `import.failed.csv` | Failed rows in the same format as the input — feed back with `--csv import.failed.csv` |
| `import.progress.json` | Checkpoint for `--resume` (auto-deleted on clean finish) |

## Progress bar

When running in an interactive terminal the script shows a live progress bar:

```
[████████████░░░░░░░░░░░░░░░░░░░░] 400/3091 (13%) | 1.3s/row | ETA 00:44:12 | conc=3↑
```

- ETA is `HH:MM:SS`
- `s/row` and ETA are based only on rows where actual work was attempted — skipped rows (object not found / already has media) are excluded so they don't distort the timing
- `conc=N` shows the current concurrency level
- `↑` / `↓` indicates the last adjustment direction

Suppressed automatically when output is piped.

## Adaptive concurrency

The queue starts at `--concurrency` (default 2) and adjusts automatically between 1 and `--max-concurrency` (default 5) using the AIMD algorithm:

| Signal | Action |
|--------|--------|
| 5 consecutive successes with healthy response time | +1 concurrency |
| Rolling avg response time > 1.5× baseline | Suppress ramp-up |
| HTTP 429 / 5xx / network error | Halve concurrency, re-queue row with 5s delay |
| Single response time > 2× baseline | Halve concurrency (row still counts as success) |

Rows that hit overload errors are **re-queued automatically** with a 5-second delay and are never written to `import.failed.csv`. Only permanent errors (bad CA data, wrong path, etc.) go to the failed file.

## Museum access

The CA JSON API filters search results to the logged-in user's assigned museum. If the `CA_USER` account is assigned to the wrong museum the script will detect it at startup and exit with a clear message:

```
ERROR: User 'claude' can see 0 objects in CollectiveAccess.
       This almost always means the user is assigned to the wrong museum.
       Fix: log in to CA and change user 'claude' to museum YBZ (23)
```

**Known setup:** `claude` user on `ca.israelalbum.org.il` → museum 23 (YBZ).

## Token expiry

Auth tokens expire after ~2 hours. The script detects 401 responses, re-authenticates automatically, and retries the failed request.

## Concurrency guidance

| Setting | When to use |
|---------|-------------|
| `--concurrency 2 --max-concurrency 3` | Production during business hours (conservative) |
| `--concurrency 2 --max-concurrency 5` | Production off-hours (default) |
| `--max-concurrency 8` | Staging only |

The adaptive queue will find the right level within the configured range. Use `--max-concurrency` to cap it for production safety.
