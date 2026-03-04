# CA Image Import

Bulk-imports images into CollectiveAccess from a CSV, linking server-side image
files to catalog objects as `ca_object_representations`.

## Usage
    node import.js --csv <file.csv> [--start N] [--limit N] [--concurrency N] \
      [--max-concurrency N] [--domain host] [--resume] [-v]

## CSV format
Columns: `title, path, image_id, item_id`
- `path`     — server-local file path already on the CA host (uploaded via SFTP)
- `image_id` — becomes the representation idno; used for duplicate detection
- `item_id`  — CA object idno to link the representation to
- `title`    — preferred label (optional)

## Environment (.env)
    CA_BASE_URL=https://<host>/service.php/json/
    CA_USER=administrator
    CA_PASSWORD=...
    CA_LOCALE=he_IL
    REL_TYPE_ID=135   # representation→object rel type for YBZ museum (23)

## Output files (written alongside the CSV)
- `import.log.csv`       — every row: status, reason, rep_id
- `import.progress.json` — completed item_ids for --resume
- `import.failed.csv`    — non-overload failures; re-feed as --csv

## Key behaviors
- Skips objects already having ≥1 representation in CA
- Skips duplicate item_ids within the same CSV (guards concurrent duplicates too)
- Adaptive concurrency (AIMD): starts at --concurrency, ramps up after 5 successes,
  backs off (÷2) on HTTP 429/5xx or network errors
- Overload errors are auto-retried with 5s delay — never written to failed.csv
- Post-import verification re-fetches every imported item to confirm exactly 1 rep

## GDrive alternative (not yet implemented)
CA's `media` field likely accepts HTTP URLs — test before implementing.
- If yes: replace `path` column with direct download URLs; add URL normalization
- Private files: Google service account + short-lived signed URLs (googleapis npm)
- If CA requires server-local paths: mount GDrive on CA server with rclone
