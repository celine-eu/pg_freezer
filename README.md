# pg_freezer

Archive PostgreSQL timeseries tables to Parquet on S3-compatible storage, verify the upload, then delete the transferred rows.

Built for sensor and IoT workloads where raw data must be pruned from the operational database without data loss. The S3 layout is Hive-style, making archived data directly queryable with Trino, Spark, DuckDB, or Athena.

---

## How it works

For each configured table, pg_freezer computes time-based batch windows (hourly, daily, or monthly), then runs each window through a safe state machine:

```
PENDING → EXPORTING → UPLOADED → VERIFIED → DELETING → DONE
                    ↘ FAILED    ↘ FAILED   ↘ FAILED  ↘ FAILED
```

**No deletion happens until verification passes.** Verification re-reads the Parquet file from S3 and compares row counts. Any mismatch transitions the batch to `FAILED`, logs the error, optionally fires a webhook, and skips the delete. A failed batch requires explicit `--force` to retry.

State is tracked per-batch as JSON manifests in S3 — no external database or coordinator needed. Runs are fully idempotent; re-running picks up where the last run left off.

---

## S3 layout

```
s3://{bucket}/
  {table}/
    year=2024/month=01/day=15/          ← Hive-partitioned Parquet
      data.parquet
    year=2024/month=01/day=15/hour=00/  ← hourly granularity
      data.parquet
  _manifests/
    {table}/
      {batch_id}.json                   ← per-batch state manifest
```

Parquet files can be registered as external tables in Trino using the Hive connector without any additional tooling.

---

## Installation

**Requires Python 3.11+**

```bash
pip install pg-freezer
# or with uv
uv add pg-freezer
```

---

## Quick start

```bash
# 1. Copy and edit the example config
cp config.example.yaml config.yaml

# 2. Preview what would be exported (no writes, no deletes)
pg-freezer run --config config.yaml --dry-run

# 3. Run for real
pg-freezer run --config config.yaml

# 4. Check progress
pg-freezer status --config config.yaml
```

---

## Configuration

pg_freezer is driven by a single YAML file. Environment variables are expanded before parsing — supports both `${VAR}` and `${VAR:=default}` syntax.

```yaml
database:
  dsn: "${PG_DSN:=postgresql://postgres:postgres@localhost:5432/db}"
  pool_size: 5
  statement_timeout: 30s

storage:
  protocol: s3
  endpoint_url: "${S3_ENDPOINT:=http://localhost:9000}"  # omit for AWS
  bucket: pg-freezer
  access_key: "${S3_ACCESS_KEY}"
  secret_key: "${S3_SECRET_KEY}"
  path_style: true   # required for MinIO; false for AWS S3

tables:
  - name: sensor_readings
    schema: public
    timestamp_column: recorded_at
    partition_by: daily       # hourly | daily | monthly
    lag: 15d                  # export records older than 15 days
    extra_filters: null       # optional SQL fragment, e.g. "sensor_type = 'temp'"

  - name: device_events
    schema: public
    timestamp_column: occurred_at
    partition_by: hourly
    lag: 6h

global:
  max_table_concurrency: 3   # tables processed in parallel
  dry_run: false
  check_interval: 1h         # used only with --daemon
  notify:
    webhook_url: null        # POST JSON payload here on batch failure
```

### Table options

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Table name |
| `schema` | string | `public` | PostgreSQL schema |
| `timestamp_column` | string | required | Ordering column (`timestamptz` or `timestamp`) |
| `partition_by` | `hourly` \| `daily` \| `monthly` | `daily` | Partition granularity |
| `lag` | timestring | `15d` | Export records older than this — keeps recent data in Postgres |
| `extra_filters` | string | `null` | Optional SQL `WHERE` fragment appended to all queries |

### Timestrings

`lag` and `statement_timeout` accept short timestrings: `15d`, `6h`, `30m`, `10s`.

### Environment variable expansion

```yaml
dsn: "${PG_DSN}"                          # required — fail if unset
dsn: "${PG_DSN:=postgresql://localhost}"  # optional — fall back to default
```

---

## CLI reference

### `pg-freezer run`

Export, verify, and delete windows for all (or one) configured tables.

```bash
pg-freezer run [OPTIONS]

Options:
  -c, --config PATH   Config file  [default: config.yaml]
  -t, --table TEXT    Process only this table
  --dry-run           Export and verify, skip deletion
  --force             Retry FAILED batches (resets them to PENDING)
  --daemon            Loop indefinitely at check_interval; prefer external cron in production
```

```bash
# All tables
pg-freezer run --config config.yaml

# Single table, dry run
pg-freezer run --config config.yaml --table sensor_readings --dry-run

# Retry failed batches
pg-freezer run --config config.yaml --force
```

### `pg-freezer status`

Aggregated batch counts per table — fast overview.

```bash
pg-freezer status [OPTIONS]

Options:
  -c, --config PATH
  -t, --table TEXT
  --json             Machine-readable output
```

```
TABLE             TOTAL    DONE  FAILED   PENDING/OTHER  LATEST WINDOW
----------------------------------------------------------------------
sensor_readings     168     165       0               3  2024-01-30T00:00:00+00:00
device_events        30      30       0               0  2024-01-30T00:00:00+00:00
```

### `pg-freezer list`

Per-partition detail — partition path, row count, file size, status. Reads from S3 manifests only; no database connection needed.

```bash
pg-freezer list [OPTIONS]

Options:
  -c, --config PATH
  -t, --table TEXT          Filter to one table
  --from DATE               Partitions starting on or after this date
  --to DATE                 Partitions starting before this date
  --status STATUS           Filter: PENDING|EXPORTING|UPLOADED|VERIFIED|DELETING|DONE|FAILED
  --json                    Machine-readable output
```

```bash
# All partitions
pg-freezer list

# Date range for one table
pg-freezer list -t sensor_readings --from 2024-01-01 --to 2024-02-01

# Only failures
pg-freezer list --status FAILED

# Pipe to jq
pg-freezer list --json | jq '.[] | select(.status == "FAILED") | .partition'
```

```
TABLE             PARTITION                       STATUS      ROWS      SIZE  EXPORTED AT
----------------------------------------------------------------------------------------
sensor_readings   year=2024/month=01/day=15       DONE      12,500    45 KB  2024-01-30T12:00
sensor_readings   year=2024/month=01/day=16       DONE       9,832    38 KB  2024-01-30T12:00
device_events     year=2024/month=01              FAILED         -        -  -

3 partition(s)  22,332 rows  83 KB
```

### `pg-freezer fetch`

Download archived Parquet data from S3 and write locally as CSV, JSON, or JSON Lines. Progress and stats go to stderr so stdout can be piped.

```bash
pg-freezer fetch [OPTIONS]

Options:
  -c, --config PATH
  -t, --table TEXT    required
  --from DATE         required — range start (inclusive)
  --to DATE           required — range end (exclusive)
  --format            csv | json | jsonl  [default: csv]
  -o, --output PATH   Output file; omit or use '-' for stdout
```

```bash
# Stream CSV to stdout
pg-freezer fetch -t sensor_readings --from 2024-01-01 --to 2024-01-08

# Write JSON Lines to file
pg-freezer fetch -t sensor_readings --from 2024-01-01 --to 2024-01-02 \
    --format jsonl --output readings.jsonl

# Pipe into DuckDB
pg-freezer fetch -t sensor_readings --from 2024-01-01 --to 2024-02-01 \
    --format jsonl | duckdb -c "SELECT avg(value) FROM read_json_auto('/dev/stdin')"

# Pretty JSON to file
pg-freezer fetch -t device_events --from 2024-01-01 --to 2024-02-01 \
    --format json -o events.json
```

### `pg-freezer replay`

Re-export a specific window range — useful to recover from data issues or re-archive a range that was already deleted.

```bash
pg-freezer replay [OPTIONS]

Options:
  -c, --config PATH
  -t, --table TEXT    required
  --from DATE         required
  --to DATE           required
  --dry-run
  --force             Re-process FAILED batches in the range
```

```bash
pg-freezer replay -t sensor_readings --from 2024-01-01 --to 2024-01-08
pg-freezer replay -t sensor_readings --from 2024-01-01 --to 2024-01-08 --force
```

---

## Data safety

pg_freezer is conservative by design:

- **Deletion is always last.** The sequence is export → upload → re-read from S3 → verify row count → delete. The delete never runs unless the S3 read-back matches.
- **Window deletes, not range deletes.** `DELETE WHERE ts >= window_start AND ts < window_end` — the closed past window is immutable for timeseries, making it safe without PKs.
- **Delete transaction guard.** A `SELECT COUNT(*) = 0` runs inside the same transaction after the DELETE. Any non-zero count rolls back the transaction.
- **Failures are terminal.** A `FAILED` batch blocks all subsequent processing of that batch. Nothing is retried automatically — operator review and `--force` are required.
- **Idempotency.** Manifests use deterministic batch IDs (SHA-256 of table + window). Re-running the same window produces the same ID and resumes from the last known state.
- **No concurrent table processing.** Within a single table, batches are always processed sequentially even when `max_table_concurrency > 1`.

### Deployment constraint

pg_freezer has no distributed locking. Two simultaneous instances against the same table will both discover the same unprocessed windows. In Kubernetes, use `concurrencyPolicy: Forbid` on the CronJob (this is the default in the Helm chart). On bare metal, use `flock` or equivalent.

---

## Kubernetes / Helm

### Install

```bash
helm install pg-freezer ./helm/pg-freezer \
  --set "secrets.pgDsn=postgresql://user:pass@postgres:5432/db" \
  --set "secrets.s3AccessKey=AKIA..." \
  --set "secrets.s3SecretKey=..." \
  --set "schedule=0 * * * *"
```

### Minimal values override

```yaml
# my-values.yaml
schedule: "0 2 * * *"   # daily at 02:00 UTC

image:
  repository: ghcr.io/your-org/pg-freezer
  tag: "0.1.0"

secrets:
  pgDsn: "postgresql://user:pass@postgres:5432/db"
  s3AccessKey: "AKIAIOSFODNN7EXAMPLE"
  s3SecretKey: "wJalrXUtnFEMI/K7MDENG"

config: |
  database:
    dsn: "${PG_DSN}"
    pool_size: 5
    statement_timeout: 60s
  storage:
    protocol: s3
    bucket: my-archive-bucket
    path_style: false
  tables:
    - name: sensor_readings
      schema: public
      timestamp_column: recorded_at
      partition_by: daily
      lag: 15d
  global:
    max_table_concurrency: 3
```

```bash
helm upgrade --install pg-freezer ./helm/pg-freezer -f my-values.yaml
```

### Key chart values

| Value | Default | Description |
|---|---|---|
| `schedule` | `"0 * * * *"` | CronJob schedule (cron syntax) |
| `concurrencyPolicy` | `Forbid` | Prevents overlapping runs — do not change |
| `timeZone` | `UTC` | CronJob timezone |
| `args.table` | `""` | Process only this table; empty = all tables |
| `args.dryRun` | `false` | Run without deleting |
| `args.force` | `false` | Retry FAILED batches |
| `secrets.pgDsn` | `""` | Mounted as `PG_DSN` env var |
| `secrets.s3AccessKey` | `""` | Mounted as `AWS_ACCESS_KEY_ID` |
| `secrets.s3SecretKey` | `""` | Mounted as `AWS_SECRET_ACCESS_KEY` |
| `serviceAccount.annotations` | `{}` | Use for AWS IRSA annotation |
| `resources.requests.memory` | `256Mi` | Increase for large batch windows |

### AWS IRSA (recommended for AWS)

Skip static credentials and use IAM Roles for Service Accounts instead:

```yaml
serviceAccount:
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/pg-freezer

secrets:
  pgDsn: "postgresql://user:pass@rds-host:5432/db"
  # s3AccessKey / s3SecretKey left empty — IRSA provides credentials
```

### Build and push the image

```bash
docker build -t ghcr.io/your-org/pg-freezer:0.1.0 .
docker push ghcr.io/your-org/pg-freezer:0.1.0
```

---

## Trino / external table

Register the archived data as a Hive external table in Trino:

```sql
CREATE TABLE hive.archive.sensor_readings (
    recorded_at  TIMESTAMP WITH TIME ZONE,
    sensor_id    INTEGER,
    sensor_name  VARCHAR,
    value        DOUBLE,
    unit         VARCHAR
)
WITH (
    external_location = 's3a://pg-freezer/sensor_readings/',
    format = 'PARQUET',
    partitioned_by = ARRAY['year', 'month', 'day']  -- match partition_by
);

-- Sync partitions after new data is archived
CALL hive.system.sync_partition_metadata('archive', 'sensor_readings', 'ADD');
```

For `partition_by: hourly` add `'hour'` to `partitioned_by`. For `monthly`, use only `['year', 'month']`.

---

## Local development

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [uv](https://docs.astral.sh/uv/)
- [Task](https://taskfile.dev/installation/) (`brew install go-task` / `snap install task`)

### Start infrastructure

```bash
# Start Postgres 16 + MinIO
task infra:up

# MinIO console: http://localhost:9001  (minioadmin / minioadmin)
```

### Generate test data

```bash
# Backfill ~1M records per table (7 days × 1-min resolution × 100 rows)
# then keep inserting live every second
task dev:generate

# Quick smoke test — ~10k records, exits when done
task dev:generate:quick

# Backfill only, no live loop (useful for CI)
task dev:generate:setup-only
```

The generator creates two tables:

| Table | Columns | Notes |
|---|---|---|
| `sensor_readings` | `recorded_at`, `sensor_id`, `sensor_name`, `value`, `unit` | 8 simulated sensors |
| `device_events` | `occurred_at`, `device_id`, `event_type`, `severity`, `payload` | 20 devices, JSONB payload |

### Export and verify

```bash
# Preview without deleting
task dev:export:dry-run

# Run for real
task dev:export

# Single table
task dev:export:table TABLE=sensor_readings

# Check manifests
task dev:status
```

### Inspect archived data

```bash
# List all S3 partitions
task dev:list

# Filter by table and date range
task dev:list TABLE=sensor_readings FROM=2024-01-01 TO=2024-02-01

# Only failures
task dev:list STATUS=FAILED

# Fetch to stdout as CSV
task dev:fetch TABLE=sensor_readings FROM=2024-01-01 TO=2024-01-08

# Fetch to file as JSON Lines
task dev:fetch:file TABLE=sensor_readings FROM=2024-01-01 TO=2024-01-08 \
    OUTPUT=out.jsonl FMT=jsonl
```

### Replay a range

```bash
task dev:replay TABLE=sensor_readings FROM=2024-01-01 TO=2024-01-08
```

### Run tests

```bash
task test          # unit tests
task test:cov      # with coverage report
```

### All available tasks

```
task --list
```

| Task | Description |
|---|---|
| `infra:up` | Start Postgres + MinIO |
| `infra:down` | Stop services |
| `infra:reset` | Destroy all data and restart |
| `dev:generate` | Backfill 1M records + live inserts |
| `dev:generate:quick` | Small backfill for smoke tests |
| `dev:generate:setup-only` | Backfill only, then exit |
| `dev:export` | Run pg_freezer (export → verify → delete) |
| `dev:export:dry-run` | Preview without deleting |
| `dev:export:table` | Single table (`TABLE=name`) |
| `dev:status` | Manifest status table |
| `dev:list` | Per-partition S3 listing |
| `dev:fetch` | Fetch range to stdout |
| `dev:fetch:file` | Fetch range to file |
| `dev:replay` | Replay a window range |
| `test` | Unit tests |
| `test:cov` | Tests with coverage |

---

## Project structure

```
src/pg_freezer/
  cli.py        — click commands: run, status, list, fetch, replay
  config.py     — pydantic v2 config models, env var expansion, timestring parsing
  engine.py     — async orchestration: window discovery, concurrency, state machine
  exporter.py   — asyncpg server-side cursor, PG→Arrow type mapping, delete tx
  storage.py    — s3fs wrapper (asyncio.to_thread, works with MinIO + AWS + any S3)
  manifest.py   — BatchManifest model, state machine transitions, ManifestStore
  partition.py  — Hive partition paths, window generation, monthly boundary math
  notify.py     — structlog configuration, webhook failure notifications
  errors.py     — exception hierarchy

scripts/
  generate.py   — test data generator (backfill + live inserts)
  config.dev.yaml

helm/pg-freezer/ — Kubernetes Helm chart (CronJob + ConfigMap + Secret + SA)
tests/           — 112 unit tests, no live infra required
```

---

## Notification on failure

Set `notify.webhook_url` in config to receive a JSON POST on any batch failure:

```json
{
  "service": "pg_freezer",
  "event": "batch_failed",
  "table": "sensor_readings",
  "batch_id": "a3f1c2d4e5b6f7a8",
  "window_start": "2024-01-15T00:00:00+00:00",
  "window_end": "2024-01-16T00:00:00+00:00",
  "error": "Verification failed: S3 Parquet has 9800 rows, expected 10000"
}
```

Works with Slack incoming webhooks, PagerDuty, or any HTTP endpoint. Notification failure is non-fatal — it logs a warning and continues.

---

## Known limitations

- **No distributed locking.** Use `concurrencyPolicy: Forbid` in Kubernetes or external mutual exclusion in other environments.
- **Window OOM for very large batches.** The server-side cursor streams rows but accumulates them in memory before writing Parquet. Reduce `lag` or use finer `partition_by` granularity for high-volume tables.
- **Schema drift.** If Postgres table columns change between runs, older and newer Parquet files will have different schemas. Trino handles added columns (nulls for old partitions); renamed columns require a `replay --force` of affected windows.
- **`extra_filters` is raw SQL.** Must only be set by trusted operators — it is injected directly into queries without parameterisation.
- **Timeseries only.** pg_freezer requires a `timestamp`/`timestamptz` column and uses window-based deletes. Tables without a time column are not supported.
