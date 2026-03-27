"""CLI entry points for pg_freezer."""

from __future__ import annotations

import asyncio
import io
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal

import click
import pyarrow as pa
import pyarrow.csv as pa_csv
import pyarrow.parquet as pq

from pg_freezer.config import load_config
from pg_freezer.engine import FreezerEngine
from pg_freezer.errors import ConfigError
from pg_freezer.manifest import ManifestStore
from pg_freezer.notify import configure_logging
from pg_freezer.partition import generate_windows, partition_path
from pg_freezer.storage import StorageClient

_DT_FORMATS = ["%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ"]


def _dt_utc(dt: datetime) -> datetime:
    return dt.replace(tzinfo=timezone.utc)


def _json_default(o: object) -> object:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


@click.group()
@click.version_option()
def main() -> None:
    """pg_freezer — archive PostgreSQL timeseries tables to Parquet on S3."""
    configure_logging()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@main.command()
@click.option("--config", "-c", default="config.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--table", "-t", default=None, help="Process only this table (by name).")
@click.option("--dry-run", is_flag=True, help="Export and verify but skip deletion.")
@click.option("--force", is_flag=True,
              help="Re-process batches in FAILED state (resets them to PENDING).")
@click.option("--daemon", is_flag=True,
              help="Run in loop mode, repeating at check_interval. "
                   "For production, prefer an external CronJob with concurrencyPolicy: Forbid.")
def run(config: str, table: str | None, dry_run: bool, force: bool, daemon: bool) -> None:
    """Export, verify, and delete configured table windows."""
    try:
        cfg = load_config(config)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    tables = [t.name for t in cfg.tables] if not table else [table]
    mode = "[dry-run] " if dry_run or cfg.global_config.dry_run else ""
    click.echo(f"{mode}pg-freezer starting — tables: {', '.join(tables)}")

    engine = FreezerEngine(cfg)

    async def _run_once() -> None:
        await engine.run(table_filter=table, dry_run=dry_run, force=force)

    async def _daemon_loop() -> None:
        interval = cfg.global_config.check_interval.total_seconds()
        click.echo(f"Daemon mode: running every {interval:.0f}s. Ctrl-C to stop.")
        while True:
            e = FreezerEngine(cfg)
            await e.run(table_filter=table, dry_run=dry_run, force=force)
            await asyncio.sleep(interval)

    try:
        if daemon:
            asyncio.run(_daemon_loop())
        else:
            asyncio.run(_run_once())
    except KeyboardInterrupt:
        click.echo("Interrupted.")
        sys.exit(0)
    except Exception as e:
        click.echo(f"Fatal error: {e}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@main.command()
@click.option("--config", "-c", default="config.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--table", "-t", default=None, help="Show only this table.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def status(config: str, table: str | None, output_json: bool) -> None:
    """Show manifest status summary for configured tables."""
    try:
        cfg = load_config(config)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    engine = FreezerEngine(cfg)
    results = asyncio.run(engine.status(table_filter=table))

    if output_json:
        click.echo(json.dumps(results, indent=2))
        return

    if not results:
        click.echo("No tables found.")
        return

    col_w = max(len(r["table"]) for r in results)
    header = (
        f"{'TABLE':<{col_w}}  {'TOTAL':>7}  {'DONE':>6}  "
        f"{'FAILED':>7}  {'PENDING/OTHER':>14}  LATEST WINDOW"
    )
    click.echo(header)
    click.echo("-" * len(header))
    for r in results:
        counts = r["status_counts"]
        done = counts.get("DONE", 0)
        failed = counts.get("FAILED", 0)
        other = r["total_batches"] - done - failed
        latest = r["latest_window"] or "-"
        click.echo(
            f"{r['table']:<{col_w}}  {r['total_batches']:>7}  {done:>6}  "
            f"{failed:>7}  {other:>14}  {latest}"
        )


# ---------------------------------------------------------------------------
# replay
# ---------------------------------------------------------------------------

@main.command()
@click.option("--config", "-c", default="config.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--table", "-t", required=True, help="Table name to replay.")
@click.option("--from", "from_dt", required=True,
              type=click.DateTime(formats=_DT_FORMATS),
              help="Start of replay window (inclusive). Example: 2024-01-01")
@click.option("--to", "to_dt", required=True,
              type=click.DateTime(formats=_DT_FORMATS),
              help="End of replay window (exclusive). Example: 2024-02-01")
@click.option("--dry-run", is_flag=True, help="Export and verify but skip deletion.")
@click.option("--force", is_flag=True, help="Re-process FAILED batches in the replay range.")
def replay(config: str, table: str, from_dt: datetime, to_dt: datetime,
           dry_run: bool, force: bool) -> None:
    """Re-process a specific window range for one table."""
    from_dt, to_dt = _dt_utc(from_dt), _dt_utc(to_dt)
    try:
        cfg = load_config(config)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    engine = FreezerEngine(cfg)
    asyncio.run(engine.replay(
        table_name=table, from_dt=from_dt, to_dt=to_dt,
        dry_run=dry_run, force=force,
    ))


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------

@main.command()
@click.option("--config", "-c", default="config.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--table", "-t", required=True, help="Table name to fetch.")
@click.option("--from", "from_dt", required=True,
              type=click.DateTime(formats=_DT_FORMATS),
              help="Start of range (inclusive). Example: 2024-01-01")
@click.option("--to", "to_dt", required=True,
              type=click.DateTime(formats=_DT_FORMATS),
              help="End of range (exclusive). Example: 2024-02-01")
@click.option("--format", "fmt",
              type=click.Choice(["csv", "json", "jsonl"], case_sensitive=False),
              default="csv", show_default=True,
              help="Output format. jsonl = one JSON object per line.")
@click.option("--output", "-o", default=None, metavar="PATH",
              help="Output file path. Omit or use '-' for stdout.")
def fetch(config: str, table: str, from_dt: datetime, to_dt: datetime,
          fmt: str, output: str | None) -> None:
    """Retrieve archived data from S3 by time range and write as CSV or JSON.

    Reads the Parquet files stored by pg-freezer run and converts them to
    a local file or stdout. Partition boundaries align with the table's
    partition_by setting in config (hourly/daily/monthly).

    Examples:

    \b
        # Last week to stdout as CSV
        pg-freezer fetch -t sensor_readings --from 2024-01-01 --to 2024-01-08

        # Specific range to a JSON Lines file
        pg-freezer fetch -t sensor_readings --from 2024-01-01 --to 2024-01-02 \\
            --format jsonl --output readings.jsonl

        # Pretty JSON to file
        pg-freezer fetch -t device_events --from 2024-01-01 --to 2024-02-01 \\
            --format json -o events.json
    """
    from_dt, to_dt = _dt_utc(from_dt), _dt_utc(to_dt)

    try:
        cfg = load_config(config)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    table_cfg = next((t for t in cfg.tables if t.name == table), None)
    if not table_cfg:
        click.echo(
            f"Table {table!r} not found in config. "
            f"Available: {[t.name for t in cfg.tables]}",
            err=True,
        )
        sys.exit(1)

    try:
        asyncio.run(_do_fetch(cfg, table_cfg, from_dt, to_dt, fmt, output))
    except KeyboardInterrupt:
        click.echo("Interrupted.", err=True)
        sys.exit(0)
    except Exception as e:
        click.echo(f"Fatal error: {e}", err=True)
        sys.exit(1)


async def _do_fetch(cfg, table_cfg, from_dt, to_dt, fmt, output_path) -> None:
    windows = generate_windows(from_dt, to_dt, table_cfg.partition_by)
    if not windows:
        click.echo("No windows in the specified range.", err=True)
        return

    click.echo(
        f"Fetching {len(windows)} {table_cfg.partition_by} window(s) "
        f"[{from_dt.date()} → {to_dt.date()}] from S3...",
        err=True,
    )

    storage = StorageClient(cfg.storage)
    arrow_tables: list[pa.Table] = []
    missing: list[str] = []

    for ws, _ in windows:
        part = partition_path(ws, table_cfg.partition_by)
        key = f"{table_cfg.name}/{part}/data.parquet"
        try:
            data = await storage.download_bytes(key)
            t = pq.read_table(pa.py_buffer(data))
            arrow_tables.append(t)
            click.echo(f"  {part}  {t.num_rows:>10,} rows", err=True)
        except FileNotFoundError:
            missing.append(part)

    if missing:
        click.echo(
            f"  ({len(missing)} partition(s) not in S3 — "
            "not archived yet or outside lag window)",
            err=True,
        )

    if not arrow_tables:
        click.echo("No data found in S3 for the specified range.", err=True)
        return

    combined = pa.concat_tables(arrow_tables, promote_options="default")
    click.echo(f"Total: {combined.num_rows:,} rows, {len(combined.schema)} columns", err=True)

    _write_output(combined, fmt, output_path)

    if output_path and output_path != "-":
        import os
        size = os.path.getsize(output_path)
        click.echo(
            f"Written to {output_path} "
            f"({size / 1024:.1f} KB, {fmt.upper()})",
            err=True,
        )


def _write_output(table: pa.Table, fmt: str, output_path: str | None) -> None:
    path: str | None = None if (not output_path or output_path == "-") else output_path

    if fmt == "csv":
        buf = io.BytesIO()
        pa_csv.write_csv(table, buf)
        data = buf.getvalue()
        if path is None:
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
        else:
            with open(path, "wb") as f:
                f.write(data)

    elif fmt == "jsonl":
        lines = "\n".join(
            json.dumps(row, default=_json_default)
            for row in table.to_pylist()
        ) + "\n"
        if path is None:
            click.echo(lines, nl=False)
        else:
            with open(path, "w") as f:
                f.write(lines)

    else:  # json
        content = json.dumps(table.to_pylist(), indent=2, default=_json_default)
        if path is None:
            click.echo(content)
        else:
            with open(path, "w") as f:
                f.write(content)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@main.command(name="list")
@click.option("--config", "-c", default="config.yaml", show_default=True,
              type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--table", "-t", default=None,
              help="Filter to this table. Omit to list all configured tables.")
@click.option("--from", "from_dt", default=None,
              type=click.DateTime(formats=_DT_FORMATS),
              help="Show partitions starting on or after this date.")
@click.option("--to", "to_dt", default=None,
              type=click.DateTime(formats=_DT_FORMATS),
              help="Show partitions starting before this date.")
@click.option("--status", "filter_status", default=None,
              type=click.Choice(
                  ["PENDING", "EXPORTING", "UPLOADED", "VERIFIED",
                   "DELETING", "DONE", "FAILED"],
                  case_sensitive=False,
              ),
              help="Filter by batch status.")
@click.option("--json", "output_json", is_flag=True, help="Output as JSON.")
def list_cmd(
    config: str,
    table: str | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
    filter_status: str | None,
    output_json: bool,
) -> None:
    """List Parquet files stored in S3 with their metadata.

    Shows every archived batch with partition path, row count, size, and
    status. Combines data from S3 manifests — no database connection needed.

    Examples:

    \b
        # All batches for all tables
        pg-freezer list

        # One table, date range
        pg-freezer list -t sensor_readings --from 2024-01-01 --to 2024-02-01

        # Only failed batches
        pg-freezer list --status FAILED

        # Machine-readable
        pg-freezer list --json | jq '.[] | select(.status == "DONE")'
    """
    from_dt_utc = _dt_utc(from_dt) if from_dt else None
    to_dt_utc   = _dt_utc(to_dt)   if to_dt   else None

    try:
        cfg = load_config(config)
    except ConfigError as e:
        click.echo(f"Configuration error: {e}", err=True)
        sys.exit(1)

    try:
        entries = asyncio.run(_do_list(cfg, table, from_dt_utc, to_dt_utc, filter_status))
    except Exception as e:
        click.echo(f"Fatal error: {e}", err=True)
        sys.exit(1)

    if output_json:
        click.echo(json.dumps(entries, indent=2, default=_json_default))
        return

    if not entries:
        click.echo("No matching partitions found.")
        return

    # Column widths
    tw = max(len(e["table"]) for e in entries)
    pw = max(len(e["partition"]) for e in entries)

    header = (
        f"{'TABLE':<{tw}}  {'PARTITION':<{pw}}  "
        f"{'STATUS':<10}  {'ROWS':>10}  {'SIZE':>9}  EXPORTED AT"
    )
    click.echo(header)
    click.echo("-" * len(header))

    for e in entries:
        rows    = f"{e['row_count']:,}"   if e["row_count"]        is not None else "-"
        size    = _fmt_size(e["size_bytes"]) if e["size_bytes"]    is not None else "-"
        exp_at  = e["exported_at"][:19]      if e["exported_at"]   else "-"
        click.echo(
            f"{e['table']:<{tw}}  {e['partition']:<{pw}}  "
            f"{e['status']:<10}  {rows:>10}  {size:>9}  {exp_at}"
        )

    total_rows  = sum(e["row_count"]  or 0 for e in entries)
    total_bytes = sum(e["size_bytes"] or 0 for e in entries)
    click.echo(
        f"\n{len(entries)} partition(s)  "
        f"{total_rows:,} rows  "
        f"{_fmt_size(total_bytes)}"
    )


async def _do_list(
    cfg,
    table_filter: str | None,
    from_dt: datetime | None,
    to_dt: datetime | None,
    status_filter: str | None,
) -> list[dict]:
    storage = StorageClient(cfg.storage)
    store   = ManifestStore(storage, cfg.storage.bucket)

    tables = cfg.tables
    if table_filter:
        tables = [t for t in tables if t.name == table_filter]

    entries = []
    for table_cfg in tables:
        manifests = await store.list_for_table(table_cfg.name)
        for m in manifests:
            if from_dt and m.window_start < from_dt:
                continue
            if to_dt and m.window_start >= to_dt:
                continue
            if status_filter and m.status.upper() != status_filter.upper():
                continue
            entries.append({
                "table":       m.table,
                "schema":      m.schema_name,
                "partition":   m.partition,
                "window_start": m.window_start.isoformat(),
                "window_end":   m.window_end.isoformat(),
                "status":      m.status,
                "batch_id":    m.batch_id,
                "row_count":   m.row_count,
                "size_bytes":  m.parquet_size_bytes,
                "parquet_path": m.parquet_path,
                "exported_at": m.exported_at.isoformat() if m.exported_at else None,
                "verified_at": m.verified_at.isoformat() if m.verified_at else None,
                "deleted_at":  m.deleted_at.isoformat()  if m.deleted_at  else None,
                "error":       m.error,
            })

    entries.sort(key=lambda e: (e["table"], e["window_start"]))
    return entries


def _fmt_size(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.0f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"
