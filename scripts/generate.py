#!/usr/bin/env python3
"""
Test data generator for pg_freezer end-to-end testing.

Creates two timeseries tables, backfills historical data (so pg_freezer has
windows to export immediately), then continuously inserts new records.

Usage:
    python scripts/generate.py
    python scripts/generate.py --dsn postgresql://user:pass@localhost/db
    python scripts/generate.py --backfill-days 3 --batch-size 20 --interval 1.0
    python scripts/generate.py --no-backfill   # live inserts only

Tables created:
    public.sensor_readings   — float sensor values, e.g. temperature/humidity
    public.device_events     — JSON event payloads from IoT devices

Pair this with config.dev.yaml and run:
    pg-freezer run --config scripts/config.dev.yaml --dry-run
    pg-freezer run --config scripts/config.dev.yaml
    pg-freezer status --config scripts/config.dev.yaml
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone

import asyncpg

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

DDL_SENSOR_READINGS = """
CREATE TABLE IF NOT EXISTS sensor_readings (
    recorded_at  TIMESTAMPTZ NOT NULL,
    sensor_id    INTEGER     NOT NULL,
    sensor_name  TEXT        NOT NULL,
    value        DOUBLE PRECISION NOT NULL,
    unit         TEXT        NOT NULL
);
"""

DDL_DEVICE_EVENTS = """
CREATE TABLE IF NOT EXISTS device_events (
    occurred_at  TIMESTAMPTZ NOT NULL,
    device_id    TEXT        NOT NULL,
    event_type   TEXT        NOT NULL,
    severity     INTEGER     NOT NULL,
    payload      JSONB       NOT NULL
);
"""

# ---------------------------------------------------------------------------
# Fake data helpers
# ---------------------------------------------------------------------------

SENSOR_POOL = [
    (1,  "temp_north",     "°C",   lambda: round(random.gauss(21, 3), 2)),
    (2,  "temp_south",     "°C",   lambda: round(random.gauss(23, 4), 2)),
    (3,  "humidity_east",  "%",    lambda: round(random.uniform(30, 80), 1)),
    (4,  "humidity_west",  "%",    lambda: round(random.uniform(35, 75), 1)),
    (5,  "pressure_roof",  "hPa",  lambda: round(random.gauss(1013, 5), 1)),
    (6,  "co2_lab",        "ppm",  lambda: round(random.gauss(600, 50), 0)),
    (7,  "vibration_pump", "mm/s", lambda: round(abs(random.gauss(0.5, 0.3)), 3)),
    (8,  "power_main",     "kW",   lambda: round(random.uniform(10, 50), 2)),
]

EVENT_TYPES = ["startup", "shutdown", "alert", "heartbeat", "config_change", "error"]
DEVICE_IDS  = [f"device_{i:03d}" for i in range(1, 21)]


def _sensor_rows(ts: datetime, count: int) -> list[tuple]:
    rows = []
    for _ in range(count):
        sid, name, unit, gen = random.choice(SENSOR_POOL)
        rows.append((ts, sid, name, gen(), unit))
    return rows


def _event_rows(ts: datetime, count: int) -> list[tuple]:
    rows = []
    for _ in range(count):
        device = random.choice(DEVICE_IDS)
        etype  = random.choice(EVENT_TYPES)
        sev    = random.randint(1, 5)
        payload = json.dumps({
            "firmware": f"1.{random.randint(0, 9)}.{random.randint(0, 99)}",
            "uptime_s": random.randint(0, 86400),
            "extra":    random.random() > 0.7,
        })
        rows.append((ts, device, etype, sev, payload))
    return rows


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

async def setup_tables(conn: asyncpg.pool.PoolConnectionProxy) -> None:
    await conn.execute(DDL_SENSOR_READINGS)
    await conn.execute(DDL_DEVICE_EVENTS)
    print("Tables ready: sensor_readings, device_events")


async def insert_batch(
    pool: asyncpg.Pool,
    ts: datetime,
    sensor_count: int,
    event_count: int,
) -> tuple[int, int]:
    sensor_rows = _sensor_rows(ts, sensor_count)
    event_rows  = _event_rows(ts, event_count)

    async with pool.acquire() as conn:
        await conn.executemany(
            "INSERT INTO sensor_readings (recorded_at, sensor_id, sensor_name, value, unit) "
            "VALUES ($1, $2, $3, $4, $5)",
            sensor_rows,
        )
        await conn.executemany(
            "INSERT INTO device_events (occurred_at, device_id, event_type, severity, payload) "
            "VALUES ($1, $2, $3, $4, $5::jsonb)",
            event_rows,
        )
    return len(sensor_rows), len(event_rows)


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

async def backfill(
    pool: asyncpg.Pool,
    days: int,
    sensor_per_ts: int,
    event_per_ts: int,
    interval_minutes: int = 10,
) -> None:
    """Insert historical data at <interval_minutes> resolution going back <days> days."""
    now     = datetime.now(tz=timezone.utc)
    start   = now - timedelta(days=days)
    current = start
    step    = timedelta(minutes=interval_minutes)

    total_ts      = int((now - start) / step)
    total_sensors = 0
    total_events  = 0
    done          = 0

    print(f"Backfilling {days} day(s) at {interval_minutes}-minute resolution "
          f"({total_ts} timestamps)…")

    t0 = time.monotonic()
    while current < now:
        s, e = await insert_batch(pool, current, sensor_per_ts, event_per_ts)
        total_sensors += s
        total_events  += e
        done          += 1
        current       += step

        if done % 100 == 0 or current >= now:
            pct     = done / total_ts * 100
            elapsed = time.monotonic() - t0
            rate    = done / elapsed if elapsed > 0 else 0
            print(
                f"  {pct:5.1f}%  {done}/{total_ts} timestamps  "
                f"{total_sensors:,} sensor rows  {total_events:,} event rows  "
                f"({rate:.0f} ts/s)",
                end="\r",
                flush=True,
            )

    elapsed = time.monotonic() - t0
    print(
        f"\nBackfill complete: {total_sensors:,} sensor rows, "
        f"{total_events:,} event rows in {elapsed:.1f}s"
    )


# ---------------------------------------------------------------------------
# Live insertion loop
# ---------------------------------------------------------------------------

async def live_loop(
    pool: asyncpg.Pool,
    sensor_per_tick: int,
    event_per_tick: int,
    interval: float,
) -> None:
    total_s = 0
    total_e = 0
    ticks   = 0
    print(f"\nLive mode: inserting ~{sensor_per_tick} sensor + "
          f"~{event_per_tick} event rows every {interval}s. Ctrl-C to stop.\n")
    try:
        while True:
            t0 = time.monotonic()
            ts = datetime.now(tz=timezone.utc)
            s, e    = await insert_batch(pool, ts, sensor_per_tick, event_per_tick)
            total_s += s
            total_e += e
            ticks   += 1
            elapsed = time.monotonic() - t0
            print(
                f"[{ts.strftime('%H:%M:%S')}] "
                f"+{s} sensor / +{e} events  "
                f"total: {total_s:,} / {total_e:,}  "
                f"({elapsed*1000:.0f}ms)",
                flush=True,
            )
            sleep_for = max(0.0, interval - elapsed)
            await asyncio.sleep(sleep_for)
    except asyncio.CancelledError:
        pass


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        "--dsn",
        default=os.environ.get(
            "PG_DSN", "postgresql://postgres:postgres@localhost:5432/testdb"
        ),
        help="PostgreSQL DSN (default: PG_DSN env var or localhost testdb)",
    )
    p.add_argument(
        "--backfill-days", type=int, default=3,
        help="Days of historical data to insert on startup (default: 3)",
    )
    p.add_argument(
        "--backfill-resolution", type=int, default=10, metavar="MINUTES",
        help="Backfill timestamp interval in minutes (default: 10)",
    )
    p.add_argument(
        "--no-backfill", action="store_true",
        help="Skip historical backfill, start live inserts immediately",
    )
    p.add_argument(
        "--batch-size", type=int, default=15,
        help="Rows per table per tick in live mode (default: 15)",
    )
    p.add_argument(
        "--interval", type=float, default=1.0,
        help="Seconds between live inserts (default: 1.0)",
    )
    p.add_argument(
        "--setup-only", action="store_true",
        help="Create tables and backfill then exit (no live loop)",
    )
    return p.parse_args()


async def main() -> None:
    args = parse_args()

    print(f"Connecting to: {args.dsn.split('@')[-1]}")  # hide credentials in output
    try:
        pool = await asyncpg.create_pool(args.dsn, min_size=2, max_size=5)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        sys.exit(1)

    async with pool.acquire() as conn:
        await setup_tables(conn)

    if not args.no_backfill:
        await backfill(
            pool,
            days=args.backfill_days,
            sensor_per_ts=args.batch_size,
            event_per_ts=max(1, args.batch_size // 3),
            interval_minutes=args.backfill_resolution,
        )

    if not args.setup_only:
        await live_loop(
            pool,
            sensor_per_tick=args.batch_size,
            event_per_tick=max(1, args.batch_size // 3),
            interval=args.interval,
        )

    await pool.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
