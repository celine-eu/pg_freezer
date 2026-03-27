"""Hive-style partition path computation and window generation."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

Granularity = Literal["hourly", "daily", "monthly"]


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def floor_to_partition(dt: datetime, granularity: Granularity) -> datetime:
    """Truncate datetime to the start of its partition window (UTC-aware)."""
    dt = _ensure_utc(dt)
    if granularity == "hourly":
        return dt.replace(minute=0, second=0, microsecond=0)
    elif granularity == "daily":
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    else:  # monthly
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def next_partition(window_start: datetime, granularity: Granularity) -> datetime:
    """Advance to the start of the next partition window."""
    from datetime import timedelta

    window_start = _ensure_utc(window_start)
    if granularity == "hourly":
        return window_start + timedelta(hours=1)
    elif granularity == "daily":
        return window_start + timedelta(days=1)
    else:  # monthly — cannot use timedelta, months have variable length
        m = window_start.month
        y = window_start.year
        if m == 12:
            return window_start.replace(year=y + 1, month=1, day=1)
        else:
            return window_start.replace(month=m + 1, day=1)


def partition_path(window_start: datetime, granularity: Granularity) -> str:
    """Return zero-padded Hive partition path string for a window start."""
    window_start = _ensure_utc(window_start)
    if granularity == "monthly":
        return f"year={window_start.year:04d}/month={window_start.month:02d}"
    elif granularity == "daily":
        return (
            f"year={window_start.year:04d}"
            f"/month={window_start.month:02d}"
            f"/day={window_start.day:02d}"
        )
    else:  # hourly
        return (
            f"year={window_start.year:04d}"
            f"/month={window_start.month:02d}"
            f"/day={window_start.day:02d}"
            f"/hour={window_start.hour:02d}"
        )


def generate_windows(
    range_start: datetime,
    range_end: datetime,
    granularity: Granularity,
) -> list[tuple[datetime, datetime]]:
    """
    Generate all (window_start, window_end) tuples from range_start to range_end.

    range_start is floored to the partition boundary first.
    range_end is exclusive — windows where window_start < range_end are included.
    Returns in chronological order.
    """
    range_start = _ensure_utc(range_start)
    range_end = _ensure_utc(range_end)

    windows: list[tuple[datetime, datetime]] = []
    current = floor_to_partition(range_start, granularity)

    while current < range_end:
        window_end = next_partition(current, granularity)
        windows.append((current, window_end))
        current = window_end

    return windows
