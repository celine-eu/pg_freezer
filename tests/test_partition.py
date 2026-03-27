"""Tests for partition path computation and window generation."""

from datetime import datetime, timezone

import pytest

from pg_freezer.partition import (
    floor_to_partition,
    generate_windows,
    next_partition,
    partition_path,
)


def dt(*args, **kwargs) -> datetime:
    return datetime(*args, tzinfo=timezone.utc, **kwargs)


class TestFloorToPartition:
    def test_daily_noon(self):
        assert floor_to_partition(dt(2024, 1, 15, 12, 30), "daily") == dt(2024, 1, 15)

    def test_daily_midnight(self):
        assert floor_to_partition(dt(2024, 1, 15, 0, 0), "daily") == dt(2024, 1, 15)

    def test_daily_end_of_day(self):
        assert floor_to_partition(dt(2024, 1, 15, 23, 59, 59), "daily") == dt(2024, 1, 15)

    def test_hourly_mid_hour(self):
        assert floor_to_partition(dt(2024, 1, 15, 14, 45, 30), "hourly") == dt(2024, 1, 15, 14)

    def test_hourly_on_hour(self):
        assert floor_to_partition(dt(2024, 1, 15, 14, 0, 0), "hourly") == dt(2024, 1, 15, 14)

    def test_monthly_mid_month(self):
        assert floor_to_partition(dt(2024, 3, 15, 10, 0), "monthly") == dt(2024, 3, 1)

    def test_monthly_first_day(self):
        assert floor_to_partition(dt(2024, 3, 1, 0, 0), "monthly") == dt(2024, 3, 1)

    def test_naive_datetime_treated_as_utc(self):
        naive = datetime(2024, 1, 15, 12, 30)
        result = floor_to_partition(naive, "daily")
        assert result.tzinfo is not None
        assert result == dt(2024, 1, 15)


class TestNextPartition:
    def test_daily(self):
        assert next_partition(dt(2024, 1, 15), "daily") == dt(2024, 1, 16)

    def test_daily_end_of_month(self):
        assert next_partition(dt(2024, 1, 31), "daily") == dt(2024, 2, 1)

    def test_daily_year_boundary(self):
        assert next_partition(dt(2024, 12, 31), "daily") == dt(2025, 1, 1)

    def test_hourly(self):
        assert next_partition(dt(2024, 1, 15, 23), "hourly") == dt(2024, 1, 16, 0)

    def test_monthly_normal(self):
        assert next_partition(dt(2024, 1, 1), "monthly") == dt(2024, 2, 1)

    def test_monthly_december(self):
        assert next_partition(dt(2024, 12, 1), "monthly") == dt(2025, 1, 1)

    def test_monthly_november(self):
        assert next_partition(dt(2024, 11, 1), "monthly") == dt(2024, 12, 1)

    def test_monthly_leap_year(self):
        # Feb 2024 is a leap year — next_partition should give March 1
        assert next_partition(dt(2024, 2, 1), "monthly") == dt(2024, 3, 1)


class TestPartitionPath:
    def test_daily_format(self):
        assert partition_path(dt(2024, 1, 5), "daily") == "year=2024/month=01/day=05"

    def test_daily_zero_padding(self):
        assert partition_path(dt(2024, 12, 31), "daily") == "year=2024/month=12/day=31"

    def test_hourly_format(self):
        result = partition_path(dt(2024, 1, 5, 9), "hourly")
        assert result == "year=2024/month=01/day=05/hour=09"

    def test_hourly_midnight(self):
        result = partition_path(dt(2024, 1, 5, 0), "hourly")
        assert result == "year=2024/month=01/day=05/hour=00"

    def test_monthly_format(self):
        assert partition_path(dt(2024, 3, 1), "monthly") == "year=2024/month=03"

    def test_monthly_single_digit(self):
        assert partition_path(dt(2024, 1, 1), "monthly") == "year=2024/month=01"


class TestGenerateWindows:
    def test_daily_three_days(self):
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 1, 4), "daily")
        assert len(windows) == 3
        assert windows[0] == (dt(2024, 1, 1), dt(2024, 1, 2))
        assert windows[1] == (dt(2024, 1, 2), dt(2024, 1, 3))
        assert windows[2] == (dt(2024, 1, 3), dt(2024, 1, 4))

    def test_daily_range_start_floored(self):
        windows = generate_windows(dt(2024, 1, 1, 12), dt(2024, 1, 3), "daily")
        # range_start is floored to 2024-01-01 00:00
        assert windows[0][0] == dt(2024, 1, 1)

    def test_hourly_four_hours(self):
        windows = generate_windows(dt(2024, 1, 1, 10), dt(2024, 1, 1, 14), "hourly")
        assert len(windows) == 4
        assert windows[0] == (dt(2024, 1, 1, 10), dt(2024, 1, 1, 11))

    def test_monthly_three_months(self):
        windows = generate_windows(dt(2024, 1, 15), dt(2024, 4, 1), "monthly")
        assert len(windows) == 3
        assert windows[0][0] == dt(2024, 1, 1)
        assert windows[2][0] == dt(2024, 3, 1)

    def test_empty_when_range_start_equals_end(self):
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 1, 1), "daily")
        assert windows == []

    def test_empty_when_range_start_after_end(self):
        windows = generate_windows(dt(2024, 1, 5), dt(2024, 1, 1), "daily")
        assert windows == []

    def test_month_boundary_wrap(self):
        windows = generate_windows(dt(2024, 12, 1), dt(2025, 2, 1), "monthly")
        assert len(windows) == 2
        assert windows[0][0] == dt(2024, 12, 1)
        assert windows[1][0] == dt(2025, 1, 1)

    def test_chronological_order(self):
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 1, 10), "daily")
        starts = [w[0] for w in windows]
        assert starts == sorted(starts)

    # --- partial-partition boundary tests (regression for the window_end > range_end fix) ---

    def test_monthly_cutoff_mid_month_excludes_incomplete(self):
        # range_end falls mid-February — February must NOT be returned
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 2, 15), "monthly")
        assert len(windows) == 1
        assert windows[0] == (dt(2024, 1, 1), dt(2024, 2, 1))

    def test_monthly_cutoff_exactly_on_boundary_includes(self):
        # range_end exactly on March 1 — February IS a complete window
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 3, 1), "monthly")
        assert len(windows) == 2
        assert windows[-1] == (dt(2024, 2, 1), dt(2024, 3, 1))

    def test_daily_cutoff_mid_day_excludes_partial(self):
        # range_end at noon — the day that started that day must NOT be returned
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 1, 3, 12), "daily")
        assert len(windows) == 2
        assert windows[-1] == (dt(2024, 1, 2), dt(2024, 1, 3))

    def test_daily_cutoff_exactly_on_midnight_includes(self):
        # range_end exactly at midnight of Jan 4 — Jan 3 IS a complete window
        windows = generate_windows(dt(2024, 1, 1), dt(2024, 1, 4), "daily")
        assert len(windows) == 3
        assert windows[-1] == (dt(2024, 1, 3), dt(2024, 1, 4))

    def test_hourly_cutoff_mid_hour_excludes_partial(self):
        windows = generate_windows(dt(2024, 1, 1, 10), dt(2024, 1, 1, 12, 30), "hourly")
        assert len(windows) == 2
        assert windows[-1] == (dt(2024, 1, 1, 11), dt(2024, 1, 1, 12))
