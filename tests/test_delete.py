"""Tests for delete_window chunked-delete behaviour (no live DB required)."""

from contextlib import asynccontextmanager
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_freezer.config import TableConfig
from pg_freezer.errors import DeleteSafetyError
from pg_freezer.exporter import DBExporter

UTC = timezone.utc
WINDOW_START = datetime(2026, 2, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 3, 1, tzinfo=UTC)

TABLE_CFG = TableConfig(
    name="mqtt_messages",
    schema_name="raw",
    timestamp_column="ts",
    partition_by="monthly",
)

_CHUNK = 25_000  # must match _DELETE_CHUNK in exporter.py


def _make_pool(chunk_counts: list[int], remaining: int = 0, ts_oid: int = 1184):
    """
    Build a mock asyncpg pool whose execute() returns each DELETE chunk count in order.
    The first execute() call (SET statement_timeout) is consumed before the chunks.
    fetchval() returns `remaining` for the final safety COUNT(*).
    """
    conn = MagicMock()
    execute_returns = ["SET"] + [f"DELETE {n}" for n in chunk_counts]
    conn.execute = AsyncMock(side_effect=execute_returns)
    conn.fetchrow = AsyncMock(return_value={"atttypid": ts_oid})
    conn.fetchval = AsyncMock(return_value=remaining)

    @asynccontextmanager
    async def _tx():
        yield

    conn.transaction = _tx

    pool = MagicMock()

    @asynccontextmanager
    async def _acquire():
        yield conn

    pool.acquire = _acquire
    return pool, conn


class TestDeleteWindowChunking:
    async def test_small_table_single_chunk(self):
        """100 rows fit in one chunk — one DELETE call, returns 100."""
        pool, conn = _make_pool(chunk_counts=[100])
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        result = await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=100)

        assert result == 100
        assert conn.execute.call_count == 2  # SET + 1 DELETE

    async def test_large_table_multiple_chunks(self):
        """80 500 rows → three full chunks + one partial, total returned correctly."""
        pool, conn = _make_pool(chunk_counts=[_CHUNK, _CHUNK, _CHUNK, 5_500])
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        result = await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=80_500)

        assert result == 3 * _CHUNK + 5_500
        assert conn.execute.call_count == 5  # SET + 4 DELETEs

    async def test_exact_chunk_boundary_loops_once_more(self):
        """Exactly _CHUNK rows: first DELETE fills the chunk, loop continues and hits DELETE 0."""
        pool, conn = _make_pool(chunk_counts=[_CHUNK, 0])
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        result = await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=_CHUNK)

        assert result == _CHUNK
        assert conn.execute.call_count == 3  # SET + DELETE _CHUNK + DELETE 0

    async def test_remaining_rows_raises_safety_error(self):
        """COUNT(*) > 0 after the delete loop → DeleteSafetyError."""
        pool, conn = _make_pool(chunk_counts=[100], remaining=5)
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        with pytest.raises(DeleteSafetyError, match="5 rows remain"):
            await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=100)

    async def test_empty_window_returns_zero(self):
        """Window already empty → DELETE 0 immediately, no error."""
        pool, conn = _make_pool(chunk_counts=[0], remaining=0)
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        result = await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=0)

        assert result == 0

    async def test_resumed_partial_delete_no_count_mismatch_error(self):
        """
        After a crash mid-delete some rows were already committed away.
        expected_count (original export) > total_deleted (this run) must not raise.
        Only the remaining==0 safety check matters.
        """
        pool, conn = _make_pool(chunk_counts=[_CHUNK, _CHUNK, 21_934])
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        result = await exporter.delete_window(
            TABLE_CFG, WINDOW_START, WINDOW_END,
            expected_count=1_000_000,  # original export count — higher than what's left
        )

        assert result == 2 * _CHUNK + 21_934

    async def test_text_timestamp_column_uses_iso_strings(self):
        """For text-type ts columns (OID 25), params passed to DELETE are ISO strings."""
        pool, conn = _make_pool(chunk_counts=[50], ts_oid=25)
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=50)

        delete_call = conn.execute.call_args_list[1]  # index 0 is SET timeout
        args = delete_call.args
        assert isinstance(args[1], str)  # $1 — ISO string, not datetime
        assert isinstance(args[2], str)  # $2

    async def test_timestamptz_column_uses_aware_datetimes(self):
        """For timestamptz columns (OID 1184), params are UTC-aware datetimes."""
        pool, conn = _make_pool(chunk_counts=[50], ts_oid=1184)
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=50)

        delete_call = conn.execute.call_args_list[1]
        args = delete_call.args
        assert isinstance(args[1], datetime)
        assert args[1].tzinfo is not None

    async def test_timestamp_no_tz_column_uses_naive_datetimes(self):
        """For timestamp-without-tz columns (OID 1114), params are naive datetimes."""
        pool, conn = _make_pool(chunk_counts=[50], ts_oid=1114)
        exporter = DBExporter(pool, statement_timeout_ms=30_000)

        await exporter.delete_window(TABLE_CFG, WINDOW_START, WINDOW_END, expected_count=50)

        delete_call = conn.execute.call_args_list[1]
        args = delete_call.args
        assert isinstance(args[1], datetime)
        assert args[1].tzinfo is None
