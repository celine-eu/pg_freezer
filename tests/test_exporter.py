"""Tests for Parquet serialization and type mapping (no live DB required)."""

from datetime import datetime, date, timezone
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pg_freezer.exporter import (
    _oid_to_arrow,
    _records_to_arrow,
    _serialize_to_parquet,
    count_parquet_rows,
)


class TestOidToArrow:
    def test_timestamptz(self):
        t = _oid_to_arrow(1184, "ts")
        assert t == pa.timestamp("us", tz="UTC")

    def test_timestamp(self):
        t = _oid_to_arrow(1114, "ts")
        assert t == pa.timestamp("us")

    def test_int4(self):
        assert _oid_to_arrow(23, "id") == pa.int32()

    def test_int8(self):
        assert _oid_to_arrow(20, "id") == pa.int64()

    def test_float8(self):
        assert _oid_to_arrow(701, "val") == pa.float64()

    def test_text(self):
        assert _oid_to_arrow(25, "name") == pa.large_utf8()

    def test_bool(self):
        assert _oid_to_arrow(16, "active") == pa.bool_()

    def test_numeric_maps_to_float64(self):
        assert _oid_to_arrow(1700, "price") == pa.float64()

    def test_uuid_maps_to_string(self):
        assert _oid_to_arrow(2950, "uid") == pa.large_utf8()

    def test_unknown_oid_falls_back_to_string(self):
        assert _oid_to_arrow(99999, "col") == pa.large_utf8()

    def test_date(self):
        assert _oid_to_arrow(1082, "d") == pa.date32()


class MockRecord:
    """Minimal mock of asyncpg.Record for testing."""
    def __init__(self, values: list):
        self._values = values

    def __getitem__(self, idx):
        return self._values[idx]

    def keys(self):
        return [f"col{i}" for i in range(len(self._values))]


def make_records(rows: list[list]) -> list[MockRecord]:
    return [MockRecord(row) for row in rows]


class TestRecordsToArrow:
    def test_basic_int_and_text(self):
        records = make_records([[1, "hello"], [2, "world"]])
        table = _records_to_arrow(
            records,
            column_names=["id", "name"],
            column_oids=[23, 25],
        )
        assert table.num_rows == 2
        assert table.schema.field("id").type == pa.int32()
        assert table.schema.field("name").type == pa.large_utf8()

    def test_nullable_values(self):
        records = make_records([[1, None], [2, "hi"]])
        table = _records_to_arrow(records, ["id", "name"], [23, 25])
        assert table.column("name")[0].as_py() is None
        assert table.column("name")[1].as_py() == "hi"

    def test_timestamptz(self):
        ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
        records = make_records([[ts]])
        table = _records_to_arrow(records, ["ts"], [1184])
        assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_float8(self):
        records = make_records([[3.14], [2.71]])
        table = _records_to_arrow(records, ["val"], [701])
        assert table.column("val")[0].as_py() == pytest.approx(3.14, rel=1e-6)

    def test_bool(self):
        records = make_records([[True], [False], [None]])
        table = _records_to_arrow(records, ["flag"], [16])
        assert table.column("flag")[0].as_py() is True
        assert table.column("flag")[2].as_py() is None


class TestSerializeToParquet:
    def test_round_trip(self):
        t = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]})
        data = _serialize_to_parquet(t)
        assert isinstance(data, bytes)
        assert len(data) > 0

        restored = pq.read_table(pa.py_buffer(data))
        assert restored.num_rows == 3
        assert restored.column_names == ["id", "name"]

    def test_empty_table(self):
        schema = pa.schema([pa.field("id", pa.int32()), pa.field("val", pa.float64())])
        empty = schema.empty_table()
        data = _serialize_to_parquet(empty)
        restored = pq.read_table(pa.py_buffer(data))
        assert restored.num_rows == 0
        assert "id" in restored.column_names

    def test_snappy_compression_default(self):
        t = pa.table({"x": list(range(100))})
        data = _serialize_to_parquet(t)
        pf = pq.ParquetFile(pa.py_buffer(data))
        # Check compression is snappy
        col_meta = pf.metadata.row_group(0).column(0)
        assert col_meta.compression.lower() == "snappy"


class TestCountParquetRows:
    def test_count(self):
        t = pa.table({"id": list(range(42))})
        data = _serialize_to_parquet(t)
        assert count_parquet_rows(data) == 42

    def test_count_empty(self):
        schema = pa.schema([pa.field("id", pa.int32())])
        data = _serialize_to_parquet(schema.empty_table())
        assert count_parquet_rows(data) == 0

    def test_count_large(self):
        t = pa.table({"id": list(range(10_000))})
        data = _serialize_to_parquet(t)
        assert count_parquet_rows(data) == 10_000


class TestDateLookAlikeColumns:
    """Columns whose names or values look date-like but whose PG type is something else."""

    # --- text / varchar / bpchar holding ISO strings ---

    def test_text_column_with_iso_timestamp_string(self):
        # OID 25 = text; asyncpg returns str, not datetime
        records = make_records([
            ["2026-04-01T00:00:00+00:00"],
            ["2026-04-15T12:30:00+00:00"],
        ])
        table = _records_to_arrow(records, ["event_time"], [25])
        assert table.schema.field("event_time").type == pa.large_utf8()
        assert table.column("event_time")[0].as_py() == "2026-04-01T00:00:00+00:00"

    def test_varchar_column_with_date_string(self):
        # OID 1043 = varchar; date-only strings stay as strings
        records = make_records([["2026-01-01"], ["2026-12-31"]])
        table = _records_to_arrow(records, ["created_date"], [1043])
        assert table.schema.field("created_date").type == pa.large_utf8()
        assert table.column("created_date")[1].as_py() == "2026-12-31"

    def test_bpchar_column_with_fixed_date_string(self):
        # OID 1042 = bpchar (char(n)); e.g. YYYYMMDD fixed-width dates
        records = make_records([["20260401"], ["20261231"]])
        table = _records_to_arrow(records, ["date_key"], [1042])
        assert table.schema.field("date_key").type == pa.large_utf8()
        assert table.column("date_key")[0].as_py() == "20260401"

    def test_name_type_column_with_date_like_string(self):
        # OID 19 = name (internal PG identifier type); not a timestamp
        records = make_records([["2026-04-01_partition"], ["2026-05-01_partition"]])
        table = _records_to_arrow(records, ["partition_name"], [19])
        assert table.schema.field("partition_name").type == pa.large_utf8()

    # --- numeric types used as epoch storage ---

    def test_bigint_column_with_unix_epoch_seconds(self):
        # OID 20 = int8; storing Unix epoch seconds looks like a timestamp by value
        records = make_records([[1743465600], [1746057600]])
        table = _records_to_arrow(records, ["event_ts"], [20])
        assert table.schema.field("event_ts").type == pa.int64()
        assert table.column("event_ts")[0].as_py() == 1743465600

    def test_numeric_column_with_epoch_millis(self):
        # OID 1700 = numeric; epoch-millis look date-like but coerce to float
        records = make_records([[Decimal("1743465600000")], [Decimal("1746057600000")]])
        table = _records_to_arrow(records, ["ts_ms"], [1700])
        assert table.schema.field("ts_ms").type == pa.float64()
        assert table.column("ts_ms")[0].as_py() == pytest.approx(1_743_465_600_000.0)

    def test_float8_column_with_fractional_epoch(self):
        # OID 701 = float8; fractional epoch seconds (e.g. from sensors)
        records = make_records([[1743465600.123], [1746057600.456]])
        table = _records_to_arrow(records, ["sensor_ts"], [701])
        assert table.schema.field("sensor_ts").type == pa.float64()

    # --- JSON/JSONB columns embedding datetime payloads ---

    def test_jsonb_column_with_datetime_payload(self):
        # OID 3802 = jsonb; asyncpg returns a dict; _coerce_value serializes to str
        records = make_records([
            [{"ts": "2026-04-01T00:00:00Z", "value": 42}],
            [{"ts": "2026-05-01T00:00:00Z", "value": 99}],
        ])
        table = _records_to_arrow(records, ["payload"], [3802])
        assert table.schema.field("payload").type == pa.large_utf8()
        import json as _json
        parsed = _json.loads(table.column("payload")[0].as_py())
        assert parsed["ts"] == "2026-04-01T00:00:00Z"

    def test_json_column_with_nested_date(self):
        # OID 114 = json; same path as jsonb
        records = make_records([[{"date": "2026-04-01", "ok": True}]])
        table = _records_to_arrow(records, ["meta"], [114])
        assert table.schema.field("meta").type == pa.large_utf8()

    # --- datetime object accidentally arriving in a text-typed column ---

    def test_datetime_object_in_text_column_falls_back_to_string(self):
        # If asyncpg somehow returns a datetime for a text column (shouldn't
        # happen in practice, but guards the fallback path), _records_to_arrow
        # must not raise — it falls back to large_utf8 serialization.
        ts = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        records = make_records([[ts]])
        table = _records_to_arrow(records, ["created_at"], [25])
        # Column must land as string, not error
        assert table.schema.field("created_at").type == pa.large_utf8()
        assert table.column("created_at")[0].as_py() is not None

    def test_date_object_in_text_column_falls_back_to_string(self):
        # Same fallback for a bare date object in a text column
        d = date(2026, 4, 1)
        records = make_records([[d]])
        table = _records_to_arrow(records, ["day"], [25])
        assert table.schema.field("day").type == pa.large_utf8()

    # --- real timestamp / date columns work correctly as contrast ---

    def test_actual_timestamptz_column(self):
        ts = datetime(2026, 4, 1, tzinfo=timezone.utc)
        records = make_records([[ts]])
        table = _records_to_arrow(records, ["ts"], [1184])
        assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_actual_date_column(self):
        # OID 1082; asyncpg returns datetime.date objects
        d = date(2026, 4, 1)
        records = make_records([[d]])
        table = _records_to_arrow(records, ["day"], [1082])
        assert table.schema.field("day").type == pa.date32()
        assert table.column("day")[0].as_py() == d

    def test_nullable_text_column_with_mixed_date_strings(self):
        records = make_records([
            ["2026-04-01"],
            [None],
            ["not-a-date"],
        ])
        table = _records_to_arrow(records, ["raw_date"], [25])
        assert table.column("raw_date")[1].as_py() is None
        assert table.column("raw_date")[2].as_py() == "not-a-date"


class TestDuckDBIntegration:
    """Verify Parquet files can be queried with DuckDB (simulates Trino read path)."""

    def test_duckdb_can_read_parquet(self):
        try:
            import duckdb
        except ImportError:
            pytest.skip("duckdb not installed")

        ts = datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)
        records = make_records([
            [1, ts, 23.5, "sensor_a"],
            [2, ts, 24.1, "sensor_b"],
        ])
        table = _records_to_arrow(records, ["id", "ts", "value", "name"], [23, 1184, 701, 25])
        data = _serialize_to_parquet(table)

        import tempfile, os
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            f.write(data)
            tmp_path = f.name

        try:
            conn = duckdb.connect()
            result = conn.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp_path}')").fetchone()
            assert result[0] == 2
        finally:
            os.unlink(tmp_path)
