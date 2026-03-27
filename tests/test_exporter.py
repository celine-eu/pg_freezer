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
