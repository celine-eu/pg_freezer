"""PostgreSQL → Parquet exporter using asyncpg and pyarrow."""

from __future__ import annotations

import io
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq

from pg_freezer.errors import DeleteSafetyError, ExportError

if TYPE_CHECKING:
    from pg_freezer.config import TableConfig

# Map asyncpg/PostgreSQL OID → pyarrow DataType
# OIDs sourced from pg_type catalog
_PG_OID_TO_ARROW: dict[int, pa.DataType] = {
    16: pa.bool_(),          # bool
    20: pa.int64(),          # int8
    21: pa.int16(),          # int2
    23: pa.int32(),          # int4
    25: pa.large_utf8(),     # text
    114: pa.large_utf8(),    # json (serialized)
    700: pa.float32(),       # float4
    701: pa.float64(),       # float8
    1082: pa.date32(),       # date
    1114: pa.timestamp("us"),                    # timestamp (no tz)
    1184: pa.timestamp("us", tz="UTC"),          # timestamptz
    1043: pa.large_utf8(),   # varchar
    1700: pa.float64(),      # numeric (lossy, Trino-compatible)
    2950: pa.large_utf8(),   # uuid
    3802: pa.large_utf8(),   # jsonb (serialized)
    17: pa.large_binary(),   # bytea
    19: pa.large_utf8(),     # name
    26: pa.int64(),          # oid
    1042: pa.large_utf8(),   # bpchar (char)
}


def _oid_to_arrow(oid: int, col_name: str, logger: Any | None = None) -> pa.DataType:
    arrow_type = _PG_OID_TO_ARROW.get(oid)
    if arrow_type is None:
        if logger:
            logger.warning(
                "unknown_pg_oid",
                oid=oid,
                column=col_name,
                fallback="large_utf8",
            )
        return pa.large_utf8()
    return arrow_type


def _coerce_value(value: Any, oid: int) -> Any:
    """Coerce a Python value from asyncpg to something pyarrow accepts."""
    if value is None:
        return None
    # json/jsonb: serialize to string
    if oid in (114, 3802):
        return json.dumps(value)
    # timestamptz: ensure UTC-aware
    if oid == 1184 and isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    # numeric: cast to float
    if oid == 1700:
        return float(value)
    # uuid: str
    if oid == 2950:
        return str(value)
    return value


class DBExporter:
    """Handles export and delete operations against a PostgreSQL pool."""

    def __init__(self, pool: asyncpg.Pool, statement_timeout_ms: int) -> None:
        self._pool = pool
        self._statement_timeout_ms = statement_timeout_ms

    async def export_window(
        self,
        table_config: "TableConfig",
        window_start: datetime,
        window_end: datetime,
        logger: Any | None = None,
    ) -> tuple[bytes, int]:
        """
        Fetch all rows in [window_start, window_end) and serialize to Parquet.
        Returns (parquet_bytes, row_count).
        Uses a server-side cursor to avoid OOM on large windows.
        """
        ts_col = table_config.timestamp_column
        schema = table_config.schema_name
        table = table_config.name

        where_clauses = [f"{ts_col} >= $1 AND {ts_col} < $2"]
        params: list[Any] = [window_start, window_end]

        if table_config.extra_filters:
            where_clauses.append(f"({table_config.extra_filters})")

        where = " AND ".join(where_clauses)
        query = f'SELECT * FROM "{schema}"."{table}" WHERE {where} ORDER BY {ts_col}'

        rows: list[asyncpg.Record] = []
        column_names: list[str] = []
        column_oids: list[int] = []

        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    f"SET statement_timeout = {self._statement_timeout_ms}"
                )
                async with conn.transaction():
                    cursor = conn.cursor(query, *params, prefetch=1000)
                    first = True
                    async for record in cursor:
                        if first:
                            column_names = list(record.keys())
                            # asyncpg Record doesn't expose OIDs directly;
                            # we introspect via the cursor's attributes after first record
                            first = False
                        rows.append(record)

                    # Introspect column types via pg_attribute
                    if column_names:
                        type_rows = await conn.fetch(
                            """
                            SELECT attname, atttypid
                            FROM pg_attribute
                            WHERE attrelid = $1::regclass
                              AND attnum > 0
                              AND NOT attisdropped
                            ORDER BY attnum
                            """,
                            f"{schema}.{table}",
                        )
                        oid_by_name = {r["attname"]: r["atttypid"] for r in type_rows}
                        column_oids = [
                            oid_by_name.get(col, 25) for col in column_names
                        ]

        except asyncpg.PostgresError as e:
            raise ExportError(
                f"Export query failed for {schema}.{table} "
                f"[{window_start} → {window_end}]: {e}"
            ) from e

        if not rows:
            # Empty window — produce empty Parquet with schema
            arrow_schema = pa.schema(
                [pa.field(name, pa.large_utf8()) for name in (column_names or ["_empty"])]
            )
            empty_table = arrow_schema.empty_table()
            return _serialize_to_parquet(empty_table), 0

        arrow_table = _records_to_arrow(rows, column_names, column_oids, logger)
        parquet_bytes = _serialize_to_parquet(arrow_table)
        return parquet_bytes, len(rows)

    async def count_window(
        self,
        table_config: "TableConfig",
        window_start: datetime,
        window_end: datetime,
    ) -> int:
        """Fast COUNT(*) for a window."""
        ts_col = table_config.timestamp_column
        schema = table_config.schema_name
        table = table_config.name

        where_clauses = [f"{ts_col} >= $1 AND {ts_col} < $2"]
        params: list[Any] = [window_start, window_end]
        if table_config.extra_filters:
            where_clauses.append(f"({table_config.extra_filters})")
        where = " AND ".join(where_clauses)

        async with self._pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {self._statement_timeout_ms}")
            result = await conn.fetchval(
                f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE {where}',
                *params,
            )
        return int(result)

    async def delete_window(
        self,
        table_config: "TableConfig",
        window_start: datetime,
        window_end: datetime,
        expected_count: int,
    ) -> int:
        """
        DELETE all rows in [window_start, window_end).

        Safety guarantees:
        1. Wrapped in explicit transaction.
        2. After DELETE: SELECT COUNT(*) in same tx must return 0.
        3. Deleted row count from DELETE must match expected_count.
        Any mismatch → ROLLBACK + raise DeleteSafetyError.
        """
        ts_col = table_config.timestamp_column
        schema = table_config.schema_name
        table = table_config.name

        where_clauses = [f"{ts_col} >= $1 AND {ts_col} < $2"]
        params: list[Any] = [window_start, window_end]
        if table_config.extra_filters:
            where_clauses.append(f"({table_config.extra_filters})")
        where = " AND ".join(where_clauses)

        async with self._pool.acquire() as conn:
            await conn.execute(f"SET statement_timeout = {self._statement_timeout_ms}")
            async with conn.transaction():
                status = await conn.execute(
                    f'DELETE FROM "{schema}"."{table}" WHERE {where}',
                    *params,
                )
                # Parse "DELETE N" status string
                deleted_count = int(status.split()[-1])

                # Post-delete verification: no rows should remain in this window
                remaining = await conn.fetchval(
                    f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE {where}',
                    *params,
                )
                remaining = int(remaining)

                if remaining != 0:
                    raise DeleteSafetyError(
                        f"Expected 0 rows after delete for {schema}.{table} "
                        f"[{window_start} → {window_end}], "
                        f"but {remaining} rows remain. Transaction rolled back."
                    )

                if deleted_count != expected_count:
                    raise DeleteSafetyError(
                        f"Deleted {deleted_count} rows from {schema}.{table} "
                        f"[{window_start} → {window_end}], "
                        f"expected {expected_count} (from export). "
                        f"Transaction rolled back."
                    )

        return deleted_count

    async def get_window_bounds(
        self,
        table_config: "TableConfig",
    ) -> tuple[datetime | None, datetime | None]:
        """Return (MIN(ts_col), MAX(ts_col)) for the table. Returns (None, None) if empty."""
        ts_col = table_config.timestamp_column
        schema = table_config.schema_name
        table = table_config.name

        extra = f" WHERE ({table_config.extra_filters})" if table_config.extra_filters else ""
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT MIN("{ts_col}") as min_ts, MAX("{ts_col}") as max_ts '
                f'FROM "{schema}"."{table}"{extra}'
            )
        if row is None or row["min_ts"] is None:
            return None, None

        def _to_utc(v: datetime) -> datetime:
            if v.tzinfo is None:
                return v.replace(tzinfo=timezone.utc)
            return v.astimezone(timezone.utc)

        return _to_utc(row["min_ts"]), _to_utc(row["max_ts"])


def _records_to_arrow(
    records: list[asyncpg.Record],
    column_names: list[str],
    column_oids: list[int],
    logger: Any | None = None,
) -> pa.Table:
    """Convert a list of asyncpg Records to a pyarrow Table."""
    arrays: list[pa.Array] = []
    fields: list[pa.Field] = []

    for i, (col_name, oid) in enumerate(zip(column_names, column_oids)):
        arrow_type = _oid_to_arrow(oid, col_name, logger)
        raw_values = [_coerce_value(r[i], oid) for r in records]
        try:
            array = pa.array(raw_values, type=arrow_type)
        except (pa.ArrowInvalid, pa.ArrowTypeError):
            # Fallback: serialize as string
            if logger:
                logger.warning(
                    "column_type_fallback",
                    column=col_name,
                    oid=oid,
                    fallback="large_utf8",
                )
            array = pa.array([str(v) if v is not None else None for v in raw_values],
                             type=pa.large_utf8())
            arrow_type = pa.large_utf8()
        arrays.append(array)
        fields.append(pa.field(col_name, arrow_type))

    return pa.table(
        {name: arr for name, arr in zip(column_names, arrays)},
        schema=pa.schema(fields),
    )


def _serialize_to_parquet(table: pa.Table, compression: str = "snappy") -> bytes:
    """Serialize a pyarrow Table to Parquet bytes."""
    buf = io.BytesIO()
    pq.write_table(table, buf, compression=compression)
    return buf.getvalue()


def count_parquet_rows(data: bytes) -> int:
    """Count rows in Parquet bytes without loading all data into memory."""
    buf = io.BytesIO(data)
    pf = pq.ParquetFile(buf)
    return pf.metadata.num_rows
