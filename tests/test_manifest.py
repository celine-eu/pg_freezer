"""Tests for manifest model, state machine, and ManifestStore."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pg_freezer.errors import InvalidTransitionError
from pg_freezer.manifest import (
    VALID_TRANSITIONS,
    BatchManifest,
    ManifestStore,
    compute_batch_id,
    manifest_s3_key,
)


def dt(*args) -> datetime:
    return datetime(*args, tzinfo=timezone.utc)


def make_manifest(**kwargs) -> BatchManifest:
    defaults = dict(
        batch_id="abc123",
        table="readings",
        schema_name="public",
        partition="year=2024/month=01/day=15",
        window_start=dt(2024, 1, 15),
        window_end=dt(2024, 1, 16),
        status="PENDING",
    )
    defaults.update(kwargs)
    return BatchManifest(**defaults)


class TestComputeBatchId:
    def test_deterministic(self):
        id1 = compute_batch_id("t", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        id2 = compute_batch_id("t", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        assert id1 == id2

    def test_different_tables(self):
        id1 = compute_batch_id("t1", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        id2 = compute_batch_id("t2", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        assert id1 != id2

    def test_different_windows(self):
        id1 = compute_batch_id("t", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        id2 = compute_batch_id("t", "s", dt(2024, 1, 2), dt(2024, 1, 3))
        assert id1 != id2

    def test_different_schemas(self):
        id1 = compute_batch_id("t", "schema_a", dt(2024, 1, 1), dt(2024, 1, 2))
        id2 = compute_batch_id("t", "schema_b", dt(2024, 1, 1), dt(2024, 1, 2))
        assert id1 != id2

    def test_length(self):
        batch_id = compute_batch_id("t", "s", dt(2024, 1, 1), dt(2024, 1, 2))
        assert len(batch_id) == 16

    def test_utc_normalization(self):
        """Naive and UTC-aware datetimes with same value produce same batch_id."""
        naive = datetime(2024, 1, 1, 0, 0, 0)
        aware = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        id_naive = compute_batch_id("t", "s", naive, datetime(2024, 1, 2))
        id_aware = compute_batch_id("t", "s", aware, dt(2024, 1, 2))
        assert id_naive == id_aware


class TestManifestS3Key:
    def test_format(self):
        key = manifest_s3_key("my_table", "abc123def456")
        assert key == "_manifests/my_table/abc123def456.json"


class TestBatchManifest:
    def test_json_round_trip(self):
        m = make_manifest(row_count=500, status="DONE")
        serialized = m.model_dump_json_str()
        restored = BatchManifest.model_load_json_str(serialized)
        assert restored.batch_id == m.batch_id
        assert restored.row_count == 500
        assert restored.status == "DONE"

    def test_window_start_serialized_with_tz(self):
        m = make_manifest()
        data = m.model_dump_json_str()
        assert "+00:00" in data or "Z" in data or "00:00:00" in data

    def test_naive_datetime_normalized(self):
        naive_start = datetime(2024, 1, 15)
        m = BatchManifest(
            batch_id="x",
            table="t",
            schema_name="s",
            partition="year=2024/month=01/day=15",
            window_start=naive_start,
            window_end=dt(2024, 1, 16),
            status="PENDING",
        )
        assert m.window_start.tzinfo is not None


class TestValidTransitions:
    def test_all_terminal_states_have_no_transitions(self):
        assert VALID_TRANSITIONS["DONE"] == set()
        assert VALID_TRANSITIONS["FAILED"] == set()

    @pytest.mark.parametrize("from_status,to_status", [
        ("PENDING", "EXPORTING"),
        ("EXPORTING", "UPLOADED"),
        ("EXPORTING", "FAILED"),
        ("UPLOADED", "VERIFIED"),
        ("UPLOADED", "FAILED"),
        ("VERIFIED", "DELETING"),
        ("VERIFIED", "FAILED"),
        ("DELETING", "DONE"),
        ("DELETING", "FAILED"),
    ])
    def test_valid(self, from_status, to_status):
        assert to_status in VALID_TRANSITIONS[from_status]

    @pytest.mark.parametrize("from_status,to_status", [
        ("DONE", "PENDING"),
        ("FAILED", "DONE"),
        ("PENDING", "DONE"),
        ("UPLOADED", "DELETING"),
    ])
    def test_invalid(self, from_status, to_status):
        assert to_status not in VALID_TRANSITIONS[from_status]


class TestManifestStore:
    """Uses an in-memory dict as a mock StorageClient."""

    def _make_store(self):
        store_data: dict[str, bytes] = {}

        storage = MagicMock()

        async def upload(key, data):
            store_data[key] = data

        async def download(key):
            if key not in store_data:
                raise FileNotFoundError(key)
            return store_data[key]

        async def list_prefix(prefix):
            return [k for k in store_data if k.startswith(prefix)]

        storage.upload_bytes = upload
        storage.download_bytes = download
        storage.list_prefix = list_prefix

        ms = ManifestStore(storage, "test-bucket")
        return ms, store_data

    @pytest.mark.asyncio
    async def test_save_and_load(self):
        ms, _ = self._make_store()
        m = make_manifest()
        await ms.save(m)
        loaded = await ms.load("readings", "abc123")
        assert loaded is not None
        assert loaded.batch_id == "abc123"

    @pytest.mark.asyncio
    async def test_load_nonexistent(self):
        ms, _ = self._make_store()
        result = await ms.load("readings", "nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_transition_valid(self):
        ms, _ = self._make_store()
        m = make_manifest(status="PENDING")
        await ms.save(m)
        updated = await ms.transition(m, "EXPORTING")
        assert updated.status == "EXPORTING"

    @pytest.mark.asyncio
    async def test_transition_invalid_raises(self):
        ms, _ = self._make_store()
        m = make_manifest(status="DONE")
        with pytest.raises(InvalidTransitionError):
            await ms.transition(m, "PENDING")

    @pytest.mark.asyncio
    async def test_transition_updates_fields(self):
        ms, _ = self._make_store()
        m = make_manifest(status="EXPORTING")
        updated = await ms.transition(
            m, "UPLOADED",
            row_count=1000,
            parquet_size_bytes=5000,
        )
        assert updated.row_count == 1000
        assert updated.parquet_size_bytes == 5000

    @pytest.mark.asyncio
    async def test_list_batch_ids(self):
        ms, _ = self._make_store()
        m1 = make_manifest(batch_id="aaa111")
        m2 = make_manifest(batch_id="bbb222")
        await ms.save(m1)
        await ms.save(m2)
        ids = await ms.list_batch_ids_for_table("readings")
        assert "aaa111" in ids
        assert "bbb222" in ids

    @pytest.mark.asyncio
    async def test_force_reset(self):
        ms, _ = self._make_store()
        m = make_manifest(status="FAILED", error="something went wrong")
        await ms.save(m)
        reset = await ms.force_reset(m)
        assert reset.status == "PENDING"
        assert reset.error is None

    @pytest.mark.asyncio
    async def test_force_reset_non_failed_raises(self):
        ms, _ = self._make_store()
        m = make_manifest(status="DONE")
        with pytest.raises(InvalidTransitionError):
            await ms.force_reset(m)
