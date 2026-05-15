"""Tests for FreezerEngine._process_batch state machine, focused on DELETING recovery."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from pg_freezer.config import TableConfig
from pg_freezer.engine import FreezerEngine
from pg_freezer.errors import DeleteSafetyError
from pg_freezer.manifest import BatchManifest, compute_batch_id
from pg_freezer.partition import partition_path

UTC = timezone.utc
WINDOW_START = datetime(2026, 2, 1, tzinfo=UTC)
WINDOW_END = datetime(2026, 3, 1, tzinfo=UTC)

TABLE_CFG = TableConfig(
    name="mqtt_messages",
    schema_name="raw",
    timestamp_column="ts",
    partition_by="monthly",
)

BATCH_ID = compute_batch_id(TABLE_CFG.name, TABLE_CFG.schema_name, WINDOW_START, WINDOW_END)
_PART_PATH = partition_path(WINDOW_START, TABLE_CFG.partition_by)
PARQUET_KEY = f"{TABLE_CFG.name}/{_PART_PATH}/data.parquet"


def _make_manifest(status: str, row_count: int = 1_000) -> BatchManifest:
    return BatchManifest(
        batch_id=BATCH_ID,
        table=TABLE_CFG.name,
        schema_name=TABLE_CFG.schema_name,
        partition=_PART_PATH,
        window_start=WINDOW_START,
        window_end=WINDOW_END,
        status=status,
        row_count=row_count,
        parquet_path=PARQUET_KEY,
    )


def _make_engine(manifest: BatchManifest, count_return: int = 0, delete_return: int | None = None):
    """Construct a FreezerEngine with all I/O replaced by mocks."""
    engine = object.__new__(FreezerEngine)

    cfg = MagicMock()
    cfg.global_config.notify.webhook_url = None
    engine._config = cfg

    def _transition(m, new_status, **kw):
        return m.model_copy(update={"status": new_status, **kw})

    store = MagicMock()
    store.load = AsyncMock(return_value=manifest)
    store.save = AsyncMock()
    store.transition = AsyncMock(side_effect=_transition)
    store.force_reset = AsyncMock(
        side_effect=lambda m: m.model_copy(update={"status": "PENDING", "error": None})
    )
    engine._manifest_store = store

    exporter = MagicMock()
    exporter.count_window = AsyncMock(return_value=count_return)
    exporter.export_window = AsyncMock(return_value=(b"fake-parquet", manifest.row_count or 100))
    exporter.delete_window = AsyncMock(return_value=delete_return if delete_return is not None else manifest.row_count)
    engine._exporter = exporter

    storage = MagicMock()
    storage.upload_bytes = AsyncMock()
    storage.download_bytes = AsyncMock(return_value=b"fake-parquet")
    engine._storage = storage

    return engine, store, exporter


def _transitions(store) -> list[str]:
    """Extract the new_status arg from every store.transition() call."""
    return [call.args[1] for call in store.transition.call_args_list]


async def _run(engine: FreezerEngine, **kwargs):
    defaults = dict(dry_run=False, force=False)
    defaults.update(kwargs)
    await engine._process_batch(TABLE_CFG, WINDOW_START, WINDOW_END, BATCH_ID, **defaults)


class TestDeletingCrashRecovery:
    async def test_zero_remaining_marks_done_without_calling_delete(self):
        """DELETING + 0 rows remaining → DONE immediately, delete_window never called."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=0)

        await _run(engine)

        exporter.delete_window.assert_not_called()
        assert "DONE" in _transitions(store)

    async def test_rows_remaining_calls_delete_window(self):
        """DELETING + rows still in PG → delete_window is called to finish the job."""
        manifest = _make_manifest("DELETING", row_count=796_934)
        engine, store, exporter = _make_engine(manifest, count_return=796_934, delete_return=796_934)

        await _run(engine)

        exporter.delete_window.assert_called_once()

    async def test_rows_remaining_ends_in_done(self):
        """DELETING resume must reach DONE after successful delete."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=500)

        await _run(engine)

        assert "DONE" in _transitions(store)

    async def test_resume_skips_export_and_upload(self):
        """Resuming DELETING must not re-export or re-upload — data is already in S3."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=500)

        await _run(engine)

        exporter.export_window.assert_not_called()
        engine._storage.upload_bytes.assert_not_called()

    async def test_resume_does_not_transition_to_deleting_again(self):
        """DELETING resume must not emit a second DELETING transition."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=100)

        await _run(engine)

        assert "DELETING" not in _transitions(store)

    async def test_resume_delete_failure_marks_failed(self):
        """If delete_window raises during DELETING resume → FAILED, not DONE."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=500)
        exporter.delete_window = AsyncMock(side_effect=DeleteSafetyError("5 rows remain"))

        await _run(engine)

        assert "FAILED" in _transitions(store)
        assert "DONE" not in _transitions(store)

    async def test_zero_remaining_dry_run_skips_manifest_write(self):
        """dry_run + DELETING + 0 remaining → no manifest write (read-only run)."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=0)

        await _run(engine, dry_run=True)

        exporter.delete_window.assert_not_called()
        store.transition.assert_not_called()

    async def test_rows_remaining_dry_run_skips_delete(self):
        """dry_run + DELETING + rows remaining → deletion is skipped, no DONE or FAILED."""
        manifest = _make_manifest("DELETING")
        engine, store, exporter = _make_engine(manifest, count_return=500)

        await _run(engine, dry_run=True)

        exporter.delete_window.assert_not_called()
        assert "DONE" not in _transitions(store)
        assert "FAILED" not in _transitions(store)


class TestVerifiedDeletionPath:
    async def test_verified_transitions_deleting_then_done(self):
        """Normal VERIFIED → DELETING → delete_window → DONE."""
        manifest = _make_manifest("VERIFIED")
        engine, store, exporter = _make_engine(manifest)

        await _run(engine)

        targets = _transitions(store)
        assert "DELETING" in targets
        assert "DONE" in targets
        assert targets.index("DELETING") < targets.index("DONE")

    async def test_verified_calls_delete_window(self):
        manifest = _make_manifest("VERIFIED")
        engine, store, exporter = _make_engine(manifest)

        await _run(engine)

        exporter.delete_window.assert_called_once_with(
            TABLE_CFG, WINDOW_START, WINDOW_END, manifest.row_count
        )

    async def test_verified_delete_failure_marks_failed(self):
        """delete_window raises from VERIFIED state → FAILED."""
        manifest = _make_manifest("VERIFIED")
        engine, store, exporter = _make_engine(manifest)
        exporter.delete_window = AsyncMock(side_effect=DeleteSafetyError("mismatch"))

        await _run(engine)

        assert "FAILED" in _transitions(store)
        assert "DONE" not in _transitions(store)

    async def test_verified_dry_run_skips_delete(self):
        """dry_run prevents deletion and leaves no DONE/FAILED transitions."""
        manifest = _make_manifest("VERIFIED")
        engine, store, exporter = _make_engine(manifest)

        await _run(engine, dry_run=True)

        exporter.delete_window.assert_not_called()
        assert "DONE" not in _transitions(store)


class TestAlreadyDoneAndFailed:
    async def test_done_is_a_no_op(self):
        manifest = _make_manifest("DONE")
        engine, store, exporter = _make_engine(manifest)

        await _run(engine)

        exporter.delete_window.assert_not_called()
        store.transition.assert_not_called()

    async def test_failed_without_force_skips(self):
        manifest = _make_manifest("FAILED")
        engine, store, exporter = _make_engine(manifest)

        await _run(engine, force=False)

        exporter.delete_window.assert_not_called()
        store.transition.assert_not_called()

    async def test_failed_with_force_resets_and_continues(self):
        """--force on a FAILED manifest resets it to PENDING and re-runs."""
        manifest = _make_manifest("FAILED", row_count=None)
        engine, store, exporter = _make_engine(manifest)
        # After force_reset, the manifest is PENDING; export_window returns bytes + count
        store.force_reset = AsyncMock(
            return_value=_make_manifest("PENDING", row_count=None)
        )
        # exporter.export_window already set to return (b"fake-parquet", 100) in _make_engine

        await _run(engine, force=True)

        store.force_reset.assert_called_once()
        exporter.export_window.assert_called_once()
