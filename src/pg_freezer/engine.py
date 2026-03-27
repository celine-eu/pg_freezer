"""Main async orchestration engine."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

import asyncpg
import structlog

from pg_freezer.config import AppConfig, TableConfig
from pg_freezer.errors import (
    DeleteSafetyError,
    ExportError,
    PgFreezerError,
    StorageError,
    VerificationError,
)
from pg_freezer.exporter import DBExporter, count_parquet_rows
from pg_freezer.manifest import (
    BatchManifest,
    BatchStatus,
    ManifestStore,
    compute_batch_id,
)
from pg_freezer.notify import get_logger, notify_failure
from pg_freezer.partition import floor_to_partition, generate_windows, partition_path
from pg_freezer.storage import StorageClient

logger = get_logger(__name__)


class FreezerEngine:
    """Orchestrates export, verification, and deletion across configured tables."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._storage: StorageClient | None = None
        self._manifest_store: ManifestStore | None = None
        self._exporter: DBExporter | None = None

    async def _setup(self) -> None:
        cfg = self._config
        self._pool = await asyncpg.create_pool(
            cfg.database.dsn.get_secret_value(),
            min_size=1,
            max_size=cfg.database.pool_size,
        )
        self._storage = StorageClient(cfg.storage)
        self._manifest_store = ManifestStore(self._storage, cfg.storage.bucket)
        timeout_ms = int(cfg.database.statement_timeout.total_seconds() * 1000)
        self._exporter = DBExporter(self._pool, timeout_ms)

    async def _teardown(self) -> None:
        if self._pool:
            await self._pool.close()

    async def run(
        self,
        table_filter: str | None = None,
        dry_run: bool = False,
        force: bool = False,
    ) -> None:
        """Main entry point for `pg-freezer run`."""
        dry_run = dry_run or self._config.global_config.dry_run
        await self._setup()
        try:
            tables = self._config.tables
            if table_filter:
                tables = [t for t in tables if t.name == table_filter]
                if not tables:
                    logger.warning("no_matching_table", table_filter=table_filter)
                    return

            semaphore = asyncio.Semaphore(self._config.global_config.max_table_concurrency)

            async def _run_table(table_cfg: TableConfig) -> None:
                async with semaphore:
                    structlog.contextvars.bind_contextvars(table=table_cfg.qualified_name)
                    try:
                        windows = await self._discover_windows(table_cfg)
                        if not windows:
                            logger.info("no_pending_windows")
                            return
                        logger.info("windows_discovered", count=len(windows))
                        await self._process_table(table_cfg, windows, dry_run, force)
                    except Exception as exc:
                        logger.error("table_processing_failed", error=str(exc), exc_info=True)
                    finally:
                        structlog.contextvars.unbind_contextvars("table")

            tasks = [asyncio.create_task(_run_table(t)) for t in tables]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException):
                    logger.error("task_raised_unhandled", error=str(result), exc_info=result)
        finally:
            await self._teardown()

    async def replay(
        self,
        table_name: str,
        from_dt: datetime,
        to_dt: datetime,
        dry_run: bool = False,
        force: bool = False,
    ) -> None:
        """Replay a specific window range for one table."""
        await self._setup()
        try:
            table_cfg = next(
                (t for t in self._config.tables if t.name == table_name), None
            )
            if not table_cfg:
                logger.error("table_not_found_in_config", table=table_name)
                return

            windows = await self._discover_windows(
                table_cfg, override_range=(from_dt, to_dt)
            )
            logger.info(
                "replay_windows",
                table=table_name,
                from_dt=from_dt.isoformat(),
                to_dt=to_dt.isoformat(),
                count=len(windows),
            )
            await self._process_table(table_cfg, windows, dry_run, force)
        finally:
            await self._teardown()

    async def status(self, table_filter: str | None = None) -> list[dict]:
        """Return status summary for all tables. Prints to stdout."""
        await self._setup()
        try:
            tables = self._config.tables
            if table_filter:
                tables = [t for t in tables if t.name == table_filter]

            results = []
            for table_cfg in tables:
                assert self._manifest_store is not None
                manifests = await self._manifest_store.list_for_table(table_cfg.name)
                counts: dict[str, int] = {}
                for m in manifests:
                    counts[m.status] = counts.get(m.status, 0) + 1
                results.append({
                    "table": table_cfg.qualified_name,
                    "total_batches": len(manifests),
                    "status_counts": counts,
                    "latest_window": manifests[-1].window_end.isoformat() if manifests else None,
                })
            return results
        finally:
            await self._teardown()

    async def _discover_windows(
        self,
        table_config: TableConfig,
        override_range: tuple[datetime, datetime] | None = None,
    ) -> list[tuple[datetime, datetime]]:
        """Determine which windows need processing for a table."""
        assert self._exporter is not None
        assert self._manifest_store is not None

        now = datetime.now(tz=timezone.utc)
        cutoff = now - table_config.lag

        if override_range:
            range_start, range_end = override_range
        else:
            min_ts, max_ts = await self._exporter.get_window_bounds(table_config)
            if min_ts is None:
                logger.info("table_is_empty")
                return []
            range_start = min_ts
            range_end = min(max_ts, cutoff) if max_ts else cutoff

        if range_start >= range_end:
            logger.info(
                "no_windows_in_range",
                range_start=range_start.isoformat(),
                range_end=range_end.isoformat(),
            )
            return []

        all_windows = generate_windows(range_start, range_end, table_config.partition_by)

        # Efficiently filter out DONE windows using just the manifest filename set
        done_batch_ids = await self._manifest_store.list_batch_ids_for_table(
            table_config.name
        )

        # For non-done states, we need the actual manifest to decide resume behavior
        # Filter: include windows not in done set
        pending_windows = []
        for ws, we in all_windows:
            batch_id = compute_batch_id(table_config.name, table_config.schema_name, ws, we)
            if batch_id not in done_batch_ids:
                pending_windows.append((ws, we))

        return pending_windows

    async def _process_table(
        self,
        table_config: TableConfig,
        windows: list[tuple[datetime, datetime]],
        dry_run: bool,
        force: bool = False,
    ) -> None:
        """Process all windows for a single table, sequentially."""
        for window_start, window_end in windows:
            batch_id = compute_batch_id(
                table_config.name, table_config.schema_name, window_start, window_end
            )
            structlog.contextvars.bind_contextvars(
                batch_id=batch_id,
                window_start=window_start.isoformat(),
                window_end=window_end.isoformat(),
            )
            try:
                await self._process_batch(
                    table_config, window_start, window_end, batch_id, dry_run, force
                )
            except Exception as exc:
                logger.error("batch_failed_unhandled", error=str(exc), exc_info=True)
                # Continue to next window
            finally:
                structlog.contextvars.unbind_contextvars("batch_id", "window_start", "window_end")

    async def _process_batch(
        self,
        table_config: TableConfig,
        window_start: datetime,
        window_end: datetime,
        batch_id: str,
        dry_run: bool,
        force: bool,
    ) -> None:
        """Core state machine: load or create manifest, resume from current state."""
        assert self._manifest_store is not None
        assert self._exporter is not None
        assert self._storage is not None
        assert self._config is not None

        store = self._manifest_store
        part_path = partition_path(window_start, table_config.partition_by)
        parquet_key = f"{table_config.name}/{part_path}/data.parquet"
        webhook_url = str(self._config.global_config.notify.webhook_url) \
            if self._config.global_config.notify.webhook_url else None

        # Load or create manifest
        manifest = await store.load(table_config.name, batch_id)
        if manifest is None:
            manifest = BatchManifest(
                batch_id=batch_id,
                table=table_config.name,
                schema_name=table_config.schema_name,
                partition=part_path,
                window_start=window_start,
                window_end=window_end,
                status="PENDING",
                parquet_path=parquet_key,
            )
            await store.save(manifest)

        # Handle terminal and force states
        if manifest.status == "DONE":
            logger.info("batch_already_done")
            return

        if manifest.status == "FAILED":
            if force:
                logger.info("batch_force_reset")
                manifest = await store.force_reset(manifest)
            else:
                logger.warning(
                    "batch_failed_skipping",
                    error=manifest.error,
                    hint="Use --force to retry",
                )
                return

        # Handle crash recovery for DELETING state
        if manifest.status == "DELETING":
            remaining = await self._exporter.count_window(
                table_config, window_start, window_end
            )
            if remaining == 0:
                logger.info("batch_delete_already_complete_on_resume")
                if not dry_run:
                    manifest = await store.transition(
                        manifest, "DONE",
                        deleted_at=datetime.now(tz=timezone.utc)
                    )
                return
            else:
                err = (
                    f"Crash detected during DELETING: {remaining} rows still in PG. "
                    "Manual investigation required."
                )
                manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(
                    webhook_url,
                    table_config.name, batch_id,
                    window_start, window_end, err, logger
                )
                return

        # --- EXPORTING ---
        if manifest.status in ("PENDING", "EXPORTING"):
            logger.info("batch_exporting")
            if not dry_run:
                manifest = await store.transition(manifest, "EXPORTING")
            try:
                parquet_bytes, row_count = await self._exporter.export_window(
                    table_config, window_start, window_end, logger
                )
            except ExportError as e:
                err = str(e)
                if not dry_run:
                    manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(webhook_url, table_config.name, batch_id,
                                     window_start, window_end, err, logger)
                return

            logger.info("batch_exported", row_count=row_count,
                        size_bytes=len(parquet_bytes))

            if dry_run:
                logger.info("dry_run_skip_upload", row_count=row_count)
                return

            # --- UPLOADING ---
            try:
                await self._storage.upload_bytes(parquet_key, parquet_bytes)
            except StorageError as e:
                err = str(e)
                manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(webhook_url, table_config.name, batch_id,
                                     window_start, window_end, err, logger)
                return

            manifest = await store.transition(
                manifest, "UPLOADED",
                row_count=row_count,
                parquet_path=parquet_key,
                parquet_size_bytes=len(parquet_bytes),
                exported_at=datetime.now(tz=timezone.utc),
            )

        # --- VERIFICATION ---
        if manifest.status == "UPLOADED":
            logger.info("batch_verifying")
            try:
                stored_bytes = await self._storage.download_bytes(parquet_key)
                stored_count = count_parquet_rows(stored_bytes)
            except (StorageError, Exception) as e:
                err = f"Verification read failed: {e}"
                manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(webhook_url, table_config.name, batch_id,
                                     window_start, window_end, err, logger)
                return

            if stored_count != manifest.row_count:
                err = (
                    f"Verification failed: S3 Parquet has {stored_count} rows, "
                    f"expected {manifest.row_count}. Deletion blocked."
                )
                manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(webhook_url, table_config.name, batch_id,
                                     window_start, window_end, err, logger)
                return

            logger.info("batch_verified", row_count=stored_count)
            manifest = await store.transition(
                manifest, "VERIFIED",
                verified_at=datetime.now(tz=timezone.utc),
            )

        # --- DELETION ---
        if manifest.status == "VERIFIED":
            if dry_run:
                logger.info("dry_run_skip_delete", row_count=manifest.row_count)
                return

            logger.info("batch_deleting", row_count=manifest.row_count)
            manifest = await store.transition(manifest, "DELETING")
            try:
                deleted = await self._exporter.delete_window(
                    table_config,
                    window_start,
                    window_end,
                    manifest.row_count,  # type: ignore[arg-type]
                )
            except DeleteSafetyError as e:
                err = str(e)
                manifest = await store.transition(manifest, "FAILED", error=err)
                await notify_failure(webhook_url, table_config.name, batch_id,
                                     window_start, window_end, err, logger)
                return

            manifest = await store.transition(
                manifest, "DONE",
                deleted_at=datetime.now(tz=timezone.utc),
            )
            logger.info("batch_done", deleted_rows=deleted)
