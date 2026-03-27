"""Batch manifest model, state machine, and S3-backed store."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, field_serializer, field_validator

from pg_freezer.errors import InvalidTransitionError

if TYPE_CHECKING:
    from pg_freezer.storage import StorageClient

BatchStatus = Literal[
    "PENDING",
    "EXPORTING",
    "UPLOADED",
    "VERIFIED",
    "DELETING",
    "DONE",
    "FAILED",
]

VALID_TRANSITIONS: dict[BatchStatus, set[BatchStatus]] = {
    "PENDING": {"EXPORTING"},
    "EXPORTING": {"UPLOADED", "FAILED"},
    "UPLOADED": {"VERIFIED", "FAILED"},
    "VERIFIED": {"DELETING", "FAILED"},
    "DELETING": {"DONE", "FAILED"},
    "DONE": set(),
    "FAILED": set(),
}


def _utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _ensure_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class BatchManifest(BaseModel):
    version: int = 1
    batch_id: str
    table: str
    schema_name: str
    partition: str
    window_start: datetime
    window_end: datetime
    status: BatchStatus
    row_count: int | None = None
    parquet_path: str | None = None
    parquet_size_bytes: int | None = None
    exported_at: datetime | None = None
    verified_at: datetime | None = None
    deleted_at: datetime | None = None
    error: str | None = None

    @field_validator("window_start", "window_end", "exported_at", "verified_at", "deleted_at", mode="before")
    @classmethod
    def _ensure_aware(cls, v: Any) -> Any:
        if isinstance(v, datetime):
            return _ensure_utc(v)
        return v

    @field_serializer("window_start", "window_end", "exported_at", "verified_at", "deleted_at")
    def _serialize_dt(self, v: datetime | None) -> str | None:
        if v is None:
            return None
        return _ensure_utc(v).isoformat()

    def model_dump_json_str(self) -> str:
        return self.model_dump_json(indent=2)

    @classmethod
    def model_load_json_str(cls, data: str) -> "BatchManifest":
        return cls.model_validate_json(data)


def compute_batch_id(
    table: str,
    schema_name: str,
    window_start: datetime,
    window_end: datetime,
) -> str:
    """
    Deterministic, short batch ID from SHA-256 of canonical key.
    Inputs are UTC-normalized before hashing.
    Returns first 16 hex characters of SHA-256 digest.
    """
    ws = _ensure_utc(window_start).isoformat()
    we = _ensure_utc(window_end).isoformat()
    key = f"{schema_name}.{table}|{ws}|{we}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def manifest_s3_key(table: str, batch_id: str) -> str:
    """S3 key for a batch manifest file."""
    return f"_manifests/{table}/{batch_id}.json"


class ManifestStore:
    """Read and write batch manifests via the storage layer."""

    def __init__(self, storage: "StorageClient", bucket: str) -> None:
        self._storage = storage
        self._bucket = bucket

    async def load(self, table: str, batch_id: str) -> BatchManifest | None:
        """Return manifest if it exists, None otherwise."""
        key = manifest_s3_key(table, batch_id)
        try:
            data = await self._storage.download_bytes(key)
            return BatchManifest.model_load_json_str(data.decode())
        except FileNotFoundError:
            return None

    async def save(self, manifest: BatchManifest) -> None:
        """Write manifest as JSON to S3. Overwrites any existing."""
        key = manifest_s3_key(manifest.table, manifest.batch_id)
        data = manifest.model_dump_json_str().encode()
        await self._storage.upload_bytes(key, data)

    async def list_batch_ids_for_table(self, table: str) -> set[str]:
        """
        List all batch IDs for a table by scanning the manifest prefix.
        Returns a set of batch_id strings (derived from filenames).
        Avoids downloading manifest content — O(1) per manifest.
        """
        prefix = f"_manifests/{table}/"
        keys = await self._storage.list_prefix(prefix)
        batch_ids: set[str] = set()
        for key in keys:
            filename = key.split("/")[-1]
            if filename.endswith(".json"):
                batch_ids.add(filename[: -len(".json")])
        return batch_ids

    async def list_for_table(self, table: str) -> list[BatchManifest]:
        """Download and parse all manifests for a table. Use sparingly."""
        prefix = f"_manifests/{table}/"
        keys = await self._storage.list_prefix(prefix)
        manifests: list[BatchManifest] = []
        for key in keys:
            if not key.endswith(".json"):
                continue
            try:
                data = await self._storage.download_bytes(key)
                manifests.append(BatchManifest.model_load_json_str(data.decode()))
            except Exception:
                pass  # corrupt manifest — skip, will surface in status command
        return sorted(manifests, key=lambda m: m.window_start)

    async def transition(
        self,
        manifest: BatchManifest,
        new_status: BatchStatus,
        **updates: Any,
    ) -> BatchManifest:
        """
        Apply a validated state machine transition.
        Updates any additional fields provided as keyword arguments.
        Saves the updated manifest and returns it.
        Raises InvalidTransitionError if the transition is not allowed.
        """
        allowed = VALID_TRANSITIONS.get(manifest.status, set())
        if new_status not in allowed:
            raise InvalidTransitionError(
                f"Cannot transition {manifest.table}/{manifest.batch_id} "
                f"from {manifest.status!r} to {new_status!r}. "
                f"Allowed: {allowed or {'(terminal state)'}}"
            )
        updated = manifest.model_copy(
            update={"status": new_status, **updates}
        )
        await self.save(updated)
        return updated

    async def force_reset(self, manifest: BatchManifest) -> BatchManifest:
        """
        Reset a FAILED manifest to PENDING (used with --force flag).
        Clears error field.
        """
        if manifest.status != "FAILED":
            raise InvalidTransitionError(
                f"force_reset only applies to FAILED manifests, "
                f"got {manifest.status!r}"
            )
        reset = manifest.model_copy(
            update={
                "status": "PENDING",
                "error": None,
                "exported_at": None,
                "verified_at": None,
                "deleted_at": None,
                "row_count": None,
                "parquet_path": None,
                "parquet_size_bytes": None,
            }
        )
        await self.save(reset)
        return reset
