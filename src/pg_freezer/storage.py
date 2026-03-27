"""fsspec/s3fs wrapper for S3-compatible storage."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

import s3fs

from pg_freezer.errors import StorageError

if TYPE_CHECKING:
    from pg_freezer.config import StorageConfig


class StorageClient:
    """
    Single s3fs.S3FileSystem instance per run.
    All blocking s3fs calls are offloaded to a thread pool via asyncio.to_thread()
    so they never block the event loop.
    Supports AWS S3, MinIO, and any S3-compatible backend via endpoint_url.
    """

    def __init__(self, config: "StorageConfig") -> None:
        self._config = config
        self._bucket = config.bucket
        self._fs: s3fs.S3FileSystem | None = None

    def _get_fs(self) -> s3fs.S3FileSystem:
        """Lazy-initialize and cache a single S3FileSystem instance."""
        if self._fs is not None:
            return self._fs

        cfg = self._config
        kwargs: dict = {}

        if cfg.access_key and cfg.secret_key:
            kwargs["key"] = cfg.access_key.get_secret_value()
            kwargs["secret"] = cfg.secret_key.get_secret_value()

        if cfg.endpoint_url:
            kwargs["client_kwargs"] = {"endpoint_url": cfg.endpoint_url}
            if cfg.endpoint_url.startswith("http://"):
                kwargs["use_ssl"] = False

        self._fs = s3fs.S3FileSystem(asynchronous=False, **kwargs)
        return self._fs

    def _full_key(self, key: str) -> str:
        return f"{self._bucket}/{key}"

    async def upload_bytes(self, key: str, data: bytes) -> int:
        """Upload bytes to s3://{bucket}/{key}. Returns bytes written."""
        full = self._full_key(key)
        try:
            fs = self._get_fs()

            def _upload() -> None:
                with fs.open(full, "wb") as f:
                    f.write(data)

            await asyncio.to_thread(_upload)
            return len(data)
        except Exception as e:
            raise StorageError(f"Upload failed for {full}: {e}") from e

    async def download_bytes(self, key: str) -> bytes:
        """Download and return full contents of s3://{bucket}/{key}."""
        full = self._full_key(key)
        try:
            fs = self._get_fs()

            def _download() -> bytes:
                if not fs.exists(full):
                    raise FileNotFoundError(f"Key not found: {full}")
                with fs.open(full, "rb") as f:
                    return f.read()

            return await asyncio.to_thread(_download)
        except FileNotFoundError:
            raise
        except Exception as e:
            raise StorageError(f"Download failed for {full}: {e}") from e

    async def exists(self, key: str) -> bool:
        """Return True if s3://{bucket}/{key} exists."""
        full = self._full_key(key)
        try:
            fs = self._get_fs()
            return await asyncio.to_thread(fs.exists, full)
        except Exception as e:
            raise StorageError(f"Exists check failed for {key}: {e}") from e

    async def file_size(self, key: str) -> int:
        """Return size in bytes of s3://{bucket}/{key}."""
        full = self._full_key(key)
        try:
            fs = self._get_fs()
            info = await asyncio.to_thread(fs.info, full)
            return info["size"]
        except Exception as e:
            raise StorageError(f"file_size failed for {full}: {e}") from e

    async def list_prefix(self, prefix: str) -> list[str]:
        """
        List all keys under s3://{bucket}/{prefix}.
        Returns keys relative to bucket root (without bucket name).
        """
        full_prefix = self._full_key(prefix)
        bucket_prefix = f"{self._bucket}/"
        try:
            fs = self._get_fs()

            def _list() -> list[str]:
                if not fs.exists(full_prefix):
                    return []
                paths = fs.glob(f"{full_prefix}**")
                return [
                    p[len(bucket_prefix):]
                    for p in paths
                    if not fs.isdir(p)
                ]

            return await asyncio.to_thread(_list)
        except Exception as e:
            raise StorageError(f"list_prefix failed for {prefix}: {e}") from e

    async def delete(self, key: str) -> None:
        """Delete a single object."""
        full = self._full_key(key)
        try:
            fs = self._get_fs()
            await asyncio.to_thread(fs.rm, full)
        except Exception as e:
            raise StorageError(f"Delete failed for {full}: {e}") from e
