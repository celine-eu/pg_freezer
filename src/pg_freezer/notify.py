"""Structured logging configuration and failure webhook notifications."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from typing import Any

import httpx
import structlog


def configure_logging(json_logs: bool | None = None) -> None:
    """
    Configure structlog.
    Auto-detects output mode: JSON for non-TTY/production, ConsoleRenderer for TTY.
    Override with json_logs=True/False or LOG_FORMAT=json|console env var.
    """
    if json_logs is None:
        env_fmt = os.environ.get("LOG_FORMAT", "").lower()
        if env_fmt == "json":
            json_logs = True
        elif env_fmt == "console":
            json_logs = False
        else:
            json_logs = not sys.stderr.isatty()

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if json_logs:
        renderer = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=shared_processors + [renderer],
        wrapper_class=structlog.make_filtering_bound_logger(0),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> structlog.BoundLogger:
    return structlog.get_logger(name)


async def notify_failure(
    webhook_url: str | None,
    table: str,
    batch_id: str,
    window_start: datetime,
    window_end: datetime,
    error: str,
    logger: structlog.BoundLogger,
) -> None:
    """
    Log failure at ERROR level and optionally POST to a webhook.
    Webhook failures are logged as warnings — they never block the main flow.
    """
    logger.error(
        "batch_failed",
        table=table,
        batch_id=batch_id,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        error=error,
    )

    if not webhook_url:
        return

    payload = {
        "service": "pg_freezer",
        "event": "batch_failed",
        "table": table,
        "batch_id": batch_id,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "error": error,
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(str(webhook_url), json=payload)
            resp.raise_for_status()
    except Exception as exc:
        logger.warning(
            "webhook_notification_failed",
            webhook_url=webhook_url,
            error=str(exc),
        )
