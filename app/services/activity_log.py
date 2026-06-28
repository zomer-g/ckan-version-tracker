"""Best-effort writer for the append-only activity log.

Every call opens its OWN short-lived DB session and commits independently of
the caller's transaction. That keeps logging fully decoupled: a log write can
never poison or fail the caller's commit (important on the worker failure
paths, where the failed-status commit must succeed), and a "failed"/"rejected"
event is still recorded even if the caller later rolls back. Any error here is
swallowed — logging is observability, never load-bearing.

See app/models/activity_log.py for the schema and event vocabulary.
"""
from __future__ import annotations

import logging
import uuid

from app.database import async_session
from app.models.activity_log import ActivityLog

logger = logging.getLogger(__name__)


async def log_event(
    *,
    event: str,
    dataset=None,
    dataset_id: str | uuid.UUID | None = None,
    dataset_title: str | None = None,
    source_type: str | None = None,
    status: str = "info",
    message: str | None = None,
    detail: str | None = None,
    actor: str | None = "system",
) -> None:
    """Append one event row. Pass a TrackedDataset via ``dataset`` to fill
    id/title/source_type automatically, or supply them explicitly. ``status``
    is "ok" | "error" | "info"; ``detail`` carries the full error text."""
    try:
        ds_id = dataset_id
        title = dataset_title
        stype = source_type
        if dataset is not None:
            ds_id = ds_id or getattr(dataset, "id", None)
            title = title if title is not None else getattr(dataset, "title", None)
            stype = stype if stype is not None else getattr(dataset, "source_type", None)
        if isinstance(ds_id, str):
            try:
                ds_id = uuid.UUID(ds_id)
            except ValueError:
                ds_id = None

        entry = ActivityLog(
            tracked_dataset_id=ds_id,
            dataset_title=(title or "")[:1000] or None,
            source_type=(stype or None),
            event=event[:40],
            status=status[:20],
            message=(message or None) and message[:500],
            detail=detail or None,
            actor=(actor or None) and actor[:255],
        )
        async with async_session() as s:
            s.add(entry)
            await s.commit()
    except Exception as e:  # noqa: BLE001 — logging must never break the caller
        logger.warning("activity_log.log_event(%s) failed: %s", event, e)
