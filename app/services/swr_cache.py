"""A process-local stale-while-revalidate cache for one expensive value.

For values that are expensive to build (seconds of SQL), change slowly, and
are read on hot public pages: the home-page catalog, the public dataset list.

* Cold (nothing cached yet): the first caller builds inline, under a lock, so
  N concurrent first requests trigger one build, not N.
* Fresh: served from memory.
* Stale (older than the TTL): still served from memory, and ONE background task
  rebuilds it with its own database session. No visitor waits on the rebuild.
* A failed background rebuild keeps the stale copy and logs; the next stale read
  tries again.

Process-local on purpose: the app runs as one uvicorn process (the poll
scheduler lives in it, so it cannot simply be scaled to several workers).
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class SWRCache:
    def __init__(self, name: str, build: Callable[[AsyncSession], Awaitable[Any]],
                 ttl_seconds: float = 60.0) -> None:
        self.name = name
        self._build = build
        self.ttl_seconds = ttl_seconds
        self.value: Any = None
        self.built_at: float = 0.0
        self._lock = asyncio.Lock()
        self._refresh_task: asyncio.Task | None = None

    def clear(self) -> None:
        self.value, self.built_at = None, 0.0

    async def get(self, db: AsyncSession) -> Any:
        if self.value is not None:
            if time.monotonic() - self.built_at >= self.ttl_seconds and (
                self._refresh_task is None or self._refresh_task.done()
            ):
                self._refresh_task = asyncio.create_task(self._refresh())
            return self.value
        async with self._lock:
            if self.value is None:
                self.value, self.built_at = await self._build(db), time.monotonic()
            return self.value

    async def _refresh(self) -> None:
        from app.database import async_session

        try:
            async with async_session() as db:
                built = await self._build(db)
            self.value, self.built_at = built, time.monotonic()
        except Exception:  # noqa: BLE001 — keep serving the stale copy
            logger.warning("%s: background refresh failed", self.name, exc_info=True)
