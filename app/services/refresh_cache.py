"""A cache whose REFRESH may be slow but whose READER never is.

Extracted when a second caller needed it, because the subtle half is easy to
get wrong twice in different ways.

The distinction it exists to keep: how long a fetch takes and how long a request
waits for it are different numbers. Conflating them is how a page ends up
serving a third party's cold start — /api/deep-search/sources gated the שאלות
לעם search button on a live TAG-IT call, and a spun-down upstream left the
button dead with a spinner cursor while tagit_mcp patiently retried for up to
100s.

So: one shared in-flight refresh, and readers that watch it for a bounded moment
and then answer with whatever they have. ``asyncio.shield`` keeps a timed-out
reader from cancelling the fetch, so the work still lands and the NEXT reader
finds it cached — a shield that drops the work would be pure waste.

Decorative metadata must never be on the critical path of a page load.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Awaitable, Callable

logger = logging.getLogger(__name__)


class BoundedRefreshCache:
    """``await cache.get(max_wait=...)`` — always fast, sometimes stale.

    ``fetch`` is an async callable returning the value. A falsy result is
    treated as "nothing learned" and does NOT overwrite what we already hold:
    an upstream that answers with an empty list is far more likely to be having
    a bad day than to have genuinely lost all its content.
    """

    def __init__(self, name: str, fetch: Callable[[], Awaitable[Any]], *,
                 ttl_seconds: float, empty: Any = None,
                 default_max_wait: float = 1.0):
        self.name = name
        self._fetch = fetch
        self._ttl = float(ttl_seconds)
        self._empty = empty if empty is not None else {}
        self.default_max_wait = float(default_max_wait)
        self._value: Any = self._empty
        self._at: float = 0.0
        self._task: asyncio.Task | None = None

    # ── state ──────────────────────────────────────────────────────────────
    @property
    def value(self) -> Any:
        """Whatever is held right now, without touching the network."""
        return self._value

    def is_fresh(self) -> bool:
        return bool(self._value) and (time.time() - self._at) < self._ttl

    def reset(self) -> None:
        self._value, self._at, self._task = self._empty, 0.0, None

    # ── refresh ────────────────────────────────────────────────────────────
    async def _run(self) -> None:
        try:
            got = await self._fetch()
        except Exception:  # noqa: BLE001 — a label is never worth an error page
            logger.info("%s: refresh failed; serving what we have",
                        self.name, exc_info=True)
            return
        if got:
            self._value, self._at = got, time.time()

    def ensure_refresh(self) -> asyncio.Task:
        """One in-flight refresh at a time, shared by every waiter."""
        if self._task is None or self._task.done():
            self._task = asyncio.get_running_loop().create_task(self._run())
        return self._task

    async def get(self, max_wait: float | None = -1.0) -> Any:
        """The value, waiting at most ``max_wait`` seconds for a refresh.

        ``max_wait=None`` waits indefinitely — correct only off a request path.
        The default uses this cache's own budget.
        """
        if self.is_fresh():
            return self._value
        if max_wait is not None and max_wait < 0:
            max_wait = self.default_max_wait
        task = self.ensure_refresh()
        try:
            if max_wait is None:
                await asyncio.shield(task)
            else:
                await asyncio.wait_for(asyncio.shield(task), max_wait)
        except asyncio.TimeoutError:
            logger.debug("%s: still in flight after %.2fs; answering without it",
                         self.name, max_wait)
        except Exception:  # noqa: BLE001 — _run already swallows its own
            logger.debug("%s: refresh raised", self.name, exc_info=True)
        return self._value
