"""What TAG-IT knows about its own corpora, and how to wake it up.

Two small things that between them fix the two ways the full-text columns kept
misleading people.

**Coverage was hardcoded, and hardcoded coverage rots.** The מבקר column said
"local government, 2018–2019 only" — a description of the first page of one
query — and the protocols column offered a date filter over a corpus that was
mostly undated, silently hiding ~89% of it. Both were written by hand from a
sample, and both were wrong. ``list_scopes`` reports doc_count, date_min/max and
``has_dates`` per scope, so the column can describe itself from the number
rather than from someone's recollection, and can DROP its date filter when the
corpus cannot support one.

**Latency was mistaken for failure.** TAG-IT's own statement_timeout on the
search path is 25s, so a *successful* 45s answer was mostly wake-up, not query.
``warm`` opens the connection and touches the full-text index; measured on their
side it is the difference between 24.5s and 4.9s for the same search. It is
called when the page asks for the source list — the moment a user is about to
search, and the only moment we know that in advance.

Both degrade to nothing: an unreachable TAG-IT leaves the static hints in place
and the page works exactly as before.
"""
from __future__ import annotations

import asyncio
import logging
import time

logger = logging.getLogger(__name__)

# Scope metadata changes when a corpus is re-imported — rare, and a stale read
# costs only a slightly-off coverage label, so this is cached generously.
_TTL_SECONDS = 900
_cache: dict = {"at": 0.0, "scopes": {}}
_refresh_task = None

# How long a REQUEST may wait on the coverage fetch before rendering without it.
# Small on purpose: this runs on the request that draws the search UI, and the
# labels are worth a fraction of a second, never a page that cannot be used.
DEFAULT_MAX_WAIT_S = 1.0

# Warming is per-scope and cheap, but not free; once per TTL is plenty.
_warmed: dict[int, float] = {}
_WARM_TTL_SECONDS = 300
_pending_warms: set = set()


async def _call(tool: str, args: dict) -> dict:
    """One TAG-IT MCP tool call, through the transport that already works.

    tagit_mcp owns the service token, the cold-start retry and the SSE/JSON
    unwrapping; there is no reason for a second copy of any of that here.
    """
    from app.services import tagit_mcp

    result = await tagit_mcp._rpc("tools/call", {"name": tool, "arguments": args})
    return tagit_mcp._tool_payload(result) or {}


def _cache_is_fresh() -> bool:
    return bool(_cache["scopes"]) and time.time() - _cache["at"] < _TTL_SECONDS


async def _refresh() -> None:
    """Fetch list_scopes into the cache. Never raises."""
    try:
        payload = await _call("list_scopes", {})
    except Exception:  # noqa: BLE001 — never break the page over a label
        logger.info("tagit list_scopes unavailable; keeping static hints",
                    exc_info=True)
        return
    out: dict[int, dict] = {}
    for s in (payload.get("scopes") or []):
        if not isinstance(s, dict):
            continue
        sid = s.get("id", s.get("scope"))
        try:
            out[int(sid)] = s
        except (TypeError, ValueError):
            continue
    if out:
        _cache["scopes"], _cache["at"] = out, time.time()


def _ensure_refresh() -> asyncio.Task:
    """One in-flight refresh at a time, shared by every waiter."""
    global _refresh_task
    if _refresh_task is None or _refresh_task.done():
        _refresh_task = asyncio.get_running_loop().create_task(_refresh())
    return _refresh_task


async def scopes(max_wait: float | None = None) -> dict[int, dict]:
    """``{scope_id: {name, doc_count, dated_doc_count, has_dates, date_min,
    date_max, updated_at}}``, cached. Empty dict when TAG-IT cannot be reached.

    ``max_wait`` bounds how long the CALLER waits, not how long the fetch takes.
    This distinction is the whole point: TAG-IT runs on a spin-down tier and
    tagit_mcp will patiently retry a cold start for up to 100s, so awaiting this
    on a request path hands a third party's boot time to our own page. That is
    exactly what happened — /api/deep-search/sources gates the שאלות לעם search
    button, and a cold TAG-IT left the button un-clickable with a spinner cursor
    and no explanation.

    So the refresh is a shared background task and the caller merely watches it
    for ``max_wait``. asyncio.shield keeps a timed-out wait from cancelling the
    fetch, so the work still lands in the cache and the NEXT request has the
    labels. A coverage note is a nice-to-have; it may never be on the critical
    path of a page load.

    NOTE the key: over MCP the field is ``id``; the REST twin calls the same
    field ``scope``. Both spellings are accepted so a switch between them does
    not silently produce an empty map.
    """
    if _cache_is_fresh():
        return _cache["scopes"]
    task = _ensure_refresh()
    try:
        if max_wait is None:
            await asyncio.shield(task)
        else:
            await asyncio.wait_for(asyncio.shield(task), max_wait)
    except asyncio.TimeoutError:
        logger.debug("tagit list_scopes still in flight after %.1fs; "
                     "serving without coverage labels this time", max_wait)
    except Exception:  # noqa: BLE001 — _refresh already swallows its own
        logger.debug("tagit list_scopes refresh failed", exc_info=True)
    return _cache["scopes"]


def coverage_label(meta: dict | None) -> str | None:
    """"1989–2019 · 981 מסמכים" — the corpus stating its own bounds.

    Returns None when the scope carries no dates, because "no date range" is
    itself worth not claiming.
    """
    if not meta:
        return None
    lo, hi = (meta.get("date_min") or "")[:4], (meta.get("date_max") or "")[:4]
    n = meta.get("doc_count")
    span = f"{lo}–{hi}" if lo and hi else None
    count = f"{int(n):,} מסמכים" if isinstance(n, int) else None
    parts = [p for p in (span, count) if p]
    return " · ".join(parts) if parts else None


def dates_usable(meta: dict | None) -> bool | None:
    """Whether a date filter over this scope would be honest.

    True/False from the service; None when we could not ask, which the caller
    must treat as "leave the static decision alone" rather than as False.
    """
    if not meta:
        return None
    if meta.get("has_dates") is False:
        return False
    dated, total = meta.get("dated_doc_count"), meta.get("doc_count")
    if isinstance(dated, int) and isinstance(total, int) and total:
        # A partly-backfilled corpus is the dangerous case: the filter works,
        # returns plausible rows, and hides everything not yet dated. The
        # protocols scope sat at 4 dated documents out of 22,036 and still
        # answered date-filtered queries.
        return dated / total >= 0.95
    return bool(meta.get("has_dates"))


async def warm(scope: int | None = None) -> None:
    """Wake the search path. Fire-and-forget: never raises, never blocks."""
    now = time.time()
    key = int(scope or 0)
    if now - _warmed.get(key, 0.0) < _WARM_TTL_SECONDS:
        return
    _warmed[key] = now
    try:
        await _call("warm", {"scope": scope} if scope else {})
    except Exception:  # noqa: BLE001 — warming is best-effort by definition
        logger.debug("tagit warm failed for scope %s", scope, exc_info=True)


def warm_in_background(scope_ids) -> None:
    """Start a warm per scope without making the caller wait for any of them."""
    for sid in scope_ids:
        try:
            task = asyncio.get_running_loop().create_task(warm(sid))
        except RuntimeError:      # no loop (sync context) — nothing to warm into
            return
        # asyncio holds only a weak reference to a running task, so a fire-and-
        # forget task with no other referent can be collected mid-flight — which
        # would cancel the warm-up this function exists to perform, silently and
        # only under memory pressure. Hold it until it finishes.
        _pending_warms.add(task)
        task.add_done_callback(_pending_warms.discard)
