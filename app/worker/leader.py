"""One scheduler at a time: leader election on a Postgres advisory lock.

Why this exists:
    An xhostd deploy starts the NEW container before it stops the OLD one, and
    they overlap for ~10-15 s (2026-10-31: old #97 finished 11:10:07, new #98
    started 11:09:56). Both ran ``init_scheduler()``, so for that window two
    schedulers fired the same jobs. Scrape tasks are protected by a partial
    unique index in the database; the in-process jobs (CKAN polls, merges, the
    history export, health checks…) are not, and ran twice.

How:
    At boot the process opens ONE dedicated connection to the app database and
    asks ``pg_try_advisory_lock(LOCK_KEY)``. A session-level advisory lock lives
    exactly as long as that server session, so the process that holds it is the
    leader, and when it exits (or crashes, or its connection is cut) Postgres
    releases the lock by itself: no lease to expire, no row to clean up. The
    loser retries every RETRY_S and starts the scheduler once it wins, which on
    a deploy is right after the old container stops.

    The wait runs in a background task: launch.sh must answer HTTP within 120 s,
    and the old container may take longer than that to go if it is busy.

    The connection must be DIRECT. Through xhostd's transaction-mode PgBouncer a
    session lock is taken on whichever server connection the pooler lent for
    that one statement, and is then held by (or leaks to) some other client.
    DATABASE_URL_DIRECT is that address (xhostd-env.sh); without a pooler it is
    DATABASE_URL itself. Where only a pooled address exists, or the database is
    not Postgres at all (SQLite in tests), there is no election: the scheduler
    starts as it always did, with a WARNING.

Losing the lock:
    The lock connection is pinged every KEEPALIVE_S. If it drops, Postgres has
    released the lock (or will, once it notices), and another process may take
    it. The leader then calls ``on_lost`` (app/main.py pauses the scheduler),
    reconnects, and calls ``on_regained`` once it holds the lock again. Pausing
    rather than carrying on is the safe side: a missed poll is retried by the
    next one, a doubled job is not undone.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable

from app.config import settings

logger = logging.getLogger(__name__)

# Any constant bigint, as long as nothing else in this database uses it. Spells
# "OVER_SCH" in ASCII, so it is recognisable in pg_locks (objid/classid halves).
LOCK_KEY = 0x4F5645525F534348
RETRY_S = 5.0
KEEPALIVE_S = 30.0
# Consecutive connection errors at boot before giving up on the election and
# starting the scheduler anyway (the documented fallback): a database that
# cannot be reached cannot run the jobs either, and a process that never
# schedules anything is worse than the 15-second overlap this guards against.
MAX_BOOT_FAILURES = 3
# How often the waiting process says so in the log.
_WAIT_LOG_EVERY_S = 60.0

# status values
OFF = "off"            # writers disabled on this instance, no election
WAITING = "waiting"    # another process holds the lock
LEADER = "leader"      # this process holds the lock and runs the scheduler
LOST = "lost"          # held the lock, the connection dropped; scheduler paused
FALLBACK = "fallback"  # no election possible; scheduler runs without one


@dataclass
class _State:
    status: str = OFF
    conn: object | None = None
    task: asyncio.Task | None = None
    since: float = 0.0


state = _State()

Callback = Callable[[], Awaitable[None] | None]


def lock_dsn() -> str | None:
    """The asyncpg DSN for the lock connection, or None when there can be no
    election (not Postgres, or only a pooled address)."""
    url = (settings.database_url_direct or settings.database_url or "").strip()
    if not url.startswith("postgres"):
        return None
    from app.database import _is_pgbouncer
    from app.services.append_store import _dsn_from

    if _is_pgbouncer(url):
        return None
    return _dsn_from(url)


async def _connect(dsn: str):
    import asyncpg

    from app.pg_ssl import asyncpg_ssl_for

    return await asyncpg.connect(
        dsn=dsn, ssl=asyncpg_ssl_for(dsn), timeout=15,
        # Names the session in pg_stat_activity, next to its lock in pg_locks.
        server_settings={"application_name": "over-scheduler-leader"},
    )


async def _try_lock(conn) -> bool:
    return bool(await conn.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_KEY, timeout=10))


async def _close(conn) -> None:
    if conn is None:
        return
    try:
        await asyncio.wait_for(conn.close(), timeout=5)
    except Exception:  # noqa: BLE001 — closing is best effort; terminate is not
        try:
            conn.terminate()
        except Exception:  # noqa: BLE001
            pass


async def _call(cb: Callback | None) -> None:
    if cb is None:
        return
    res = cb()
    if asyncio.iscoroutine(res):
        await res


def _set(status: str) -> None:
    state.status = status
    state.since = time.monotonic()


async def _acquire(dsn: str, *, connect, boot: bool) -> bool:
    """Loop until this process holds the lock (True), or, at boot only, until
    MAX_BOOT_FAILURES consecutive connection errors (False: fall back)."""
    conn = None
    failures = 0
    last_log = 0.0
    try:
        while True:
            try:
                if conn is None:
                    conn = await connect(dsn)
                if await _try_lock(conn):
                    state.conn, conn = conn, None
                    return True
                failures = 0
                if state.status != LOST:
                    _set(WAITING)
                now = time.monotonic()
                if now - last_log >= _WAIT_LOG_EVERY_S:
                    last_log = now
                    logger.info("scheduler leader: another instance holds the lock, "
                                "retrying every %.0fs (a deploy overlap ends in ~15s)", RETRY_S)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                failures += 1
                await _close(conn)
                conn = None
                if boot and failures >= MAX_BOOT_FAILURES:
                    logger.warning("scheduler leader: cannot reach the lock (%s: %s) after %d "
                                   "tries; starting the scheduler WITHOUT leader election",
                                   type(e).__name__, e, failures)
                    return False
                logger.warning("scheduler leader: lock attempt failed (%s: %s)",
                               type(e).__name__, e)
            await asyncio.sleep(RETRY_S)
    finally:
        await _close(conn)


async def _keepalive() -> None:
    """Return when the lock connection stops answering."""
    while True:
        await asyncio.sleep(KEEPALIVE_S)
        conn = state.conn
        try:
            if conn is None or conn.is_closed():
                raise ConnectionError("lock connection closed")
            await conn.fetchval("SELECT 1", timeout=10)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            logger.error("scheduler leader: lock connection lost (%s: %s); pausing the "
                         "scheduler until the lock is held again", type(e).__name__, e)
            await _close(conn)
            state.conn = None
            return


async def run(on_leader: Callback, *, on_lost: Callback | None = None,
              on_regained: Callback | None = None, connect=None) -> None:
    """Become leader, then call ``on_leader`` once; keep the lock for life.

    ``connect`` is injectable for tests (an async ``dsn -> connection``)."""
    connect = connect or _connect
    dsn = lock_dsn()
    if dsn is None:
        logger.warning("scheduler leader: no direct Postgres address (DATABASE_URL_DIRECT), "
                       "so no leader election; starting the scheduler without it")
        _set(FALLBACK)
        await _call(on_leader)
        return
    if not await _acquire(dsn, connect=connect, boot=True):
        _set(FALLBACK)
        await _call(on_leader)
        return
    _set(LEADER)
    logger.info("scheduler leader: lock acquired, starting the scheduler")
    await _call(on_leader)
    while True:
        await _keepalive()
        _set(LOST)
        await _call(on_lost)
        await _acquire(dsn, connect=connect, boot=False)
        _set(LEADER)
        logger.warning("scheduler leader: lock held again, resuming the scheduler")
        await _call(on_regained)


def start(on_leader: Callback, **kw) -> asyncio.Task:
    """Run the election in the background (never blocks boot)."""
    async def _guarded():
        try:
            await run(on_leader, **kw)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — a bug here must not leave writers off silently
            logger.exception("scheduler leader: election crashed")
            if state.status in (OFF, WAITING):
                # Never got to start: fall back rather than run nothing.
                _set(FALLBACK)
                await _call(on_leader)
    state.task = asyncio.create_task(_guarded())
    return state.task


async def release() -> None:
    """Stop waiting, and let the next process lead (shutdown). Closing the
    session releases the lock; an explicit unlock first makes it immediate even
    if the close lingers."""
    task, state.task = state.task, None
    if task is not None and not task.done():
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
    conn, state.conn = state.conn, None
    if conn is not None:
        try:
            await asyncio.wait_for(
                conn.fetchval("SELECT pg_advisory_unlock($1)", LOCK_KEY), timeout=3)
        except Exception:  # noqa: BLE001 — the close below releases it anyway
            pass
        await _close(conn)
        logger.info("scheduler leader: lock released")
    if state.status != OFF:
        _set(OFF)


def holds_scheduler() -> bool:
    """Whether this process should be running jobs right now (for /healthz)."""
    return state.status in (LEADER, FALLBACK)
