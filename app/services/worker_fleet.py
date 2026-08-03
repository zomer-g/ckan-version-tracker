"""The fleet as it reports itself, and the drain switch for one machine.

Why a table instead of deriving the fleet from ``scrape_tasks``: the tasks tell
you which workers RAN something, never which are alive and idle right now — and
"alive and idle" is precisely the state you are waiting for when you want to
restart a worker to update its code. There was also nowhere to put the pause
flag, since a worker was not a row anywhere.

The drain is deliberately the gentlest possible mechanism: a paused worker is
simply told "no task" (204) when it asks for its next one. Nothing is killed,
nothing is rolled back, and a scrape that has been running for an hour finishes.
The worker needs no new code to obey it — 204 is the response it has always
handled — which matters because the worker repo deploys separately from this one.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.models.worker_node import WorkerNode

logger = logging.getLogger(__name__)

# A worker polls about once a second. Writing last_seen_at on every poll would
# put a row update per worker per second on a DB billed by compute, to gain
# precision nobody reads — the panel shows "seen 3s ago" the same either way.
_TOUCH_EVERY = timedelta(seconds=20)

# No poll for this long and the machine is presumed gone (crashed, shut down,
# mid-restart). Matches the task heartbeat window in poll_for_task, so a worker
# going quiet and its task being auto-failed are reported on the same clock.
OFFLINE_AFTER = timedelta(minutes=10)


def _as_utc(dt: datetime | None) -> datetime | None:
    """Read a stored timestamp as UTC-aware.

    Postgres hands back timestamptz already aware, but SQLite (the test DB, and
    anything reading an older row written before the column was tz-aware) hands
    back naive — and subtracting the two raises. Assuming UTC is right here
    because every writer in this module stamps UTC. Same defence as
    ``engine_epoch`` in app/services/govmap_coverage.py.
    """
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def worker_key(worker_id: str | None, worker_ip: str | None) -> str:
    """Stable key for one machine.

    The explicit id wins because two workers behind one NAT share an IP and
    would otherwise pause each other. Workers too old to send an id are keyed
    by address so they are still listed and still pausable, at that precision.
    """
    wid = (worker_id or "").strip()
    if wid:
        return wid[:80]
    ip = (worker_ip or "").strip()
    if ip and ip != "unknown":
        return f"ip:{ip}"[:80]
    return "unknown"


async def touch_worker(
    db: AsyncSession,
    *,
    worker_id: str | None,
    worker_ip: str | None,
    worker_version: str | None = None,
    worker_upstream: str | None = None,
) -> WorkerNode | None:
    """Record this poll and return the machine's row (including ``paused``).

    Returns None if the fleet table can't be read or written — the caller then
    proceeds as if nothing is paused. Same reasoning as the source caps: bookkeeping
    about the fleet must never be able to stop the fleet from getting work.
    """
    key = worker_key(worker_id, worker_ip)
    now = datetime.now(timezone.utc)
    try:
        node = await db.get(WorkerNode, key)
        if node is None:
            node = WorkerNode(
                worker_key=key,
                worker_id=(worker_id or "").strip()[:64] or None,
                worker_ip=(worker_ip or "").strip()[:64] or None,
                worker_version=(worker_version or "").strip()[:64] or None,
                worker_upstream=(worker_upstream or "").strip()[:16] or None,
                last_seen_at=now,
            )
            db.add(node)
            await db.commit()
            logger.info("New worker in the fleet: %s", key)
            return node

        # Throttled: identity/version changes are worth a write immediately
        # (a restarted worker on new code should not look stale for 20s), a
        # plain heartbeat is not.
        version = (worker_version or "").strip()[:64] or None
        upstream = (worker_upstream or "").strip()[:16] or None
        ip = (worker_ip or "").strip()[:64] or None
        changed = (
            (version is not None and version != node.worker_version)
            or (upstream is not None and upstream != node.worker_upstream)
            or (ip is not None and ip != node.worker_ip)
        )
        seen = _as_utc(node.last_seen_at)
        if changed or seen is None or (now - seen) > _TOUCH_EVERY:
            node.last_seen_at = now
            if version is not None:
                node.worker_version = version
            if upstream is not None:
                node.worker_upstream = upstream
            if ip is not None:
                node.worker_ip = ip
            await db.commit()
        return node
    except SQLAlchemyError as e:  # noqa: BLE001 — dispatch outranks bookkeeping
        await db.rollback()
        logger.warning(
            "workers table unreadable (%s: %s) — dispatching without fleet "
            "bookkeeping until it is back", type(e).__name__, e,
        )
        return None


async def fleet(db: AsyncSession) -> list[dict]:
    """Every worker we have seen, with what it is doing right now."""
    nodes = (await db.scalars(select(WorkerNode))).all()

    # What each machine currently holds. Keyed the same way the poll keys it,
    # so a task and its worker line up even for id-less workers.
    running = await db.execute(
        select(ScrapeTask, TrackedDataset)
        .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
        .where(ScrapeTask.status == "running")
    )
    current: dict[str, dict] = {}
    for task, ds in running.all():
        current[worker_key(task.worker_id, task.worker_ip)] = {
            "task_id": str(task.id),
            "dataset_id": str(ds.id),
            "dataset_title": ds.title,
            "phase": task.phase,
            "progress": task.progress,
            "started_at": task.created_at.isoformat() if task.created_at else None,
            "last_report_at": task.updated_at.isoformat() if task.updated_at else None,
        }

    now = datetime.now(timezone.utc)
    out = []
    for n in nodes:
        seen = _as_utc(n.last_seen_at)
        offline = seen is None or (now - seen) > OFFLINE_AFTER
        task = current.get(n.worker_key)
        out.append({
            "worker_key": n.worker_key,
            "worker_id": n.worker_id,
            "worker_ip": n.worker_ip,
            "worker_version": n.worker_version,
            "worker_upstream": n.worker_upstream,
            "last_seen_at": seen.isoformat() if seen else None,
            "offline": offline,
            "paused": n.paused,
            "paused_at": n.paused_at.isoformat() if n.paused_at else None,
            "paused_by": n.paused_by,
            "current_task": task,
            # The state an operator waits for before restarting a machine:
            # drained means "paused AND holding nothing".
            "drained": bool(n.paused and task is None),
        })
    # Busy first, then live-but-idle, then the machines that stopped reporting.
    out.sort(key=lambda w: (w["offline"], w["current_task"] is None, w["worker_key"]))
    return out
