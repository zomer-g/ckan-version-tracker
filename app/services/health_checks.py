"""System health checks: one registry, a daily light pass, a weekly full pass,
and a run on demand. Every pass is kept in ``app.health_runs``.

See docs/health-checks.md for the plan and what each check means.

WHY ONE REGISTRY
────────────────
OVER already had pieces of this: the queue panel's "open failures", the fleet
panel, ``/healthz``, and three scripts (scripts/api_smoke_test.py,
api_docs_test.py, source_health_check.py) that someone had to remember to run.
Each answered one question, and none of them kept a history. So a scraper that
broke on Tuesday was found when somebody happened to look, and nobody could say
whether it had been broken since Tuesday or since last month.

A check here is a small async function registered with :func:`check`. It
returns a :class:`CheckResult`: a status, one sentence in Hebrew for the admin,
optional ``details`` and optional ``items`` — sub-results, one per thing the
check looked at (one per scraper source, one per external upstream). Items are
what let the log say *which* scraper broke, and the diff between runs works at
item level too.

THE TWO TIERS
─────────────
``daily``  — cheap: a handful of indexed queries, a few HTTP GETs. Minutes, not
             hours; nothing reads a big table or downloads a big file.
``weekly`` — every check, with ``ctx.deep`` set: wider time windows, a random
             sample of R2 objects actually HEADed, the existing API contract
             scripts run against the live site.
``manual`` — the admin picks the checks (or a tier) and presses run.

WHAT THE LOG ADDS
─────────────────
Every result is compared with the most recent earlier run that ran the same
check. A key that was fine and is not any more is a *new problem*; one that was
broken and is fine is *resolved*; one broken in both carries the ``since`` of
the first run that saw it. New problems are what the optional webhook sends, so
a source that has been broken for a week does not page anyone seven times.

MEMORY
──────
The web process lives in 512 MB (see the trim_heap job in the scheduler). The
checks run one after another, never in parallel, each under its own timeout,
and nothing here holds more than a few thousand small rows at once.
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import delete, func, select, text, update

from app.config import settings

logger = logging.getLogger(__name__)

OK, WARN, FAIL, SKIP, ERROR = "ok", "warn", "fail", "skip", "error"
# Worst first. ERROR (the check itself crashed or timed out) ranks with FAIL:
# a check that cannot answer is not evidence that things are fine.
_RANK = {OK: 0, SKIP: 0, WARN: 1, FAIL: 2, ERROR: 2}
BAD = frozenset({WARN, FAIL, ERROR})

TIERS = ("daily", "weekly", "manual")

# A run still marked "running" this long after it started was cut off by a
# restart or deploy; the next run closes it as an error.
_STALE_RUN = timedelta(hours=2)


@dataclass
class CheckResult:
    status: str
    message: str
    details: dict | None = None
    # [{"key", "label", "status", "message", ...extra}] — one per sub-thing.
    items: list[dict] | None = None


@dataclass
class Ctx:
    """What a check gets: whether this is the deep pass, and a DB session factory
    (injectable so tests can hand in SQLite)."""
    deep: bool
    session_factory: Callable
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class CheckDef:
    key: str
    component: str
    title: str
    description: str
    tiers: frozenset
    timeout: float
    fn: Callable[[Ctx], Awaitable[CheckResult]]


REGISTRY: dict[str, CheckDef] = {}


def check(key: str, *, component: str, title: str, description: str,
          tiers=("daily", "weekly"), timeout: float = 60.0):
    """Register a check. Order of registration is the order of the run."""
    def deco(fn):
        REGISTRY[key] = CheckDef(key, component, title, description,
                                 frozenset(tiers), timeout, fn)
        return fn
    return deco


def worst(statuses) -> str:
    out = OK
    for s in statuses:
        if _RANK.get(s, 0) > _RANK.get(out, 0) or (s == ERROR and out == FAIL):
            out = s
    return out


def overall(statuses) -> str:
    """A run's status. ``error`` for a check counts as a failed run."""
    w = worst(statuses)
    return FAIL if w == ERROR else w


def checks_for(tier: str, keys: list[str] | None = None) -> list[CheckDef]:
    if keys:
        unknown = [k for k in keys if k not in REGISTRY]
        if unknown:
            raise ValueError(f"unknown checks: {', '.join(unknown)}")
        return [REGISTRY[k] for k in REGISTRY if k in set(keys)]
    if tier == "weekly":
        return list(REGISTRY.values())
    return [c for c in REGISTRY.values() if tier in c.tiers]


def _as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _ago(dt: datetime | None, now: datetime) -> str:
    if dt is None:
        return "אף פעם"
    s = int((now - _as_utc(dt)).total_seconds())
    if s < 3600:
        return f"לפני {max(s // 60, 0)} דק׳"
    if s < 86400:
        return f"לפני {s // 3600} שע׳"
    return f"לפני {s // 86400} ימים"


def _clip(s: str | None, n: int = 300) -> str | None:
    if s is None:
        return None
    s = str(s).strip()
    return s if len(s) <= n else s[: n - 1] + "…"


def _session(ctx: Ctx):
    return ctx.session_factory()


# ═════════════════════════════════════════════════════════════════════════
# Checks
# ═════════════════════════════════════════════════════════════════════════

# ── infrastructure ──────────────────────────────────────────────────────

@check("db.app", component="מסד נתונים", title="מסד הנתונים של האפליקציה",
       description="SELECT 1 מול מסד האפליקציה וזמן התגובה. בשבועית: גודל המסד ומספר החיבורים.")
async def check_app_db(ctx: Ctx) -> CheckResult:
    t0 = time.monotonic()
    async with _session(ctx) as db:
        await db.execute(text("SELECT 1"))
        ms = int((time.monotonic() - t0) * 1000)
        details: dict[str, Any] = {"latency_ms": ms}
        if ctx.deep and db.bind.dialect.name == "postgresql":
            try:
                size = (await db.execute(text(
                    "SELECT pg_database_size(current_database())"))).scalar()
                conns = (await db.execute(text(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE datname = current_database()"))).scalar()
                details.update(size_gb=round((size or 0) / 1e9, 2), connections=conns)
            except Exception as e:  # noqa: BLE001 — extra facts, not the verdict
                details["stats_error"] = _clip(str(e))
    status = WARN if ms > 1500 else OK
    msg = f"המסד עונה ({ms} ms)"
    if "size_gb" in details:
        msg += f" · {details['size_gb']} GB · {details['connections']} חיבורים"
    return CheckResult(status, msg, details)


@check("db.archive", component="מסד נתונים", title="ארכיון ה-SQL (טבלאות append)",
       description="חיבור למסד הארכיון (APPEND_DATABASE_URL) ולתפקיד הקריאה-בלבד של קונסולת ה-SQL הציבורית.")
async def check_archive_db(ctx: Ctx) -> CheckResult:
    from app.services import append_store
    if not append_store.is_configured():
        return CheckResult(SKIP, "ארכיון ה-SQL לא מוגדר בסביבה הזו")
    items = []
    t0 = time.monotonic()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.fetchval("SELECT 1")
    items.append({"key": "write_pool", "label": "חיבור כתיבה", "status": OK,
                  "message": f"{int((time.monotonic() - t0) * 1000)} ms"})
    try:
        t0 = time.monotonic()
        ro = await append_store.get_readonly_pool()
        async with ro.acquire() as conn:
            await conn.fetchval("SELECT 1")
        items.append({"key": "readonly_pool", "label": "חיבור קריאה (קונסולה ציבורית)",
                      "status": OK, "message": f"{int((time.monotonic() - t0) * 1000)} ms"})
    except Exception as e:  # noqa: BLE001
        items.append({"key": "readonly_pool", "label": "חיבור קריאה (קונסולה ציבורית)",
                      "status": FAIL, "message": _clip(f"{type(e).__name__}: {e}")})
    st = worst(i["status"] for i in items)
    bad = [i["label"] for i in items if i["status"] in BAD]
    return CheckResult(st, "הארכיון עונה" if not bad else "נכשל: " + ", ".join(bad),
                       items=items)


@check("storage.r2", component="אחסון קבצים", title="Cloudflare R2",
       description="שהקובץ של הגרסה האחרונה שנשמרה ל-R2 קיים (HEAD). בשבועית: מדגם של עד 40 קבצים מגרסאות אקראיות.",
       timeout=180)
async def check_r2(ctx: Ctx) -> CheckResult:
    from app.models.version_index import VersionIndex
    from app.services import storage_client as sc
    if not sc.storage_client.is_configured():
        return CheckResult(SKIP, "R2 לא מוגדר בסביבה הזו")
    async with _session(ctx) as db:
        recent = (await db.execute(
            select(VersionIndex.id, VersionIndex.tracked_dataset_id,
                   VersionIndex.version_number, VersionIndex.resource_mappings)
            .where(VersionIndex.resource_mappings.isnot(None))
            .order_by(VersionIndex.detected_at.desc()).limit(30)
        )).all()
        sample = list(recent)
        if ctx.deep:
            sample += (await db.execute(
                select(VersionIndex.id, VersionIndex.tracked_dataset_id,
                       VersionIndex.version_number, VersionIndex.resource_mappings)
                .where(VersionIndex.resource_mappings.isnot(None))
                .order_by(func.random()).limit(60)
            )).all()
    budget = 40 if ctx.deep else 1
    keys: list[tuple[str, str]] = []
    seen: set[str] = set()
    for _vid, ds_id, vnum, mappings in sample:
        for _name, value in sc.enumerate_files(mappings if isinstance(mappings, dict) else None):
            if sc.is_storage_value(value) and value not in seen:
                seen.add(value)
                keys.append((f"{ds_id}#v{vnum}", value))
                break  # one file per version is enough to prove the version
        if len(keys) >= budget:
            break
    if not keys:
        return CheckResult(WARN, "לא נמצאה אף גרסה עם קובץ ב-R2 לבדיקה")
    sem = asyncio.Semaphore(4)

    async def head(v):
        async with sem:
            return await sc.storage_client.object_size(v)

    sizes = await asyncio.gather(*(head(v) for _l, v in keys))
    missing = [{"version": lbl, "key": sc.key_of(v)} for (lbl, v), s in zip(keys, sizes) if s is None]
    n = len(keys)
    if not missing:
        return CheckResult(OK, f"{n}/{n} קבצים נמצאו ב-R2", {"checked": n})
    st = FAIL if len(missing) == n or len(missing) > max(2, n // 10) else WARN
    return CheckResult(st, f"{len(missing)} מתוך {n} קבצים חסרים ב-R2 (או ש-R2 לא עונה)",
                       {"checked": n, "missing": missing[:20]})


def _cgroup_limit_mb() -> int | None:
    """The container's memory limit (cgroup v2, then v1), or None if unbounded.

    xhostd gives the container 8 GB; a fixed 512 (the old Render dyno) would
    report a healthy process as failing every day."""
    for path in ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        try:
            raw = open(path).read().strip()
        except OSError:
            continue
        if raw.isdigit() and int(raw) < 1 << 50:   # v1 says "unlimited" with a huge number
            return int(raw) // (1024 * 1024)
    return None


@check("process.memory", component="שרת", title="זיכרון התהליך",
       description="RSS של תהליך ה-web מול מגבלת הזיכרון של הקונטיינר (cgroup, או HEALTH_MEMORY_LIMIT_MB). אזהרה ב-80%, כשל ב-92%.")
async def check_memory(ctx: Ctx) -> CheckResult:
    from app.services import heap
    kb = heap.rss_kb()
    if kb is None:
        return CheckResult(SKIP, "אין מדידת RSS בפלטפורמה הזו")
    mb = kb // 1024
    limit = settings.health_memory_limit_mb or _cgroup_limit_mb() or 512
    frac = mb / limit if limit else 0
    st = FAIL if frac >= 0.92 else WARN if frac >= 0.8 else OK
    return CheckResult(st, f"{mb} MB מתוך {limit} MB ({int(frac * 100)}%)",
                       {"rss_mb": mb, "limit_mb": limit})


# ── scheduling ──────────────────────────────────────────────────────────

# Jobs init_scheduler always registers. A missing one means a job silently
# stopped existing, which nothing else would ever report.
CORE_JOBS = (
    "cleanup_stuck_scrape_tasks", "trim_heap", "drain_datastore_push_queue",
    "cleanup_stuck_push_jobs", "resume_interrupted_appends",
    "drain_drive_export_queue", "cleanup_stuck_drive_exports",
    "govmap_coverage_rollout", "refresh_dataset_sizes", "sampling_schedule",
    "purge_api_access_log", "recheck_console_role",
)


def _expects_poll_job(ds) -> bool:
    cfg = ds.scraper_config or {}
    return bool(ds.poll_interval and ds.poll_interval > 0
                and not cfg.get("coverage_managed"))


@check("scheduler.jobs", component="מתזמן", title="המתזמן ומשימות הרקע",
       description="שהמתזמן רץ, שכל משימות הרקע הקבועות רשומות, ושלכל מאגר פעיל יש משימת polling.")
async def check_scheduler(ctx: Ctx) -> CheckResult:
    from app.models.tracked_dataset import TrackedDataset
    from app.worker.scheduler import scheduler
    if not settings.writers_enabled:
        return CheckResult(SKIP, "המתזמן כבוי במופע הזה (SCHEDULER_ENABLED / MAINTENANCE_MODE / VERSION_FREEZE)")
    if not scheduler.running:
        return CheckResult(FAIL, "המתזמן לא רץ — שום polling ושום משימת רקע לא מתבצעים")
    jobs = {j.id: j for j in scheduler.get_jobs()}
    expected = list(CORE_JOBS)
    if settings.catalog_watch_enabled:
        expected.append("catalog_watch")
    if settings.health_checks_enabled:
        expected += ["health_daily", "health_weekly"]
    missing_core = [j for j in expected if j not in jobs]
    paused = [j.id for j in jobs.values() if j.next_run_time is None]
    async with _session(ctx) as db:
        rows = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.poll_interval,
                   TrackedDataset.scraper_config)
            .where(TrackedDataset.is_active.is_(True), TrackedDataset.status == "active")
        )).all()
    missing_polls = []
    for ds_id, title, interval, cfg in rows:
        if not (interval and interval > 0 and not (cfg or {}).get("coverage_managed")):
            continue
        if f"poll_{ds_id}" not in jobs:
            missing_polls.append({"dataset_id": str(ds_id), "title": title})
    st = OK
    parts = [f"{len(jobs)} משימות רשומות"]
    if missing_core:
        st = FAIL
        parts.append("חסרות משימות ליבה: " + ", ".join(missing_core))
    if missing_polls:
        st = worst([st, WARN])
        parts.append(f"{len(missing_polls)} מאגרים פעילים בלי משימת polling")
    if paused:
        st = worst([st, WARN])
        parts.append(f"{len(paused)} משימות מושהות")
    return CheckResult(st, " · ".join(parts), {
        "jobs": len(jobs), "missing_core": missing_core,
        "missing_polls": missing_polls[:50], "missing_polls_count": len(missing_polls),
        "paused": paused[:50],
    })


# ── the worker fleet and its queue ──────────────────────────────────────

@check("workers.fleet", component="ורקרים", title="צי הוורקרים (govil-scraper)",
       description="כמה ורקרים חיים (דיווחו ב-10 הדקות האחרונות), כמה לא מושהים, והאם הם על הגרסה הנעוצה.")
async def check_fleet(ctx: Ctx) -> CheckResult:
    from app.services.worker_fleet import fleet
    async with _session(ctx) as db:
        nodes = await fleet(db)
    online = [n for n in nodes if not n["offline"]]
    working = [n for n in online if not n["paused"]]
    pin = (settings.worker_required_version or "").strip()
    items = []
    for n in nodes:
        if n["offline"]:
            st, msg = SKIP, "לא מדווח (" + _ago(datetime.fromisoformat(n["last_seen_at"]) if n["last_seen_at"] else None, ctx.now) + ")"
        else:
            st, msg = OK, "מושהה" if n["paused"] else ("עובד" if n["current_tasks"] else "פנוי")
            ver = (n.get("worker_version") or "")
            if pin and ver and not (ver.startswith(pin) or pin.startswith(ver)):
                st, msg = WARN, msg + f" · גרסה {ver[:10]} ≠ {pin[:10]}"
            if (n.get("worker_upstream") or "") == "behind":
                st, msg = WARN, msg + " · מאחורי origin"
        items.append({"key": n["worker_key"], "label": n["worker_key"], "status": st, "message": msg})
    if not online:
        return CheckResult(FAIL, "אין אף ורקר חי — שום גירוד לא מתבצע", items=items)
    if not working:
        return CheckResult(FAIL, f"{len(online)} ורקרים חיים, כולם מושהים", items=items)
    st = worst(i["status"] for i in items)
    return CheckResult(st, f"{len(working)} ורקרים פעילים מתוך {len(online)} חיים ({len(nodes)} מוכרים)",
                       {"online": len(online), "working": len(working), "known": len(nodes)},
                       items=items)


@check("queue.scrape", component="ורקרים", title="תור הגירוד",
       description="גודל התור, גיל המשימה הממתינה הוותיקה בפסי העדיפות השוטפים (≥100), ומשימות רצות בלי heartbeat.")
async def check_scrape_queue(ctx: Ctx) -> CheckResult:
    from app.models.scrape_task import PRIORITY_ROUTINE, ScrapeTask
    async with _session(ctx) as db:
        pending = (await db.execute(
            select(func.count()).select_from(ScrapeTask).where(ScrapeTask.status == "pending")
        )).scalar() or 0
        oldest = (await db.execute(
            select(func.min(ScrapeTask.created_at)).where(
                ScrapeTask.status == "pending", ScrapeTask.priority >= PRIORITY_ROUTINE)
        )).scalar()
        running = (await db.execute(
            select(func.count()).select_from(ScrapeTask).where(ScrapeTask.status == "running")
        )).scalar() or 0
        silent = (await db.execute(
            select(func.count()).select_from(ScrapeTask).where(
                ScrapeTask.status == "running",
                ScrapeTask.updated_at < ctx.now - timedelta(minutes=15))
        )).scalar() or 0
    age_h = ((ctx.now - _as_utc(oldest)).total_seconds() / 3600) if oldest else 0
    st = FAIL if age_h >= 24 else WARN if age_h >= 6 else OK
    if silent:
        st = worst([st, WARN])
    msg = f"{pending} ממתינות · {running} רצות"
    if oldest:
        msg += f" · הוותיקה בפס השוטף ממתינה {age_h:.1f} שע׳"
    if silent:
        msg += f" · {silent} רצות בלי heartbeat מעל 15 דק׳"
    return CheckResult(st, msg, {"pending": pending, "running": running,
                                 "oldest_routine_hours": round(age_h, 1),
                                 "silent_running": silent})


@check("queue.background", component="תורי רקע", title="תורי Datastore ו-Drive",
       description="משימות push ל-Datastore ויצוא ל-Drive: כשלים בחלון, ממתינות ותיקות, ורצות תקועות.")
async def check_background_queues(ctx: Ctx) -> CheckResult:
    from app.models.datastore_push_job import DatastorePushJob
    from app.models.drive_export_job import DriveExportJob
    window = timedelta(days=7 if ctx.deep else 1)
    items = []
    async with _session(ctx) as db:
        for key, label, M in (("datastore", "Datastore push", DatastorePushJob),
                              ("drive", "יצוא ל-Drive", DriveExportJob)):
            failed = (await db.execute(select(func.count()).select_from(M).where(
                M.status == "failed", M.updated_at >= ctx.now - window))).scalar() or 0
            oldest = (await db.execute(select(func.min(M.created_at)).where(
                M.status == "pending"))).scalar()
            stuck = (await db.execute(select(func.count()).select_from(M).where(
                M.status == "running", M.updated_at < ctx.now - timedelta(hours=1)))).scalar() or 0
            age_h = ((ctx.now - _as_utc(oldest)).total_seconds() / 3600) if oldest else 0
            st = OK
            parts = []
            if failed:
                st = WARN
                parts.append(f"{failed} נכשלו")
            if age_h >= 2:
                st = worst([st, WARN if age_h < 24 else FAIL])
                parts.append(f"ממתינה {age_h:.1f} שע׳")
            if stuck:
                st = worst([st, WARN])
                parts.append(f"{stuck} תקועות")
            items.append({"key": key, "label": label, "status": st,
                          "message": " · ".join(parts) or "תקין"})
    st = worst(i["status"] for i in items)
    return CheckResult(st, "; ".join(f"{i['label']}: {i['message']}" for i in items), items=items)


# ── scrapers, per source ────────────────────────────────────────────────

def _source_verdict(attempted: int, failing: int) -> str:
    """A source is broken when at least half of what it tried is failing — a
    one-dataset source with its one dataset failing, or a GovMap sweep where
    most layers fail. A few failing among many is a warning: GovMap carries a
    steady handful of layers whose public WFS is gone."""
    if failing == 0:
        return OK
    if attempted and failing / attempted >= 0.5:
        return FAIL
    return WARN


@check("scrapers.runs", component="סקרייפרים", title="ריצות הסקרייפרים לפי מקור",
       description=("לכל מקור (govmap, ckan, mevaker, munidata…): כמה מאגרים ניסו לרוץ בחלון (24 שע׳; 7 ימים בשבועית), "
                    "בכמה הריצה האחרונה נכשלה (ריצות שנקטעו אינן נספרות), ושגיאות לדוגמה. "
                    "מקור שחצי ממה שניסה נכשל = שבור; כשל חלקי = אזהרה. מאגרי CKAN נבדקים לפי last_error."),
       timeout=120)
async def check_scraper_runs(ctx: Ctx) -> CheckResult:
    from app.models.scrape_task import ScrapeTask, is_interrupted
    from app.models.tracked_dataset import TrackedDataset
    from app.services.source_load import source_key
    window = timedelta(days=7 if ctx.deep else 1)
    cutoff = ctx.now - window
    async with _session(ctx) as db:
        rows = (await db.execute(
            select(ScrapeTask.tracked_dataset_id, ScrapeTask.status, ScrapeTask.phase,
                   func.substr(ScrapeTask.error, 1, 400), ScrapeTask.completed_at,
                   TrackedDataset.ckan_id, TrackedDataset.source_type, TrackedDataset.title)
            .join(TrackedDataset, ScrapeTask.tracked_dataset_id == TrackedDataset.id)
            .where(ScrapeTask.status.in_(("completed", "failed")),
                   ScrapeTask.completed_at >= cutoff)
        )).all()
        # In-process sources (CKAN and the like) never queue a task: their
        # verdict is the dataset's own last_error, written by the poll job.
        inproc = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.ckan_id, TrackedDataset.source_type,
                   TrackedDataset.title, func.substr(TrackedDataset.last_error, 1, 400),
                   TrackedDataset.last_polled_at)
            .where(TrackedDataset.is_active.is_(True), TrackedDataset.status == "active",
                   TrackedDataset.source_type == "ckan",
                   TrackedDataset.last_polled_at >= cutoff)
        )).all()

    # newest finished task per dataset decides that dataset's state
    latest: dict = {}
    per_src: dict[str, dict] = {}
    for ds_id, status, phase, err, done, ckan_id, stype, title in rows:
        if status == "failed" and is_interrupted(phase, err):
            continue
        k = source_key(ckan_id, stype)
        s = per_src.setdefault(k, {"completed": 0, "failed": 0, "datasets": {}})
        s["completed" if status == "completed" else "failed"] += 1
        prev = latest.get(ds_id)
        if prev is None or _as_utc(done) > _as_utc(prev[0]):
            latest[ds_id] = (done, status, err, title, k)
    for ds_id, (done, status, err, title, k) in latest.items():
        per_src[k]["datasets"][ds_id] = (status, err, title, done)
    task_ds = set(latest)
    for ds_id, ckan_id, stype, title, err, polled in inproc:
        if ds_id in task_ds:
            continue
        k = source_key(ckan_id, stype)
        s = per_src.setdefault(k, {"completed": 0, "failed": 0, "datasets": {}})
        s["datasets"][ds_id] = ("failed" if err else "completed", err, title, polled)

    items = []
    for k, s in per_src.items():
        ds = s["datasets"]
        failing = {i: v for i, v in ds.items() if v[0] == "failed"}
        st = _source_verdict(len(ds), len(failing))
        samples = sorted(failing.items(), key=lambda kv: _as_utc(kv[1][3]) or ctx.now, reverse=True)[:5]
        msg = f"{len(ds) - len(failing)}/{len(ds)} מאגרים תקינים"
        if failing:
            msg += f" · {len(failing)} נכשלים"
        items.append({
            "key": k, "label": k, "status": st, "message": msg,
            "attempted": len(ds), "failing": len(failing),
            "tasks_completed": s["completed"], "tasks_failed": s["failed"],
            "samples": [{"dataset_id": str(i), "title": v[2], "error": _clip(v[1], 300),
                         "at": _as_utc(v[3]).isoformat() if v[3] else None}
                        for i, v in samples],
        })
    items.sort(key=lambda i: (-_RANK[i["status"]], -i["failing"], i["key"]))
    if not items:
        return CheckResult(WARN, f"אף מקור לא רץ ב-{window.days or 1} הימים האחרונים")
    broken = [i["key"] for i in items if i["status"] == FAIL]
    warned = [i["key"] for i in items if i["status"] == WARN]
    if broken:
        msg = f"{len(broken)} מקורות שבורים: {', '.join(broken[:8])}"
    elif warned:
        msg = f"{len(warned)} מקורות עם כשלים חלקיים: {', '.join(warned[:8])}"
    else:
        msg = f"כל {len(items)} המקורות שרצו תקינים"
    return CheckResult(worst(i["status"] for i in items), msg,
                       {"window_hours": int(window.total_seconds() // 3600)}, items=items)


@check("scrapers.overdue", component="סקרייפרים", title="מאגרים שלא נבדקו בזמן",
       description="מאגרים פעילים שה-poll האחרון שלהם ישן מפי 3 מהמחזור שהוגדר להם — כלומר המתזמן לא מגיע אליהם.")
async def check_overdue(ctx: Ctx) -> CheckResult:
    from app.models.tracked_dataset import TrackedDataset
    from app.services.source_load import source_key
    async with _session(ctx) as db:
        rows = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.ckan_id, TrackedDataset.source_type,
                   TrackedDataset.title, TrackedDataset.poll_interval,
                   TrackedDataset.last_polled_at, TrackedDataset.scraper_config,
                   TrackedDataset.created_at)
            .where(TrackedDataset.is_active.is_(True), TrackedDataset.status == "active")
        )).all()
    per: dict[str, dict] = {}
    for ds_id, ckan_id, stype, title, interval, polled, cfg, created in rows:
        cfg = cfg or {}
        if not interval or interval <= 0 or cfg.get("coverage_managed") or cfg.get("push_mode") == "external":
            continue
        k = source_key(ckan_id, stype)
        p = per.setdefault(k, {"total": 0, "late": []})
        p["total"] += 1
        ref = _as_utc(polled) or _as_utc(created)
        grace = max(3 * interval, 6 * 3600)
        if ref and (ctx.now - ref).total_seconds() > grace:
            p["late"].append({"dataset_id": str(ds_id), "title": title,
                              "last_polled": _ago(polled, ctx.now)})
    items = []
    for k, p in per.items():
        n = len(p["late"])
        st = OK if n == 0 else (FAIL if p["total"] >= 2 and n / p["total"] >= 0.5 else WARN)
        items.append({"key": k, "label": k, "status": st,
                      "message": f"{n}/{p['total']} באיחור" if n else f"{p['total']} בזמן",
                      "late": p["late"][:10], "late_count": n})
    items.sort(key=lambda i: (-_RANK[i["status"]], -i["late_count"], i["key"]))
    late = sum(i["late_count"] for i in items)
    return CheckResult(worst(i["status"] for i in items) if items else OK,
                       f"{late} מאגרים באיחור" if late else "כל המאגרים נבדקים בזמן",
                       items=items or None)


@check("scrapers.silent", component="סקרייפרים", title="שבירה שקטה: מאגרים בלי איסוף מוצלח",
       description=("לכל מאגר פעיל: מתי האיסוף המוצלח האחרון (משימה שהסתיימה, גרסה שנחתה, או poll של CKAN בלי שגיאה). "
                    "מאגר שלא הצליח מעל פי 3 מהמחזור שלו (לפחות 3 ימים; 120 יום לשכבות כיסוי GovMap) — נשבר בשקט."),
       tiers=("weekly",), timeout=180)
async def check_silent(ctx: Ctx) -> CheckResult:
    from app.models.scrape_task import ScrapeTask
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex
    from app.services.source_load import source_key
    async with _session(ctx) as db:
        last_task = dict((await db.execute(
            select(ScrapeTask.tracked_dataset_id, func.max(ScrapeTask.completed_at))
            .where(ScrapeTask.status == "completed")
            .group_by(ScrapeTask.tracked_dataset_id))).all())
        last_ver = dict((await db.execute(
            select(VersionIndex.tracked_dataset_id, func.max(VersionIndex.detected_at))
            .group_by(VersionIndex.tracked_dataset_id))).all())
        rows = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.ckan_id, TrackedDataset.source_type,
                   TrackedDataset.title, TrackedDataset.poll_interval,
                   TrackedDataset.last_polled_at, TrackedDataset.last_error,
                   TrackedDataset.scraper_config, TrackedDataset.created_at,
                   TrackedDataset.source_gone_at)
            .where(TrackedDataset.is_active.is_(True), TrackedDataset.status == "active")
        )).all()
    per: dict[str, dict] = {}
    for ds_id, ckan_id, stype, title, interval, polled, err, cfg, created, gone in rows:
        cfg = cfg or {}
        if gone is not None:
            continue  # the publisher removed it; nothing to collect
        if cfg.get("coverage_managed"):
            limit = 120 * 86400
        elif not interval or interval <= 0:
            continue
        else:
            limit = max(3 * interval, 3 * 86400)
        cands = [_as_utc(last_task.get(ds_id)), _as_utc(last_ver.get(ds_id))]
        if stype == "ckan" and not err:
            cands.append(_as_utc(polled))
        ok_at = max([c for c in cands if c], default=None)
        k = source_key(ckan_id, stype)
        p = per.setdefault(k, {"total": 0, "stale": []})
        p["total"] += 1
        ref = ok_at or _as_utc(created)
        if ref and (ctx.now - ref).total_seconds() > limit:
            p["stale"].append({"dataset_id": str(ds_id), "title": title,
                               "last_success": _ago(ok_at, ctx.now),
                               "error": _clip(err, 200)})
    items = []
    for k, p in per.items():
        n = len(p["stale"])
        st = OK if n == 0 else (FAIL if p["total"] and n / p["total"] >= 0.5 else WARN)
        items.append({"key": k, "label": k, "status": st,
                      "message": f"{n}/{p['total']} בלי איסוף מוצלח" if n else f"{p['total']} תקינים",
                      "stale": p["stale"][:10], "stale_count": n})
    items.sort(key=lambda i: (-_RANK[i["status"]], -i["stale_count"], i["key"]))
    stale = sum(i["stale_count"] for i in items)
    return CheckResult(worst(i["status"] for i in items) if items else OK,
                       f"{stale} מאגרים בלי איסוף מוצלח בזמן הצפוי" if stale else "לכל המאגרים יש איסוף מוצלח עדכני",
                       items=items or None)


@check("datasets.quality", component="מאגרים", title="אזהרות איכות ומקורות שנעלמו",
       description="מאגרים פעילים עם import_warning (גרסה אחרונה חשודה), ומקורות שהמפרסם הסיר בשבוע האחרון.")
async def check_dataset_quality(ctx: Ctx) -> CheckResult:
    from app.models.tracked_dataset import TrackedDataset
    async with _session(ctx) as db:
        warned = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.import_warning,
                   TrackedDataset.import_warning_at)
            .where(TrackedDataset.is_active.is_(True), TrackedDataset.import_warning.isnot(None))
            .order_by(TrackedDataset.import_warning_at.desc()).limit(200)
        )).all()
        gone = (await db.execute(
            select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.source_gone_at)
            .where(TrackedDataset.is_active.is_(True),
                   TrackedDataset.source_gone_at >= ctx.now - timedelta(days=7))
        )).all()
    fresh = [w for w in warned if w[3] and _as_utc(w[3]) >= ctx.now - timedelta(days=7)]
    st = WARN if (fresh or gone) else OK
    msg = f"{len(warned)} מאגרים עם אזהרת איכות ({len(fresh)} חדשות השבוע) · {len(gone)} מקורות נעלמו השבוע"
    return CheckResult(st, msg, {
        "import_warnings": [{"dataset_id": str(i), "title": t, "warning": _clip(w, 200),
                             "at": _as_utc(a).isoformat() if a else None} for i, t, w, a in warned[:20]],
        "gone_this_week": [{"dataset_id": str(i), "title": t,
                            "at": _as_utc(a).isoformat()} for i, t, a in gone[:20]],
    })


@check("govmap.coverage", component="סקרייפרים", title="כיסוי שכבות GovMap",
       description="כמה שכבות בקטלוג GovMap מעולם לא נאספו, ומתי הופעלה שכבה לאחרונה (הרולאאוט עובד?).")
async def check_govmap_coverage(ctx: Ctx) -> CheckResult:
    from app.models.govmap_coverage import GovmapCoverage
    async with _session(ctx) as db:
        total = (await db.execute(select(func.count()).select_from(GovmapCoverage))).scalar() or 0
        if not total:
            return CheckResult(SKIP, "טבלת הכיסוי ריקה")
        never = (await db.execute(select(func.count()).select_from(GovmapCoverage).where(
            GovmapCoverage.last_triggered_at.is_(None)))).scalar() or 0
        last = (await db.execute(select(func.max(GovmapCoverage.last_triggered_at)))).scalar()
    idle_h = ((ctx.now - _as_utc(last)).total_seconds() / 3600) if last else None
    st = OK
    if idle_h is None or idle_h > 24:
        st = WARN
    msg = f"{total - never}/{total} שכבות נאספו לפחות פעם אחת · הפעלה אחרונה {_ago(last, ctx.now)}"
    return CheckResult(st, msg, {"total": total, "never_triggered": never,
                                 "hours_since_last_trigger": round(idle_h, 1) if idle_h is not None else None})


@check("knesset.mirror", component="סקרייפרים", title="מסד הנתונים של הכנסת (מראת ODATA)",
       description=("מתי סונכרנה הטבלה האחרונה בסכמת knesset. הסנכרון רץ על הוורקר: יומי למה שחדש, "
                    "ובשבת בדיעבד ל-30 יום אחורה. אזהרה אחרי 30 שעות בלי סנכרון או כשטבלה בשגיאה, כשל אחרי 8 ימים."))
async def check_knesset_mirror(ctx: Ctx) -> CheckResult:
    from app.services import knesset_db
    if not knesset_db.is_configured():
        return CheckResult(SKIP, "מראת הכנסת כבויה בסביבה הזו")
    pool = await knesset_db.append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT table_name, status, error, last_synced_at "
                f"FROM {knesset_db._qtable('sync_state')}")
        except Exception as e:  # noqa: BLE001 — schema missing = never built
            return CheckResult(FAIL, _clip(f"אין טבלת sync_state: {e}", 300))
    if not rows:
        return CheckResult(FAIL, "המראה ריקה — אף טבלה לא נרשמה")
    last = max((_as_utc(r["last_synced_at"]) for r in rows if r["last_synced_at"]), default=None)
    errors = [{"table": r["table_name"], "error": _clip(r["error"], 200)}
              for r in rows if r["status"] == "error"]
    age_h = (ctx.now - last).total_seconds() / 3600 if last else None
    st = OK
    if age_h is None or age_h > 8 * 24:
        st = FAIL
    elif age_h > 30 or errors:
        st = WARN
    msg = f"סנכרון אחרון {_ago(last, ctx.now)} · {len(rows)} טבלאות"
    if errors:
        msg += f" · {len(errors)} בשגיאה: " + ", ".join(e["table"] for e in errors[:5])
    return CheckResult(st, msg, {"hours_since_sync": round(age_h, 1) if age_h is not None else None,
                                 "errors": errors[:20]})


# ── the public site and the upstreams ───────────────────────────────────

def _public_base() -> str | None:
    base = (settings.app_base_url or "").rstrip("/")
    if not base or "localhost" in base or "127.0.0.1" in base:
        return None
    return base


_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/131.0 Safari/537.36 over-health-check")


async def _probe(client, url: str, *, expect: str | None = None,
                 headers: dict | None = None, max_bytes: int = 65536) -> tuple[str, str, int]:
    """GET url, read at most ``max_bytes``. Returns (status, message, ms)."""
    t0 = time.monotonic()
    try:
        async with client.stream("GET", url, headers=headers or {}) as r:
            body = b""
            async for chunk in r.aiter_bytes():
                body += chunk
                if len(body) >= max_bytes:
                    break
            ms = int((time.monotonic() - t0) * 1000)
            if r.status_code >= 400:
                return FAIL, f"HTTP {r.status_code}", ms
            # A 200 at the END of a redirect to a maintenance / geo-block page is
            # not an answer: knesset.gov.il 303s non-Israeli addresses to
            # /maintenance-page-geo, which then serves a happy 200.
            final = str(r.url).lower()
            if r.history and ("maintenance" in final or "blocked" in final):
                return FAIL, f"הופנה ל-{r.url} (תחזוקה או חסימה גיאוגרפית)", ms
            if expect and expect.encode() not in body:
                return FAIL, f"HTTP {r.status_code} אבל בלי '{expect}' בתשובה", ms
            return (WARN if ms > 8000 else OK), f"HTTP {r.status_code} · {ms} ms", ms
    except Exception as e:  # noqa: BLE001
        return FAIL, _clip(f"{type(e).__name__}: {e}", 200), int((time.monotonic() - t0) * 1000)


@check("site.public", component="אתר ו-API", title="האתר הציבורי וה-API",
       description="בקשות HTTP אמיתיות לכתובת הציבורית (APP_BASE_URL): healthz, דף הבית, ‎/api/v1, שרת ה-MCP.",
       timeout=120)
async def check_public_site(ctx: Ctx) -> CheckResult:
    import httpx
    base = _public_base()
    if not base:
        return CheckResult(SKIP, "APP_BASE_URL מקומי — אין כתובת ציבורית לבדוק")
    targets = [
        ("healthz", "‎/healthz", f"{base}/healthz", '"ok"'),
        ("home", "דף הבית", f"{base}/", "<div id=\"root\""),
        ("v1_datasets", "‎/api/v1/datasets", f"{base}/api/v1/datasets?limit=1", None),
        ("mcp", "MCP (well-known)", f"{base}/.well-known/oauth-protected-resource/mcp", None),
    ]
    if ctx.deep:
        targets += [
            ("sitemap", "sitemap.xml", f"{base}/sitemap.xml", "<urlset"),
            ("mcp_knesset", "MCP כנסת (well-known)",
             f"{base}/.well-known/oauth-protected-resource/knesset/mcp", None),
        ]
    items = []
    async with httpx.AsyncClient(timeout=25, follow_redirects=True,
                                 headers={"User-Agent": _UA}) as client:
        for key, label, url, expect in targets:
            st, msg, _ms = await _probe(client, url, expect=expect)
            items.append({"key": key, "label": label, "status": st, "message": msg, "url": url})
    bad = [i["label"] for i in items if i["status"] in BAD]
    return CheckResult(worst(i["status"] for i in items),
                       "כל הנקודות עונות" if not bad else "בעיה ב: " + ", ".join(bad), items=items)


@check("upstream.sources", component="מקורות חיצוניים", title="זמינות המקורות הממשלתיים",
       description=("האם המקורות עצמם עונים מהשרת: data.gov.il (ובשבועית גם קטלוג GovMap ודומיין ה-R2 הציבורי). "
                    "כך מבדילים בין 'המקור נפל' ל'הסקרייפר שלנו נשבר'. כשל כאן הוא אזהרה בלבד."),
       timeout=120)
async def check_upstreams(ctx: Ctx) -> CheckResult:
    targets = [
        ("data.gov.il", "data.gov.il (CKAN)", "https://data.gov.il/api/3/action/package_search?rows=0", '"success"', None),
    ]
    # The server itself is geo-blocked by knesset.gov.il by design now: the
    # mirror is fed by the worker fleet (knesset_sync_queue), and its health is
    # `knesset.mirror` below. Probe the feed from here only when a proxy says
    # the server is meant to reach it.
    if settings.knesset_proxy_url:
        targets.append(("knesset", "כנסת — ODATA (דרך ה-proxy)",
                        "https://knesset.gov.il/OdataV4/ParliamentInfo/KNS_KnessetDates?$top=1",
                        None, None))
    if ctx.deep:
        from app.services.govmap_coverage import CATALOG_URL, _catalog_headers
        targets.append(("govmap", "GovMap — קטלוג שכבות", CATALOG_URL, None, _catalog_headers()))
        pub = (settings.s3_public_base_url or "").rstrip("/")
        if pub:
            targets.append(("r2_public", "דומיין ההורדות (R2)", pub + "/", None, None))
    items = []
    # knesset_http.client: the Knesset probe goes the way the mirror does —
    # through KNESSET_PROXY_URL when set — so a green here means the mirror can
    # reach the feed. Other targets on this client still go direct.
    from app.services import knesset_http
    async with knesset_http.client(timeout=25, follow_redirects=True,
                                   headers={"User-Agent": _UA}) as client:
        for key, label, url, expect, hdrs in targets:
            st, msg, _ms = await _probe(client, url, expect=expect, headers=hdrs, max_bytes=8192)
            if key == "r2_public" and msg.startswith("HTTP 404"):
                st, msg = OK, "הדומיין עונה (404 על השורש — צפוי)"
            if st == FAIL:
                st = WARN  # the upstream, not us
            items.append({"key": key, "label": label, "status": st, "message": msg})
    bad = [i["label"] for i in items if i["status"] in BAD]
    return CheckResult(worst(i["status"] for i in items),
                       "כל המקורות עונים" if not bad else "לא עונים: " + ", ".join(bad), items=items)


_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def _run_script(script: str, *args: str, timeout: float = 420) -> tuple[int | None, str]:
    """Run one of scripts/*.py in a child process (its own memory, not ours)."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable, os.path.join(_ROOT, "scripts", script), *args,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        env={**os.environ, "PYTHONIOENCODING": "utf-8"},
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        return None, f"timeout after {int(timeout)}s"
    return proc.returncode, out.decode("utf-8", "replace")


def _failing_lines(output: str, limit: int = 25) -> list[str]:
    lines = [ln.strip() for ln in output.splitlines()]
    bad = [ln for ln in lines if "FAIL" in ln or "✗" in ln or "❌" in ln]
    return bad[:limit] or lines[-limit:]


@check("site.api_contract", component="אתר ו-API", title="חוזה ה-API הציבורי המתועד",
       description="מריץ את scripts/api_docs_test.py מול האתר החי: כל endpoint מתועד בדף ‎/api מחזיר את המבנה שהובטח.",
       tiers=("weekly",), timeout=480)
async def check_api_contract(ctx: Ctx) -> CheckResult:
    base = _public_base()
    if not base:
        return CheckResult(SKIP, "APP_BASE_URL מקומי")
    code, out = await _run_script("api_docs_test.py", "--base", base)
    if code == 0:
        return CheckResult(OK, "כל ההבטחות בדף ה-API מתקיימות", {"tail": out.splitlines()[-5:]})
    return CheckResult(FAIL, f"api_docs_test נכשל (exit {code})", {"failing": _failing_lines(out)})


@check("site.source_types", component="סקרייפרים", title="בדיקת קצה-לקצה לכל סוג מקור",
       description=("מריץ את scripts/source_health_check.py: לכל סוג מקור (mevaker, govmap, hatzav, idf, health, avodata, scraper, ckan) "
                    "לוקח את המאגר שנאסף אחרון ובודק שנאסף, שהקבצים בצורה הנכונה, ושהקובץ באמת יורד."),
       tiers=("weekly",), timeout=480)
async def check_source_types(ctx: Ctx) -> CheckResult:
    base = _public_base()
    if not base:
        return CheckResult(SKIP, "APP_BASE_URL מקומי")
    code, out = await _run_script("source_health_check.py", "--base", base)
    if code == 0:
        return CheckResult(OK, "כל סוגי המקורות עברו", {"tail": out.splitlines()[-12:]})
    return CheckResult(FAIL, f"source_health_check נכשל (exit {code})", {"failing": _failing_lines(out)})


# ═════════════════════════════════════════════════════════════════════════
# Runner
# ═════════════════════════════════════════════════════════════════════════

class HealthBusy(RuntimeError):
    pass


_lock = asyncio.Lock()
_background: set[asyncio.Task] = set()


def is_running() -> bool:
    return _lock.locked()


async def _run_one(c: CheckDef, ctx: Ctx) -> dict:
    t0 = time.monotonic()
    try:
        r = await asyncio.wait_for(c.fn(ctx), timeout=c.timeout)
        res = {"status": r.status, "message": r.message,
               "details": r.details, "items": r.items}
    except asyncio.TimeoutError:
        res = {"status": ERROR, "message": f"הבדיקה לא הסתיימה תוך {int(c.timeout)} שניות"}
    except Exception as e:  # noqa: BLE001 — one broken check must not end the run
        logger.exception("health check %s crashed", c.key)
        res = {"status": ERROR, "message": _clip(f"הבדיקה קרסה: {type(e).__name__}: {e}", 400)}
    res.update(key=c.key, component=c.component, title=c.title,
               duration_ms=int((time.monotonic() - t0) * 1000))
    return res


def _flat(results: list[dict]) -> dict[str, dict]:
    """key -> result, with items flattened as ``check/item``."""
    out = {}
    for r in results or []:
        out[r["key"]] = r
        for it in r.get("items") or []:
            out[f"{r['key']}/{it['key']}"] = it
    return out


def compare(results: list[dict], previous: dict[str, dict], now_iso: str) -> tuple[list, list]:
    """Stamp ``since`` on every bad result/item and say what is new and what is
    resolved, against ``previous`` (key -> the latest earlier verdict)."""
    new, resolved = [], []
    for key, cur in _flat(results).items():
        prev = previous.get(key)
        bad_now = cur.get("status") in BAD
        bad_before = bool(prev) and prev.get("status") in BAD
        if bad_now and bad_before:
            cur["since"] = prev.get("since") or now_iso
            if _RANK.get(cur["status"], 0) > _RANK.get(prev["status"], 0):
                new.append(key)  # got worse: warn -> fail
        elif bad_now:
            cur["since"] = now_iso
            new.append(key)
        elif bad_before and cur.get("status") == OK:
            resolved.append(key)
    return new, resolved


async def _previous_verdicts(db, keys: set[str], before) -> dict[str, dict]:
    """For each check key, its flattened verdicts from the latest finished run
    that ran it."""
    from app.models.health_run import HealthRun
    runs = (await db.execute(
        select(HealthRun.results).where(
            HealthRun.finished_at.isnot(None), HealthRun.status != "running",
            HealthRun.started_at < before)
        .order_by(HealthRun.started_at.desc()).limit(10)
    )).scalars().all()
    out: dict[str, dict] = {}
    found: set[str] = set()
    for results in runs:
        for r in results or []:
            if r.get("key") in keys and r["key"] not in found:
                found.add(r["key"])
                out.update({k: v for k, v in _flat([r]).items()})
        if found >= keys:
            break
    return out


async def run_checks(tier: str = "daily", keys: list[str] | None = None, *,
                     trigger: str = "scheduler", session_factory=None,
                     run_id=None) -> dict:
    """Run a tier (or the given checks) now and record it. Single-flight."""
    from app.models.health_run import HealthRun
    if session_factory is None:
        from app.database import async_session as session_factory
    if tier not in TIERS:
        raise ValueError(f"tier must be one of {TIERS}")
    defs = checks_for(tier, keys)
    if _lock.locked():
        raise HealthBusy("בדיקה אחרת כבר רצה")
    async with _lock:
        now = datetime.now(timezone.utc)
        async with session_factory() as db:
            # Close any run a restart cut off.
            await db.execute(update(HealthRun).where(
                HealthRun.status == "running",
                HealthRun.started_at < now - _STALE_RUN,
            ).values(status=ERROR, finished_at=now,
                     summary={"note": "הריצה נקטעה (הפעלה מחדש של השרת)"})
                .execution_options(synchronize_session=False))
            run = None
            if run_id is not None:
                run = await db.get(HealthRun, run_id)
            if run is None:
                run = HealthRun(tier=tier, trigger=trigger, status="running", started_at=now,
                                **({"id": run_id} if run_id else {}))
                db.add(run)
            await db.commit()
            run_id = run.id

        ctx = Ctx(deep=(tier == "weekly"), session_factory=session_factory)
        t0 = time.monotonic()
        results = []
        for c in defs:
            results.append(await _run_one(c, ctx))

        async with session_factory() as db:
            prev = await _previous_verdicts(db, {c.key for c in defs}, now)
            new, resolved = compare(results, prev, now.isoformat())
            counts = {s: 0 for s in (OK, WARN, FAIL, SKIP, ERROR)}
            for r in results:
                counts[r["status"]] = counts.get(r["status"], 0) + 1
            status = overall(r["status"] for r in results)
            summary = {**counts, "new_problems": new, "resolved": resolved,
                       "checks": [c.key for c in defs]}
            run = await db.get(HealthRun, run_id)
            run.status = status
            run.finished_at = datetime.now(timezone.utc)
            run.duration_ms = int((time.monotonic() - t0) * 1000)
            run.summary = summary
            run.results = results
            # retention
            await db.execute(delete(HealthRun).where(
                HealthRun.started_at < now - timedelta(days=max(settings.health_retention_days, 7)))
                .execution_options(synchronize_session=False))
            await db.commit()

        _forget_summary()
        # One line per non-ok verdict, so the app log alone says what a run found.
        for key, r in _flat(results).items():
            if r.get("status") in BAD:
                logger.warning("health %s · %s: %s — %s", tier, key, r.get("status"),
                               _clip(r.get("message"), 300))
                for line in ((r.get("details") or {}).get("failing") or [])[:15]:
                    logger.warning("health %s · %s   %s", tier, key, _clip(line, 300))
        log = logger.warning if status != OK else logger.info
        log("health %s run %s: %s (%s new problems, %s resolved)",
            tier, run_id, status, len(new), len(resolved))
        out = {"id": str(run_id), "tier": tier, "status": status, "summary": summary,
               "results": results}
        if new:
            await _alert(out)
        return out


def start_in_background(tier: str, keys: list[str] | None, trigger: str) -> str:
    """For the admin button: create the run row's id now, run it on the loop."""
    import uuid
    if _lock.locked():
        raise HealthBusy("בדיקה אחרת כבר רצה")
    checks_for(tier, keys)  # validate before returning an id
    rid = uuid.uuid4()

    async def go():
        try:
            await run_checks(tier, keys, trigger=trigger, run_id=rid)
        except HealthBusy:
            pass
        except Exception:  # noqa: BLE001
            logger.exception("health run %s failed", rid)

    t = asyncio.get_running_loop().create_task(go())
    _background.add(t)
    t.add_done_callback(_background.discard)
    return str(rid)


async def scheduled(tier: str) -> None:
    """Scheduler entry point: never raises."""
    try:
        await run_checks(tier, trigger="scheduler")
    except HealthBusy:
        logger.info("health %s skipped: another run is in progress", tier)
    except Exception:  # noqa: BLE001
        logger.exception("scheduled health %s run failed", tier)


# ── what /healthz says about the checks ─────────────────────────────────
# The outside monitor (.github/workflows/external-monitor.yml) cannot sign in,
# so /healthz carries the one fact it needs to notice that the checks stopped
# running: when each tier last finished, and its verdict. Cached, because
# /healthz is hit by every platform probe.

_SUMMARY_TTL = timedelta(minutes=5)
_summary: dict | None = None
_summary_at: datetime | None = None


async def _latest_by_tier(session_factory) -> dict:
    from app.models.health_run import HealthRun
    out = {}
    async with session_factory() as db:
        for tier in ("daily", "weekly"):
            r = (await db.execute(
                select(HealthRun.finished_at, HealthRun.status, HealthRun.summary)
                .where(HealthRun.tier == tier, HealthRun.finished_at.isnot(None),
                       HealthRun.status != "running")
                .order_by(HealthRun.started_at.desc()).limit(1)
            )).first()
            out[tier] = None if r is None else {
                "finished_at": _as_utc(r[0]).isoformat(),
                "status": r[1],
                "new_problems": len((r[2] or {}).get("new_problems") or []),
            }
    return out


async def public_summary(session_factory=None) -> dict | None:
    """{"daily": {...} | None, "weekly": {...} | None}; None if unreadable."""
    global _summary, _summary_at
    now = datetime.now(timezone.utc)
    if _summary is not None and _summary_at and now - _summary_at < _SUMMARY_TTL:
        return _summary
    if session_factory is None:
        from app.database import async_session as session_factory
    try:
        _summary = await asyncio.wait_for(_latest_by_tier(session_factory), timeout=5)
        _summary_at = now
    except Exception:  # noqa: BLE001 — /healthz must answer regardless
        logger.warning("health summary for /healthz unavailable", exc_info=True)
        return _summary
    return _summary


def _forget_summary() -> None:
    global _summary_at
    _summary_at = None


async def catch_up(session_factory=None) -> None:
    """After boot: run a tier whose last run is overdue (never ran, or the
    scheduled one was missed because the process was down at its hour).
    Daily if older than 26 h, weekly if older than 8 days."""
    if session_factory is None:
        from app.database import async_session as session_factory
    try:
        latest = await _latest_by_tier(session_factory)
    except Exception:  # noqa: BLE001
        logger.exception("health catch-up: cannot read the last runs")
        return
    now = datetime.now(timezone.utc)
    for tier, limit in (("daily", timedelta(hours=26)), ("weekly", timedelta(days=8))):
        last = latest.get(tier)
        if last and now - datetime.fromisoformat(last["finished_at"]) < limit:
            continue
        logger.info("health catch-up: running the overdue %s pass", tier)
        await scheduled(tier)


# ── alerting ────────────────────────────────────────────────────────────

def alert_text(run: dict) -> str:
    flat = _flat(run["results"])
    lines = [f"🩺 גרסאות לעם — בדיקה {run['tier']}: {run['status'].upper()} · "
             f"{len(run['summary']['new_problems'])} בעיות חדשות"]
    for key in run["summary"]["new_problems"][:15]:
        r = flat.get(key, {})
        lines.append(f"• [{r.get('status')}] {key}: {r.get('message', '')}")
    base = _public_base()
    if base:
        lines.append(f"{base}/admin#health")
    return "\n".join(lines)


async def _alert(run: dict) -> None:
    url = (settings.health_alert_webhook_url or "").strip()
    if not url:
        return
    import httpx
    body = alert_text(run)
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            # "text" for Slack / Google Chat / Mattermost, "content" for Discord.
            await client.post(url, json={"text": body, "content": body[:1900]})
    except Exception:  # noqa: BLE001 — an alert failing must not fail the run
        logger.exception("health alert webhook failed")
