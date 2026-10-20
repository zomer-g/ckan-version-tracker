"""Admin view of where the SQL database's bytes go.

  GET /api/admin/storage            — every table with its size, attributed to
                                      a project, a source and (when it has one)
                                      the dataset that owns it
  GET /api/admin/storage?refresh=1  — the same, bypassing the 10-minute cache
  GET/POST /api/admin/storage/hash-compaction[/start|/stop]
                                    — convert the dedup hashes to uuid
                                      (app/services/hash_compaction.py)
  GET/POST /api/admin/storage/index-migration[/start|/stop]
                                      partial dedup indexes for row validity
                                      (app/services/index_migration.py)
  GET  /api/admin/storage/history   — closed rows exported to Parquet on R2:
                                      the job, the audit, the catalog by dataset
  POST /api/admin/storage/history/run|stop, GET /history/audit
                                      (app/services/history_export.py)
  GET  /api/admin/storage/history/query-status, POST /history/query-smoke
                                    — the DuckDB history engine
                                      (app/services/history_query.py)
  POST/GET /api/admin/storage/closures/reopen
                                    — reopen rows a non-closing source closed
                                      (append_store.reopen_closures)
  GET  /api/admin/storage/history/candidates, PATCH /history/windows,
  POST /history/cold/{id}, POST /history/rehydrate/{id}
                                    — the cold tier and window recommendations
                                      (history_export, phase 4)
  GET  /api/admin/storage/orphans   — the append tables whose dataset is gone
  POST /api/admin/storage/orphans/drop
                                    — drop them (re-derived at call time)

The breakdown itself (by project / source / schema / dataset) is done in the
browser from the one table list, so every grouping and every drill-down adds
up to the same total and no second query is needed to switch views.

ATTRIBUTION
-----------
A dataset's tables carry its id in their name: ``append_<base>_<dsid8>``,
``append_<base>_<dsid8>_<rid8>`` (append_store.table_name*) and, in ``idx``,
``<base>_<dsid8>_<hash8>`` (index_mirror). Scanning the name's segments from the
right for a known ``dsid8`` recovers the dataset for all three shapes — the
same rule scripts/db_classify_versions.py uses. An ``append_`` table whose
dataset no longer exists is reported as an orphan: it is space nothing reads.

Everything that is not a dataset table belongs to a project by schema or by
name prefix (``ocal``, ``over_re_*`` …), see :func:`project_of`.

The report is read-only (catalog functions); the hash compaction and the orphan
drop write. Dropping an orphan removes nothing a dataset can reach: a table is
an orphan only when NO tracked dataset, in any status, carries its id octet.
"""
import logging
import time
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.database import get_db
from app.models.tracked_dataset import TrackedDataset
from app.models.user import User
from app.services.source_load import source_key

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/storage", tags=["admin-storage"])

_CACHE_TTL = 600.0
_cache: dict = {"at": 0.0, "data": None}

# Dataset tables: the whole corpus of גרסאות לעם, one table (or one per
# resource) per tracked dataset. Grouped further by source in the UI.
PROJECT_DATASETS = "datasets"
PROJECT_INDEXES = "indexes"
PROJECT_ORPHANS = "orphans"

# key -> Hebrew label. The UI shows the label; the key is stable for tests.
PROJECT_LABELS = {
    PROJECT_DATASETS: "גרסאות לעם · טבלאות מאגרים",
    PROJECT_INDEXES: "גרסאות לעם · אינדקסי אוספים (idx)",
    PROJECT_ORPHANS: "טבלאות יתומות (המאגר נמחק)",
    "nadlan": "נדל\"ן לעם (over_re_*)",
    "ocal": "יומן לעם",
    "ocoi": "ניגוד עניינים לעם",
    "odata": "מידע לעם (ייבוא)",
    "knesset": "מראה הכנסת",
    "reference": "מפתחות יישובים ורשויות",
    "site_index": "אינדקס האתר ופרופיילר",
    "system": "מערכת (אפליקציה, הרשאות, הרחבות)",
    "other": "אחר",
}

_SCHEMA_PROJECT = {
    "ocal": "ocal",
    "ocoi": "ocoi",
    "odata": "odata",
    "knesset": "knesset",
    "app": "system",
    "auth": "system",
    "extensions": "system",
    "_loader": "system",
}

# public tables that are not dataset tables, by name prefix (first match wins).
_PUBLIC_PREFIX_PROJECT = (
    ("over_re_", "nadlan"),
    ("over_settlement", "reference"),
    ("over_authorit", "reference"),
    ("over_dataset", "site_index"),
    ("over_govmap_layer", "site_index"),
    ("over_column", "site_index"),
    ("over_table", "site_index"),
    ("alembic_version", "system"),
    ("spatial_ref_sys", "system"),
)


def dsid_of(table: str, known: set[str]) -> str | None:
    """The dataset id prefix (first 8 hex, no dashes) a table name carries."""
    for seg in reversed(table.split("_")):
        if len(seg) == 8 and seg in known:
            return seg
    return None


def orphan_tables(tables: list[dict], known: set[str]) -> list[dict]:
    """The ``public.append_*`` tables whose dataset no longer exists, largest
    first. ``tables`` is :func:`_catalog`'s list, ``known`` the dsid8 set of
    EVERY tracked dataset (any status, pending requests included).

    Only the ``append_`` shape qualifies: an index-mirror table in ``idx`` is
    rebuilt by its own job and a project table never carries a dataset octet,
    so neither is anyone's to drop here."""
    out = []
    for t in tables:
        if t["schema"] != "public" or not t["name"].startswith("append_"):
            continue
        if dsid_of(t["name"], known) is not None:
            continue
        est = t.get("est_rows")
        out.append({
            "table": t["name"],
            "total": int(t.get("total") or 0),
            "est_rows": int(est) if est is not None and est >= 0 else None,
        })
    out.sort(key=lambda r: -r["total"])
    return out


def project_of(schema: str, table: str, has_dataset: bool) -> str:
    """Which project a table's bytes belong to (a PROJECT_LABELS key)."""
    if schema == "idx":
        return PROJECT_INDEXES
    if schema in _SCHEMA_PROJECT:
        return _SCHEMA_PROJECT[schema]
    if schema == "public":
        if table.startswith("append_"):
            return PROJECT_DATASETS if has_dataset else PROJECT_ORPHANS
        for prefix, project in _PUBLIC_PREFIX_PROJECT:
            if table.startswith(prefix):
                return project
    return "other"


async def _catalog() -> tuple[int | None, list[dict]]:
    from app.services import append_store

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        db_bytes = await conn.fetchval("SELECT pg_database_size(current_database())")
        rows = await conn.fetch(
            """
            SELECT n.nspname AS schema, c.relname AS name,
                   pg_total_relation_size(c.oid) AS total,
                   pg_relation_size(c.oid)       AS heap,
                   pg_indexes_size(c.oid)        AS idx,
                   c.reltuples::bigint           AS est_rows
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relkind IN ('r', 'm', 'p')
              AND n.nspname NOT IN ('pg_catalog', 'information_schema')
              AND n.nspname NOT LIKE 'pg\\_%'
            """
        )
    return db_bytes, [dict(r) for r in rows]


async def _datasets(db: AsyncSession) -> dict[str, dict]:
    """dsid8 -> dataset summary, for every tracked dataset whatever its status."""
    res = await db.execute(select(
        TrackedDataset.id, TrackedDataset.title, TrackedDataset.ckan_id,
        TrackedDataset.source_type, TrackedDataset.status, TrackedDataset.organization,
    ))
    out: dict[str, dict] = {}
    for id_, title, ckan_id, st, status, org in res.all():
        dsid8 = str(id_).replace("-", "")[:8]
        # A prefix collision is vanishingly rare; the first one wins and the
        # table is still counted, only possibly under a sibling's title.
        out.setdefault(dsid8, {
            "id": str(id_),
            "title": title,
            "source": source_key(ckan_id, st),
            "status": status,
            "org": org,
        })
    return out


async def build_report(db: AsyncSession) -> dict:
    db_bytes, tables = await _catalog()
    by_prefix = await _datasets(db)
    known = set(by_prefix)

    out_tables: list[list] = []
    used: dict[str, dict] = {}
    for t in tables:
        schema, name = t["schema"], t["name"]
        dsid = None
        if schema == "idx" or (schema == "public" and name.startswith("append_")):
            dsid = dsid_of(name, known)
        project = project_of(schema, name, dsid is not None)
        ds = by_prefix.get(dsid) if dsid else None
        if ds:
            used[ds["id"]] = ds
        total = int(t["total"] or 0)
        heap = int(t["heap"] or 0)
        idx = int(t["idx"] or 0)
        est = int(t["est_rows"]) if t["est_rows"] is not None and t["est_rows"] >= 0 else None
        # Compact rows: ~2.7k tables, so a list beats repeating the keys.
        out_tables.append([
            schema, name, total, heap, idx, max(total - heap - idx, 0), est,
            project, ds["id"] if ds else None,
        ])

    out_tables.sort(key=lambda r: -r[2])
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "database_bytes": int(db_bytes) if db_bytes is not None else None,
        "columns": ["schema", "table", "total", "heap", "indexes", "toast",
                    "est_rows", "project", "dataset_id"],
        "tables": out_tables,
        "datasets": used,
        "projects": PROJECT_LABELS,
    }


@router.get("")
async def storage_report(
    refresh: bool = Query(False),
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_admin_user),
):
    now = time.monotonic()
    if not refresh and _cache["data"] is not None and now - _cache["at"] < _CACHE_TTL:
        return {**_cache["data"], "cached": True}
    try:
        data = await build_report(db)
    except Exception as e:  # noqa: BLE001 — surface the DB error to the admin
        logger.exception("storage report failed")
        raise HTTPException(status_code=502, detail=f"קריאת גדלי הטבלאות נכשלה: {e}")
    _cache.update(at=now, data=data)
    return {**data, "cached": False}


# ── Shrinking: drop the orphans ──────────────────────────────────────────────

async def _orphans(db: AsyncSession) -> tuple[list[dict], int]:
    _, tables = await _catalog()
    known = set(await _datasets(db))
    rows = orphan_tables(tables, known)
    return rows, sum(r["total"] for r in rows)


@router.get("/orphans")
async def orphans_report(
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_admin_user),
):
    """Every append table whose dataset is gone, with its size. Read-only."""
    rows, total = await _orphans(db)
    return {"tables": rows, "count": len(rows), "total_bytes": total}


@router.post("/orphans/drop")
async def orphans_drop(
    db: AsyncSession = Depends(get_db),
    user: User = Depends(get_admin_user),
):
    """Drop the orphan tables. The list is derived again HERE, against the
    datasets table as it is now, never taken from the client or a cache: a
    dataset re-added between the report and the click keeps its table."""
    from app.services import append_store
    from app.services.data_catalog import invalidate_catalog_cache

    rows, total = await _orphans(db)
    dropped: list[str] = []
    failed: list[str] = []
    freed = 0
    for r in rows:
        try:
            await append_store.drop_table(r["table"])
            dropped.append(r["table"])
            freed += r["total"]
        except Exception as e:  # noqa: BLE001 — one failure must not strand the rest
            failed.append(f"{r['table']}: {type(e).__name__}: {e}")
            logger.warning("orphan drop: %s failed: %s", r["table"], e)
    invalidate_catalog_cache()
    _cache.update(at=0.0, data=None)
    logger.info("Orphan tables dropped by %s: %d of %d, %d bytes", user.email,
                len(dropped), len(rows), freed)
    return {"dropped": dropped, "failed": failed, "count": len(rows),
            "freed_bytes": freed, "total_bytes": total}


# ── Shrinking: compact the dedup hashes ──────────────────────────────────────

@router.get("/hash-compaction")
async def hash_compaction_status(_: User = Depends(get_admin_user)):
    """How many hash columns are still text, and the running job's progress."""
    from app.services import hash_compaction
    return await hash_compaction.summary()


@router.post("/hash-compaction/start")
async def hash_compaction_start(_: User = Depends(get_admin_user)):
    import asyncio
    from app.services import hash_compaction
    if hash_compaction.status()["running"]:
        return {"started": False, "reason": "already running"}
    asyncio.create_task(hash_compaction.run())
    return {"started": True}


@router.post("/hash-compaction/stop")
async def hash_compaction_stop(_: User = Depends(get_admin_user)):
    from app.services import hash_compaction
    hash_compaction.request_stop()
    return {"stopping": True}


# ── Row validity: make the dedup indexes partial (WHERE last_seen IS NULL) ───

@router.get("/index-migration")
async def index_migration_status(_: User = Depends(get_admin_user)):
    """How many append tables still dedup with a full index, and the job's progress."""
    from app.services import index_migration
    return await index_migration.summary()


@router.post("/index-migration/start")
async def index_migration_start(_: User = Depends(get_admin_user)):
    import asyncio
    from app.services import index_migration
    if index_migration.status()["running"]:
        return {"started": False, "reason": "already running"}
    asyncio.create_task(index_migration.run())
    return {"started": True}


@router.post("/index-migration/stop")
async def index_migration_stop(_: User = Depends(get_admin_user)):
    from app.services import index_migration
    index_migration.request_stop()
    return {"stopping": True}


# ── History: closed rows to Parquet on R2 (docs/history-phase2-plan.md) ──────

@router.get("/history")
async def history_status(_: User = Depends(get_admin_user),
                         db: AsyncSession = Depends(get_db)):
    """The export job's state, the last audit, and the catalog per dataset
    (parts, rows in R2, rows deleted from SQL, bytes, window)."""
    from app.services import history_export
    return await history_export.admin_summary(db)


def simulated_clock(as_of: str | None):
    """The ``as_of`` override of a manual export run as an aware UTC datetime,
    or None. A bare date is the end of that UTC day (append_store.parse_as_of).
    Only a FUTURE clock is accepted: the override exists to exercise a month
    before it is due, never to re-plan the past with a shorter cutoff."""
    from datetime import datetime, timezone

    from app.services import append_store
    if as_of is None or not str(as_of).strip():
        return None
    try:
        now = append_store.parse_as_of(as_of)
    except ValueError as e:
        raise HTTPException(status_code=422, detail=f"as_of: {e}") from e
    if now < datetime.now(timezone.utc):
        raise HTTPException(status_code=409,
                            detail="as_of must be in the future: the simulated clock "
                                   "only moves forward")
    return now


@router.post("/history/run")
async def history_run(dataset_id: str | None = Query(None),
                      as_of: str | None = Query(
                          None, description="simulated clock (ISO date or datetime, UTC); "
                                            "must be in the future"),
                      _: User = Depends(get_admin_user)):
    """Run the export now (all datasets, or one). Deletes only where a dataset
    has history_window_days, exactly as the daily job would. ``as_of`` runs the
    same plan under a future clock, so a month can be exported and its rows
    past the window deleted before the real job would get to it."""
    import asyncio
    from app.services import history_export
    now = simulated_clock(as_of)
    if history_export.status()["running"]:
        return {"started": False, "reason": "already running"}
    ok, why = history_export.is_available()
    if not ok:
        raise HTTPException(status_code=409, detail=why)
    asyncio.create_task(history_export.run(only_dataset=dataset_id, now=now))
    return {"started": True, "as_of": now.isoformat() if now else None}


@router.post("/history/stop")
async def history_stop(_: User = Depends(get_admin_user)):
    from app.services import history_export
    history_export.request_stop()
    return {"stopping": True}


@router.get("/history/audit")
async def history_audit(_: User = Depends(get_admin_user)):
    """HEAD every catalog key in R2 now; lists the keys that are missing."""
    from app.services import history_export
    return await history_export.audit_history_parts()


# ── History query engine (docs/history-phase3-plan.md) ───────────────────────

@router.get("/history/query-status")
async def history_query_status(_: User = Depends(get_admin_user)):
    """The DuckDB engine: available or why not, the extensions, how the hot
    tables are read, the gate (running / waiting) and counters since boot."""
    from app.services import history_query
    return history_query.status()


@router.post("/history/query-smoke")
async def history_query_smoke(table: str = Query(..., description="an append table with a Parquet mirror"),
                              _: User = Depends(get_admin_user)):
    """End to end without any history part: count(*) of the table's existing
    Parquet mirror on R2 and of the hot table, both through the engine."""
    from app.services import history_query
    try:
        return await history_query.smoke(table)
    except history_query.EngineBusy as e:
        raise HTTPException(status_code=429, detail=str(e))
    except history_query.HistoryQueryError as e:
        raise HTTPException(status_code=400, detail=str(e))


# ── Wrong closures (docs/history-phase4-plan.md section 0) ───────────────────
#
# A source that does not close by its effective append_closes_rows (prices)
# got closed rows anyway; this reopens them. One background job at a time, its
# progress in memory like index_migration: a real run over 196 tables and ~20M
# rows takes far longer than a request.

_reopen: dict = {"running": False, "dry_run": None, "started_at": None,
                 "finished_at": None, "current": None, "target": None,
                 "tables": [], "refused": [], "failed": [], "totals": {}}


def reopen_targets(datasets: list, *, source: str | None = None,
                   dataset_id: str | None = None) -> tuple[list, list[dict]]:
    """``(datasets to reopen, refused [{dataset_id, title, reason}])``. Pure but
    for the registry cache: a dataset is a target only when it matches and its
    effective append_closes_rows is False (append_store.reopen_refusal)."""
    from app.services import append_store

    ok, refused = [], []
    for ds in datasets:
        cfg = ds.scraper_config or {}
        if dataset_id is not None and str(ds.id) != str(dataset_id):
            continue
        if source is not None and cfg.get("kind") != source:
            continue
        why = append_store.reopen_refusal(ds)
        if why:
            refused.append({"dataset_id": str(ds.id), "title": ds.title, "reason": why})
        else:
            ok.append(ds)
    return ok, refused


def _reopen_totals(tables: list[dict]) -> dict:
    keys = ("closed", "duplicates", "deleted", "reopened")
    return {k: sum(int(t.get(k) or 0) for t in tables) for k in keys} | {"tables": len(tables)}


async def _reopen_run(ds_ids: list[str], dry_run: bool, actor: str | None) -> None:
    from app.database import async_session
    from app.services import (activity_log, append_store, append_tables, history_export,
                              sampling_runs)

    try:
        async with async_session() as db:
            datasets = (await db.execute(
                select(TrackedDataset).where(TrackedDataset.id.in_(
                    [uuid.UUID(i) for i in ds_ids])))).scalars().all()
            plan = [(ds, await append_tables.resolve_tables(ds, db)) for ds in datasets]
        pool = await append_store.get_pool()
        for ds, tables in plan:
            stamp = sampling_runs.stamp_column(ds)
            done: list[dict] = []
            for t in tables:
                table = t["table"]
                _reopen["current"] = table
                try:
                    async with pool.acquire() as conn:
                        ident = await history_export._table_ident(conn, table)
                    if ident is None:
                        raise RuntimeError("no partial dedup index")
                    cols = await append_store.user_columns(table)
                    res = await append_store.reopen_closures(
                        table, dry_run=dry_run, ident_col=ident[0],
                        stamp_col=stamp if stamp and stamp in cols else None)
                    res["dataset_id"] = str(ds.id)
                    _reopen["tables"].append(res)
                    done.append(res)
                except Exception as e:  # noqa: BLE001 — one table must not stop the rest
                    logger.warning("closures reopen: %s failed: %s", table, e)
                    _reopen["failed"].append({"table": table, "dataset_id": str(ds.id),
                                              "error": f"{type(e).__name__}: {e}"[:400]})
                _reopen["totals"] = _reopen_totals(_reopen["tables"])
            if not dry_run:
                tot = _reopen_totals(done)
                await activity_log.log_event(
                    event="closures_reopened", dataset=ds, status="ok", actor=actor,
                    message=(f"פתיחה מחדש של סגירות שגויות: {tot['reopened']:,} שורות נפתחו, "
                             f"{tot['deleted']:,} עותקים כפולים נמחקו ב-{tot['tables']} טבלאות"))
    except Exception:  # noqa: BLE001 — surface in status, never crash the app
        logger.exception("closures reopen: run aborted")
        _reopen["failed"].append({"table": _reopen["current"], "error": "aborted, see log"})
    finally:
        _reopen.update(running=False, current=None,
                       finished_at=datetime.now(timezone.utc).isoformat())


@router.get("/closures/reopen")
async def closures_reopen_status(_: User = Depends(get_admin_user)):
    """The last (or running) reopen job: per table closed / duplicates counted,
    and for a real run deleted / reopened, plus refused datasets and failures."""
    return {k: v for k, v in _reopen.items()}


@router.post("/closures/reopen")
async def closures_reopen(source: str | None = Query(None),
                          dataset_id: str | None = Query(None),
                          dry_run: bool = Query(True),
                          user: User = Depends(get_admin_user),
                          db: AsyncSession = Depends(get_db)):
    """Reopen the rows a source closed although its effective
    ``append_closes_rows`` is false: per identity keep the earliest row and
    delete the later copies, set ``last_seen`` back to NULL in ctid batches,
    VACUUM. ``dry_run`` (the default) only counts. Refused for a dataset whose
    closing is in force. Runs in the background; GET shows the progress."""
    import asyncio

    if (source is None) == (dataset_id is None):
        raise HTTPException(status_code=400, detail="pass exactly one of source / dataset_id")
    if _reopen["running"]:
        return {"started": False, "reason": "already running", **_reopen}
    datasets = (await db.execute(select(TrackedDataset))).scalars().all()
    ok, refused = reopen_targets(datasets, source=source, dataset_id=dataset_id)
    if not ok:
        raise HTTPException(status_code=409, detail={
            "error": "nothing to reopen", "refused": refused,
            "message": "אין מאגרים מתאימים: רק מקור שה-append_closes_rows האפקטיבי שלו false"})
    _reopen.update(running=True, dry_run=bool(dry_run),
                   started_at=datetime.now(timezone.utc).isoformat(), finished_at=None,
                   current=None, target={"source": source, "dataset_id": dataset_id},
                   tables=[], refused=refused, failed=[], totals={})
    asyncio.create_task(_reopen_run([str(d.id) for d in ok], bool(dry_run),
                                    getattr(user, "email", None)))
    return {"started": True, "datasets": len(ok), "refused": refused, "dry_run": bool(dry_run)}


# ── The cold tier and window recommendations (docs/history-phase4-plan.md) ───

@router.get("/history/candidates")
async def history_candidates(_: User = Depends(get_admin_user),
                             db: AsyncSession = Depends(get_db)):
    """Datasets nothing updated for 180 days and nobody read through the API
    for 90 (candidates for the cold tier), and per dataset the closed rows
    older than 90 days a window would move, with the bytes it would free.
    Shows, never decides; also carries the tier job's state."""
    from app.services import history_export
    return await history_export.candidates(db)


@router.patch("/history/windows")
async def history_windows(body: dict, _: User = Depends(get_admin_user),
                          db: AsyncSession = Depends(get_db)):
    """``{"items": [{"dataset_id", "days"}]}``: set (days >= 30) or clear
    (null) ``history_window_days`` on many datasets at once. Validated whole
    and written in one commit: one bad item and nothing changes (400)."""
    from app.services import history_export
    try:
        pairs = history_export.validate_window_items((body or {}).get("items") or [])
        return {"updated": await history_export.apply_windows(db, pairs)}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/history/cold/{dataset_id}")
async def history_cold(dataset_id: str, confirm_rehydrate: bool = Query(False),
                       user: User = Depends(get_admin_user)):
    """Move every table of the dataset to ``full`` Parquet parts on R2 and empty
    it in SQL (verified first). Allowed for a paused dataset or one polled less
    than monthly; otherwise only with ``confirm_rehydrate=true``, which makes
    the next poll bring it back to SQL before loading. Background job; GET
    /history (``tier_job``) or /history/candidates show its progress."""
    import asyncio
    from app.services import history_export
    ok, why = history_export.is_available()
    if not ok:
        raise HTTPException(status_code=409, detail=why)
    if history_export.tier_status()["running"]:
        raise HTTPException(status_code=409, detail="another tier move is running")
    try:
        await history_export.check_cold_move(dataset_id, confirm_rehydrate=confirm_rehydrate)
    except history_export.ColdRefused as e:
        raise HTTPException(status_code=409, detail=str(e))
    asyncio.create_task(history_export.move_to_cold(
        dataset_id, confirm_rehydrate=confirm_rehydrate, actor=getattr(user, "email", None)))
    return {"started": True, "dataset_id": dataset_id, "confirm_rehydrate": confirm_rehydrate}


@router.post("/history/compact")
async def history_compact(_: User = Depends(get_admin_user)):
    """TRUNCATE every cold-tier table that is already empty in SQL, so the
    space its indexes still hold is given back (a plain VACUUM never shrinks
    an index). Lists what was compacted, skipped (rows left) and failed."""
    from app.services import history_export
    if history_export.tier_status()["running"]:
        raise HTTPException(status_code=409, detail="another tier move is running")
    return await history_export.compact_cold_tables()


_access_log_job: dict = {"running": False, "started_at": None, "finished_at": None,
                         "purged": None, "before": None, "after": None, "error": None}


async def _compact_access_log() -> None:
    """Purge past the retention, then ``VACUUM FULL`` the log so the space
    leaves the database (a DELETE alone only frees it for reuse). The writer
    drops a batch it cannot insert during the lock, which is acceptable for a
    log; no request reads this table."""
    from sqlalchemy import text

    from app.database import engine
    from app.services import api_access_log
    _access_log_job.update(running=True, started_at=datetime.now(timezone.utc).isoformat(),
                           finished_at=None, purged=None, before=None, after=None, error=None)
    try:
        async with engine.connect() as c:
            _access_log_job["before"] = int((await c.execute(
                text("SELECT pg_total_relation_size('app.api_access_log')"))).scalar())
        _access_log_job["purged"] = await api_access_log.purge_old()
        async with engine.connect() as c:
            c2 = await c.execution_options(isolation_level="AUTOCOMMIT")
            await c2.execute(text("VACUUM FULL app.api_access_log"))
            _access_log_job["after"] = int((await c2.execute(
                text("SELECT pg_total_relation_size('app.api_access_log')"))).scalar())
    except Exception as e:  # noqa: BLE001 — surface in status
        logger.exception("access log compact failed")
        _access_log_job["error"] = f"{type(e).__name__}: {e}"[:400]
    finally:
        _access_log_job.update(running=False,
                               finished_at=datetime.now(timezone.utc).isoformat())


@router.get("/access-log/compact")
async def access_log_compact_status(_: User = Depends(get_admin_user)):
    return dict(_access_log_job)


@router.post("/access-log/compact")
async def access_log_compact(_: User = Depends(get_admin_user)):
    """Background: purge the API access log past API_ACCESS_LOG_RETENTION_DAYS
    and VACUUM FULL it. GET for the result (bytes before and after)."""
    import asyncio
    if _access_log_job["running"]:
        return {"started": False, "reason": "already running"}
    asyncio.create_task(_compact_access_log())
    return {"started": True}


@router.post("/history/rehydrate/{dataset_id}")
async def history_rehydrate(dataset_id: str, user: User = Depends(get_admin_user)):
    """Bring a cold dataset back to SQL: COPY its parts, verify count and hash
    against the catalog, and only then clear ``history_tier``. Background job."""
    import asyncio
    from app.services import history_export
    if history_export.tier_status()["running"]:
        raise HTTPException(status_code=409, detail="another tier move is running")
    asyncio.create_task(history_export.run_rehydrate(dataset_id,
                                                     actor=getattr(user, "email", None)))
    return {"started": True, "dataset_id": dataset_id}
