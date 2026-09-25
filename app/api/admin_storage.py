"""Admin view of where the SQL database's bytes go.

  GET /api/admin/storage            — every table with its size, attributed to
                                      a project, a source and (when it has one)
                                      the dataset that owns it
  GET /api/admin/storage?refresh=1  — the same, bypassing the 10-minute cache
  GET/POST /api/admin/storage/hash-compaction[/start|/stop]
                                    — convert the dedup hashes to uuid
                                      (app/services/hash_compaction.py)
  GET/POST /api/admin/storage/offload/{dataset_id}
                                    — move a dataset's rows to CSV files on R2
                                      (app/services/sql_offload.py)

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

The report is read-only (catalog functions); the two shrinking actions write.
"""
import logging
import time
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


# ── Shrinking: compact hashes, move rows to files ───────────────────────────

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


@router.get("/offload/{dataset_id}")
async def offload_plan(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_admin_user),
):
    """Dry run of moving a dataset's rows to files, plus any run's progress."""
    from app.api.utils import parse_uuid
    from app.services import sql_offload
    ds_uuid = parse_uuid(dataset_id)
    try:
        plan = await sql_offload.plan(db, ds_uuid)
    except LookupError:
        raise HTTPException(status_code=404, detail="המאגר לא נמצא")
    return {**plan, "run": sql_offload.status(str(ds_uuid))}


@router.post("/offload/{dataset_id}")
async def offload_start(
    dataset_id: str,
    db: AsyncSession = Depends(get_db),
    _: User = Depends(get_admin_user),
):
    """Move a dataset's SQL tables to CSV files on R2, in the background."""
    import asyncio
    from app.api.utils import parse_uuid
    from app.database import async_session
    from app.services import sql_offload
    ds_uuid = parse_uuid(dataset_id)
    run = sql_offload.status(str(ds_uuid))
    if run and run.get("state") == "running":
        return {"started": False, "reason": "already running", "run": run}
    try:
        plan = await sql_offload.plan(db, ds_uuid)
    except LookupError:
        raise HTTPException(status_code=404, detail="המאגר לא נמצא")
    if not plan["tables"]:
        raise HTTPException(status_code=409, detail="למאגר אין טבלאות SQL להעביר")

    async def _go():
        async with async_session() as s:
            await sql_offload.offload(s, ds_uuid)
        _cache["data"] = None  # the storage report is stale now

    asyncio.create_task(_go())
    return {"started": True, "tables": len(plan["tables"]), "bytes": plan["bytes"]}
