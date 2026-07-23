"""Whole-site table catalog — the backend of the central /data SQL console.

Enumerates EVERY queryable table on over.org.il in one place:
  * every NEON dataset table in the append DB's ``public`` schema
    (``append_*`` — one per append_only dataset, or one per resource for
    multi-resource NEON datasets), plus
  * the 48 Knesset ODATA mirror tables in the ``knesset`` schema.

The /data page (frontend DataSqlPage) uses this to render a searchable,
source-grouped table browser + a per-table detail "cube" (sample rows, source
link, tags, raw-file download), and runs free SQL over both schemas at once
(search_path = public, knesset) through the least-privilege read-only role.

Nothing here writes: it reads the OVER app DB (tracked_datasets / version_index)
for metadata and the append DB (append_store helpers) for row estimates, columns
and samples. Row counts in the LIST are planner ESTIMATES (cheap); the DETAIL
cube computes the exact count for the single opened table.
"""
from __future__ import annotations

import asyncio
import logging
import time

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import append_store
from app.services.storage_client import dataset_archives_neon

logger = logging.getLogger(__name__)

# search_path handed to the read-only console so every schema resolves unqualified.
#
# `extensions` is last and is not a data schema — it holds PostGIS, installed
# there so its ~1,000 functions and spatial_ref_sys stay out of the catalog and
# the autocomplete. It has to be on the path anyway, or ST_AsText / ST_DWithin
# do not resolve and every spatial example in the help below is a syntax error.
# Last position means the three data schemas win any name collision.
CONSOLE_SEARCH_PATH = "public, knesset, idx, extensions"

# ── catalog cache ────────────────────────────────────────────────────────────
# build_catalog() is called on EVERY /data page load *and* on every detail-cube
# open (table_detail re-derives the whole catalog just to look up one row). Each
# call costs one full scan of tracked_datasets + its tags, a DISTINCT ON over
# version_index that materialises every dataset's resource_mappings JSONB, and
# two catalog queries against the append DB.
#
# Measured against a rollout-scale corpus (~2,900 index tables): ~3 s of DB work
# and ~2.6 MB of JSONB pulled into memory PER REQUEST — on a 512 MB dyno with a
# documented OOM history. Nothing here changes between polls, so a short
# process-local TTL cache removes essentially all of it. One process, so a plain
# dict beats Redis; the lock keeps a cold cache from being rebuilt N times
# concurrently.
_CACHE_TTL_SECONDS = 300.0
_catalog_cache: list[dict] | None = None
_catalog_cache_at: float = 0.0
_catalog_lock = asyncio.Lock()


def invalidate_catalog_cache() -> None:
    """Drop the cached catalog so the next read rebuilds it.

    Call after anything that adds/removes/renames a queryable table (a sync that
    swapped a table in, a dataset activation/deletion). Cheap and idempotent —
    when in doubt, call it; the TTL is the backstop for whatever forgets to."""
    global _catalog_cache, _catalog_cache_at
    _catalog_cache = None
    _catalog_cache_at = 0.0


def _source_url(ds: TrackedDataset) -> str:
    """Source URL for a tracked dataset (mirrors app.api.datasets._build_source_url;
    inlined to avoid an api→service import cycle)."""
    if ds.source_type in ("scraper", "govmap", "cbs") and ds.source_url:
        return ds.source_url
    org = ds.organization or ""
    name = ds.ckan_name or ""
    base = f"https://data.gov.il/he/datasets/{org}/{name}"
    if ds.resource_id:
        base = f"{base}/{ds.resource_id}"
    return base


def _dataset_is_neon(ds: TrackedDataset) -> bool:
    """True if this dataset accumulates queryable rows in the NEON append DB."""
    return ds.storage_mode == "append_only" or dataset_archives_neon(ds)


async def _latest_mappings(db: AsyncSession, dataset_ids: list) -> dict:
    """{tracked_dataset_id: (version_id, resource_mappings)} for the newest
    version of each dataset — one query via DISTINCT ON (…) ORDER BY version
    DESC. Datasets with no version yet are simply absent from the map."""
    if not dataset_ids:
        return {}
    rows = (await db.execute(
        select(
            VersionIndex.tracked_dataset_id,
            VersionIndex.id,
            VersionIndex.resource_mappings,
        )
        .where(VersionIndex.tracked_dataset_id.in_(dataset_ids))
        .distinct(VersionIndex.tracked_dataset_id)
        .order_by(VersionIndex.tracked_dataset_id, VersionIndex.version_number.desc())
    )).all()
    return {r[0]: (r[1], r[2] or {}) for r in rows}


def _tables_of(ds: TrackedDataset, mappings: dict) -> list[dict]:
    """Resolve the physical NEON table(s) of one dataset from its latest version's
    resource_mappings. Returns [{table, resource_name|None}] — one entry for a
    single-table dataset, several for a multi-resource NEON dataset. Falls back to
    the deterministic single-table name when no mapping exists yet."""
    multi = mappings.get("_append_tables")
    if isinstance(multi, dict) and multi:
        names = mappings.get("_names") or {}
        return [
            {"table": tbl, "resource_name": names.get(rid)}
            for rid, tbl in multi.items() if tbl
        ]
    single = mappings.get("append_table")
    if isinstance(single, str) and single:
        return [{"table": single, "resource_name": None}]
    return [{"table": append_store.table_name(ds), "resource_name": None}]


def _ds_record(ds: TrackedDataset, tbl: str, resource_name: str | None,
               version_id, est_rows: int | None, columns: list[dict]) -> dict:
    """One catalog row for a dataset table (public schema)."""
    title = ds.title or ds.ckan_name or tbl
    if resource_name:
        title = f"{title} — {resource_name}"
    return {
        "table": tbl,
        "schema": "public",
        "kind": "dataset",
        "title": title,
        "dataset_id": str(ds.id),
        "version_id": str(version_id) if version_id else None,
        "organization": ds.organization,
        "ckan_id": ds.ckan_id,
        "source_type": ds.source_type or "ckan",
        "source_url": _source_url(ds),
        "archive_url": f"/archive/{ds.id}",
        "versions_url": f"/versions/{ds.id}",
        "tags": [t.name for t in (ds.tags or [])],
        "columns": columns,
        "est_rows": est_rows,
    }


async def build_catalog(db: AsyncSession, *, use_cache: bool = True) -> list[dict]:
    """The unified, source-grouped table list for the /data browser.

    Served from a short-lived process-local cache (see _CACHE_TTL_SECONDS) since
    the underlying data only changes when a poll lands. Pass ``use_cache=False``
    to force a rebuild. Callers must treat the result as READ-ONLY — it is the
    cached list itself, not a copy."""
    global _catalog_cache, _catalog_cache_at
    if use_cache:
        cached, age = _catalog_cache, time.monotonic() - _catalog_cache_at
        if cached is not None and age < _CACHE_TTL_SECONDS:
            return cached
        async with _catalog_lock:
            # Another waiter may have rebuilt it while we queued for the lock.
            cached, age = _catalog_cache, time.monotonic() - _catalog_cache_at
            if cached is not None and age < _CACHE_TTL_SECONDS:
                return cached
            built = await _build_catalog_uncached(db)
            _catalog_cache, _catalog_cache_at = built, time.monotonic()
            return built
    return await _build_catalog_uncached(db)


async def _build_catalog_uncached(db: AsyncSession) -> list[dict]:
    """Dataset (public) tables first, then the Knesset schema tables. Row counts
    are planner estimates; a table with no physical rows yet (est is None) is
    still listed so a freshly-tracked dataset appears immediately."""
    datasets = (await db.execute(
        select(TrackedDataset)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .options(selectinload(TrackedDataset.tags))
        .order_by(TrackedDataset.title)
    )).scalars().all()
    neon_ds = [d for d in datasets if _dataset_is_neon(d)]
    mappings = await _latest_mappings(db, [d.id for d in neon_ds])

    # One cheap round-trip each for estimates + columns across ALL append tables.
    est = await append_store.list_public_tables()
    cols_by_table = await append_store.public_table_columns()

    out: list[dict] = []
    seen: set[str] = set()
    for ds in neon_ds:
        version_id, maps = mappings.get(ds.id, (None, {}))
        for t in _tables_of(ds, maps):
            tbl = t["table"]
            if tbl in seen:
                continue
            # Only surface tables that physically exist (have columns in the DB).
            columns = cols_by_table.get(tbl)
            if not columns:
                continue
            seen.add(tbl)
            out.append(_ds_record(ds, tbl, t["resource_name"], version_id,
                                   est.get(tbl), columns))

    out.extend(await _index_records(db, datasets))
    out.extend(await _knesset_records())
    return out


async def _index_records(db: AsyncSession, datasets: list[TrackedDataset]) -> list[dict]:
    """Catalog rows for the ``idx`` schema — the mirrored index CSVs of
    scraper/govmap datasets (kind='index').

    These are the tables that let /data search INSIDE a collection: a GovMap
    layer's feature attributes, an FOI dataset's item + file index. One table per
    dataset, holding its LATEST version only (history stays in R2)."""
    from app.services import index_mirror
    try:
        mirrored = await index_mirror.list_tables()
    except Exception:  # noqa: BLE001 — never let this break the whole catalog
        logger.debug("data_catalog: idx list_tables failed", exc_info=True)
        return []
    if not mirrored:
        return []

    by_id = {str(d.id): d for d in datasets}
    cols_by_table = await append_store.schema_table_columns(index_mirror.SCHEMA)
    recs: list[dict] = []
    for m in mirrored:
        ds = by_id.get(m["dataset_id"])
        if ds is None:
            continue                      # dataset deleted/paused since the sync
        columns = cols_by_table.get(m["table"])
        if not columns:
            continue                      # table gone (dropped out of band)
        recs.append({
            "table": m["table"],
            "schema": index_mirror.SCHEMA,
            "kind": "index",
            "title": ds.title or ds.ckan_name or m["table"],
            "dataset_id": m["dataset_id"],
            "version_id": None,
            "organization": ds.organization,
            "ckan_id": ds.ckan_id,
            "source_type": ds.source_type or "scraper",
            "source_url": _source_url(ds),
            "archive_url": f"/archive/{ds.id}",
            "versions_url": f"/versions/{ds.id}",
            "tags": [t.name for t in (ds.tags or [])],
            "columns": columns,
            "est_rows": m.get("rows"),
        })
    return recs


async def _knesset_records() -> list[dict]:
    """Catalog rows for the Knesset schema tables (kind='knesset')."""
    from app.services import knesset_db
    if not knesset_db.is_configured():
        return []
    try:
        tables = await knesset_db.list_tables()
    except Exception:  # noqa: BLE001 — the mirror may not have initialised yet
        logger.debug("data_catalog: knesset list_tables failed", exc_info=True)
        return []
    recs: list[dict] = []
    for t in tables:
        recs.append({
            "table": t["table"],
            "schema": "knesset",
            "kind": "knesset",
            "title": t.get("entity_set") or t["table"],
            "description": t.get("description") or "",
            "group": t.get("group"),
            "source_type": "knesset",
            "source_url": "https://main.knesset.gov.il/activity/info/pages/databases.aspx",
            "page_url": "/knesset?tab=sql",
            "tags": [],
            "columns": t.get("columns") or [],
            "est_rows": t.get("total_rows"),
        })
    return recs


# ── copy-to-AI schema ────────────────────────────────────────────────────────

# Order the schemas are presented in: the curated tables first, then the
# auto-generated collection indexes (the long tail).
_SCHEMA_ORDER = ["public", "knesset", "idx"]
_SCHEMA_LABEL = {
    "public": "מאגרי נתונים (data.gov.il ומקורות נוספים)",
    "knesset": "מסד הנתונים של הכנסת",
    "idx": "אינדקסים של אוספים (שכבות GovMap, מסמכי חופש מידע ועוד)",
}


async def schema_text_all(db: AsyncSession, *, schema: str | None = None) -> str:
    """The whole queryable catalog as compact DDL — what the copy-to-AI button
    hands an assistant so it can write SQL over the site.

    ONE LINE per table rather than a multi-line CREATE TABLE block. The verbose
    form is nicer to read but this catalog is ~370 tables and ~3,800 columns
    today and grows with every mirrored collection; the compact form keeps the
    whole thing pasteable while staying valid DDL. Per-table detail is still
    available from the browser (``/schema.txt?table=…``).

    ``schema`` narrows the dump to one schema for anyone who only needs a slice.
    """
    catalog = await build_catalog(db)
    if schema:
        catalog = [r for r in catalog if r["schema"] == schema]

    lines = [
        "-- גרסאות לעם (over.org.il) — סכימת מסד הנתונים לכתיבת SQL",
        "-- קריאה בלבד: משפט SELECT / WITH יחיד.",
        "-- search_path = " + CONSOLE_SEARCH_PATH + " — אפשר לכתוב שם טבלה בלי",
        "-- שם הסכימה, אלא אם השם מופיע ביותר מסכימה אחת.",
        "-- שם עם אות גדולה, עברית או מילה שמורה חייב מרכאות כפולות — הוא מוצג",
        "-- למטה בדיוק בצורה שבה יש לכתוב אותו.",
        "-- טבלאות knesset: הכול באותיות קטנות (KNS_Bill ← kns_bill).",
        "-- כל העמודות בסכימת idx הן text; המירו לפי הצורך (col::numeric, col::date).",
        "-- שכבות ממ\"ג: geometry_wkt היא טקסט. אם קיימת גם עמודת geom (טיפוס",
        "--   geometry, EPSG:4326) אפשר לשאול שאלות מרחביות:",
        "--   ST_AsText(geom) לקריאת הגיאומטריה — בחירת geom עצמה מחזירה hex.",
        "--   geom && ST_MakeEnvelope(34.7,31.9,35.0,32.2,4326) לסינון לפי מלבן.",
        "--   ST_DWithin(geom::geography, ST_SetSRID(ST_MakePoint(34.78,32.08),4326)::geography, 500)",
        "--   למרחק במטרים (בלי ::geography המרחק יוצא במעלות וחסר משמעות).",
    ]
    by_schema: dict[str, list[dict]] = {}
    for rec in catalog:
        by_schema.setdefault(rec["schema"], []).append(rec)

    order = {s: i for i, s in enumerate(_SCHEMA_ORDER)}
    for sch in sorted(by_schema, key=lambda s: order.get(s, len(order))):
        recs = sorted(by_schema[sch], key=lambda r: r["table"])
        label = _SCHEMA_LABEL.get(sch, sch)
        lines.append("")
        lines.append(f"-- ═══ {sch} — {label} ({len(recs)} טבלאות) ═══")
        for rec in recs:
            cols = rec.get("columns") or []
            if not cols:
                continue
            body = ", ".join(
                f"{append_store._ident_ref(c['name'])} {c.get('type') or 'text'}"
                for c in cols
            )
            ref = f"{sch}.{append_store._ident_ref(rec['table'])}"
            title = (rec.get("title") or "").strip().replace("\n", " ")
            comment = f"  -- {title}" if title else ""
            lines.append(f"CREATE TABLE {ref} ({body});{comment}")
    return "\n".join(lines).rstrip() + "\n"


# ── Detail cube ──────────────────────────────────────────────────────────────

def _internal_key(k: str) -> bool:
    """resource_mappings keys that are bookkeeping, not user-facing file
    resources (see app/api/versions.py _extract_resource_ids)."""
    return k.startswith("_") or k in ("metadata",)


def _files_of(version_id, mappings: dict) -> list[dict]:
    """[{name, url}] direct raw-file download links for the latest version's
    named resources, via the existing /versions/{vid}/download/{key} route.
    Empty for NEON-only datasets (no file snapshot)."""
    if not version_id or not mappings:
        return []
    files: list[dict] = []
    for key, val in mappings.items():
        if _internal_key(key) or not val:
            continue
        files.append({
            "name": key,
            "url": f"/api/versions/{version_id}/download/{key}",
        })
    return files


async def table_detail(table: str, db: AsyncSession) -> dict | None:
    """Full detail for one table (the /data cube): metadata + sample rows + exact
    row count + raw-file links. Returns None if ``table`` is not a known catalog
    table (the security gate — callers 404 on None)."""
    catalog = await build_catalog(db)
    rec = next((r for r in catalog if r["table"] == table), None)
    if rec is None:
        return None

    schema = rec["schema"]
    sample = await append_store.sample_rows(table, schema=schema, limit=20)

    if rec["kind"] == "knesset":
        rec = {**rec, "row_count": rec.get("est_rows"), "files": [],
               "sample": sample, "csv_export": True}
        return rec

    if rec["kind"] == "index":
        # Mirrored index CSV: the raw file lives on R2 and is reachable from the
        # dataset's versions page, so no per-version download links here.
        try:
            count = await append_store.table_count(table, schema=schema)
        except Exception:  # noqa: BLE001
            count = rec.get("est_rows")
        return {**rec, "row_count": count, "files": [], "sample": sample,
                "csv_export": True}

    # Dataset table — exact count + raw-file links from the latest version.
    try:
        count = await append_store.table_count(table)
    except Exception:  # noqa: BLE001
        count = rec.get("est_rows")
    version_id = rec.get("version_id")
    # Re-fetch this dataset's latest mappings for the raw-file links (one row).
    from uuid import UUID
    lm = await _latest_mappings(db, [UUID(rec["dataset_id"])])
    _, maps = next(iter(lm.values()), (None, {}))
    return {**rec, "row_count": count, "files": _files_of(version_id, maps),
            "sample": sample,
            "csv_url": f"/api/append/{rec['dataset_id']}/download.csv"}
