"""The site's own index — one queryable row per tracked dataset.

``public.over_datasets`` (+ ``public.over_dataset_files``) answer, in SQL, the
question the site could previously only answer by clicking through it: for every
dataset גרסאות לעם tracks — what is it, how big, what formats does it hold, can
it be queried here or only downloaded, does it carry geometry, how far back does
its history go, and what is the id an API call needs.

Written by גרסאות לעם, so they sit with the other ``over_*`` index tables in the
/data console and can be JOINed against the data they describe:

    SELECT title, rows_latest, file_formats
      FROM over_datasets
     WHERE content_type = 'mapping_layer' AND sql_status = 'download_only'

is "which mapping layers can I not query yet", which nothing before this could
express.

WHAT THIS TABLE DELIBERATELY DOES NOT SAY
-----------------------------------------
How anything is COLLECTED. No source_type, no storage mode, no engine or worker,
no scraper configuration, no mirroring internals, no object-storage keys. Those
are implementation, they change without the data changing, and a public index
that exposes them invites questions about the plumbing rather than the holdings.

Every column here is a property of the DATA or of what the site can DO with it —
observable by any reader of the site, and stable across any change to how it
arrives. The one seam is ``sql_schema``, which is needed to write SQL against
the table and is a location, not a method.

REFRESH
-------
Rebuilt whole on a timer (see the scheduler) and on demand from the admin. It is
a derived view — nothing here is a source of truth, so a rebuild is always safe
and the table is never migrated, only replaced. TRUNCATE + INSERT rather than a
staging swap on purpose: the relation stays the same one, so the read-only
console role keeps its grant instead of needing a re-GRANT after every refresh.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import append_store, data_catalog
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

DATASETS_TABLE = "over_datasets"
FILES_TABLE = "over_dataset_files"

SITE = "https://www.over.org.il"

# Row-count bands. Chosen so the words mean the same thing to a reader as they
# do to the system: the break at 100k is where a mapping layer stops being
# something a browser can draw, which is the practical difference a user of this
# index cares about.
_SIZE_BANDS = ((10_000, "small"), (100_000, "medium"), (1_000_000, "large"))
_SIZE_LARGEST = "very_large"

# resource_mappings keys that are bookkeeping rather than files. Same list
# site_stats and the public API use — kept in sync by being about the DATA
# shape, not about any pipeline.
_NON_FILE_KEYS = frozenset({
    "_hashes", "_resource_ids", "_appendonly_seen", "_names", "_filedates",
    "_probes", "append_table", "_append_tables", "metadata",
})

# Formats carried by the list-valued mapping keys, which have no filename to
# read one off. Mirrors app/api/v1.py::_KEY_FORMATS.
_KEY_FORMATS = {
    "_geojson": "GeoJSON",
    "_gpkg": "GPKG",
    "_parquet": "GeoParquet",
    "_symbology": "ZIP",
    "_zip_parts": "ZIP",
    "_zip": "ZIP",
}

# Geometry lives under one of these column names wherever it is queryable.
_WKT_COLUMNS = {"geometry_wkt", "wkt", "geometry"}
_GEOM_COLUMN = "geom"


def _is_file_value(v) -> bool:
    """A mapping value that points at a real downloadable object: an ``r2:``
    marker or an ODATA resource id. The same test the public API applies."""
    return isinstance(v, str) and (v.startswith("r2:") or len(v) >= 30)


def _size_class(rows: int | None) -> str | None:
    if rows is None:
        return None
    for ceiling, label in _SIZE_BANDS:
        if rows < ceiling:
            return label
    return _SIZE_LARGEST


def _format_of(name: str) -> str | None:
    """Format from a resource name's extension, uppercased. None when the name
    carries no extension — most named CKAN resources do not."""
    if "." not in name:
        return None
    ext = name.rsplit(".", 1)[-1].strip().lower()
    if not ext or len(ext) > 8 or not ext.isalnum():
        return None
    return {"geojson": "GeoJSON", "gpkg": "GPKG", "parquet": "GeoParquet",
            "csv": "CSV", "xlsx": "XLSX", "json": "JSON", "pdf": "PDF",
            "zip": "ZIP", "xml": "XML", "txt": "TXT"}.get(ext, ext.upper())


def _files_of(mappings: dict | None, change: dict | None) -> list[dict]:
    """[{name, format}] for the files one version holds.

    Two sources, because neither is complete on its own: ``resource_mappings``
    knows every stored object but a named resource's format is only in its name,
    and ``change_summary.resources`` declares formats but only for the resources
    the source itself listed."""
    declared = {}
    for r in ((change or {}).get("resources") or []):
        if isinstance(r, dict) and r.get("name"):
            declared[str(r["name"])] = (r.get("format") or "").strip().upper() or None

    out: list[dict] = []
    for key, val in (mappings or {}).items():
        if key in _NON_FILE_KEYS:
            continue
        if isinstance(val, list):                       # aggregate key
            fmt = _KEY_FORMATS.get(key)
            out += [{"name": key.lstrip("_"), "format": fmt}
                    for v in val if _is_file_value(v)]
        elif _is_file_value(val):
            fmt = declared.get(key) or _format_of(key) or _KEY_FORMATS.get(key)
            out.append({"name": key, "format": fmt})
    return out


def _content_type(has_geometry: bool, formats: set[str], files: int,
                  queryable: bool) -> str:
    """What KIND of thing this dataset is, judged by what it holds.

    Judged from content on purpose — a mapping layer is a mapping layer whether
    it arrives as a table of geometry or as a GeoPackage."""
    if has_geometry or formats & {"GeoJSON", "GPKG", "GeoParquet", "SHP", "KML"}:
        return "mapping_layer"
    if formats & {"PDF", "DOC", "DOCX"} or (files > 5 and "ZIP" in formats):
        return "document_collection"
    if queryable or formats & {"CSV", "XLSX", "JSON", "XML"}:
        return "data_table"
    return "catalog_only"


def _geometry_status(content_type: str, columns: list[dict],
                     queryable: bool) -> str:
    """How far a spatial question can go on this dataset."""
    if content_type != "mapping_layer":
        return "not_applicable"
    if not queryable:
        return "download_only"
    names = {(c.get("name") or "").lower() for c in columns}
    if _GEOM_COLUMN in names:
        return "spatial_queries"
    if names & _WKT_COLUMNS:
        return "text_only"
    return "download_only"


async def _dataset_rows(db: AsyncSession) -> tuple[list[tuple], list[tuple]]:
    """Build every row of both tables. Pure read; touches no object storage."""
    datasets = (await db.execute(
        select(TrackedDataset)
        .where(TrackedDataset.status.in_(["active", "pending"]))
        .options(selectinload(TrackedDataset.tags))
        .order_by(TrackedDataset.title)
    )).scalars().all()
    if not datasets:
        return [], []
    ids = [d.id for d in datasets]

    # Latest version per dataset — the one whose contents the index describes.
    latest = {r[0]: r for r in (await db.execute(
        select(VersionIndex.tracked_dataset_id, VersionIndex.version_number,
               VersionIndex.detected_at, VersionIndex.resource_mappings,
               VersionIndex.change_summary)
        .where(VersionIndex.tracked_dataset_id.in_(ids))
        .distinct(VersionIndex.tracked_dataset_id)
        .order_by(VersionIndex.tracked_dataset_id,
                  VersionIndex.version_number.desc())
    )).all()}

    # How deep the history goes. One grouped query, no JSONB.
    history = {r[0]: (int(r[1]), r[2]) for r in (await db.execute(
        select(VersionIndex.tracked_dataset_id,
               func.count(VersionIndex.id),
               func.min(VersionIndex.detected_at))
        .where(VersionIndex.tracked_dataset_id.in_(ids))
        .group_by(VersionIndex.tracked_dataset_id)
    )).all()}

    # What of this is queryable, from the same catalog /data serves. Reused
    # rather than re-derived: the table-resolution rules live in one place.
    by_dataset: dict[str, list[dict]] = {}
    try:
        for rec in await data_catalog.build_catalog(db):
            if rec.get("dataset_id"):
                by_dataset.setdefault(rec["dataset_id"], []).append(rec)
    except Exception:  # noqa: BLE001 — an index without the SQL columns still helps
        logger.warning("site_index: catalog unavailable, rows will omit "
                       "queryability", exc_info=True)

    now = datetime.now(timezone.utc)
    ds_rows, file_rows = [], []
    for ds in datasets:
        did = str(ds.id)
        _, vnum, detected_at, mappings, change = latest.get(
            ds.id, (None, None, None, None, None))
        versions, first_at = history.get(ds.id, (0, None))

        files = _files_of(mappings, change)
        formats = sorted({f["format"] for f in files if f["format"]})

        # The catalog can hold several tables for one dataset (a multi-resource
        # NEON dataset); the biggest is the one worth pointing a reader at.
        recs = sorted(by_dataset.get(did, []),
                      key=lambda r: r.get("est_rows") or 0, reverse=True)
        rec = recs[0] if recs else None
        columns = (rec or {}).get("columns") or []
        queryable = rec is not None

        rows_latest = (change or {}).get("total_rows")
        rows_latest = int(rows_latest) if isinstance(rows_latest, (int, float)) else None
        sql_rows = (rec or {}).get("est_rows")

        has_geom_col = bool({(c.get("name") or "").lower() for c in columns}
                            & (_WKT_COLUMNS | {_GEOM_COLUMN}))
        content_type = _content_type(has_geom_col, set(formats), len(files),
                                     queryable)

        ds_rows.append((
            did,
            ds.title,
            ds.organization,
            ", ".join(t.name for t in (ds.tags or [])) or None,
            content_type,
            _size_class(rows_latest if rows_latest is not None else sql_rows),
            rows_latest,
            len(files),
            ", ".join(formats) or None,
            (rec or {}).get("schema"),
            (rec or {}).get("table"),
            int(sql_rows) if sql_rows is not None else None,
            "queryable" if queryable else ("download_only" if files else "none"),
            _geometry_status(content_type, columns, queryable),
            any(f["name"] == "geojson" for f in files),
            int(versions),
            first_at,
            detected_at,
            ds.last_polled_at,
            round((ds.poll_interval or 0) / 3600.0, 2) or None,
            ds.status,
            ds.source_gone_at is None,
            ds.import_warning,
            data_catalog._source_url(ds),
            f"{SITE}/versions/{did}",
            f"{SITE}/api/v1/datasets/{did}",
            now,
        ))

        for f in files:
            file_rows.append((did, ds.title, int(vnum) if vnum else None,
                              f["name"], f["format"], now))

    return ds_rows, file_rows


_DATASETS_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(DATASETS_TABLE)} (
    dataset_id            text PRIMARY KEY,
    title                 text,
    publisher             text,
    subject_tags          text,
    content_type          text,
    size_class            text,
    rows_latest           bigint,
    files_latest          integer,
    file_formats          text,
    sql_schema            text,
    sql_table             text,
    sql_rows              bigint,
    sql_status            text,
    geometry_status       text,
    map_preview           boolean,
    versions              integer,
    first_version_at      timestamptz,
    last_version_at       timestamptz,
    last_checked_at       timestamptz,
    -- double precision, not numeric: asyncpg maps numeric to decimal.Decimal
    -- and rejects the plain float this is built from.
    check_interval_hours  double precision,
    status                text,
    source_available      boolean,
    quality_note          text,
    source_url            text,
    page_url              text,
    api_url               text,
    refreshed_at          timestamptz
)
"""

_FILES_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(FILES_TABLE)} (
    dataset_id      text,
    title           text,
    version_number  integer,
    file_name       text,
    file_format     text,
    refreshed_at    timestamptz
)
"""

# COPY column order — must match the tuples _dataset_rows builds, in order.
_DATASETS_COLS = [
    "dataset_id", "title", "publisher", "subject_tags", "content_type",
    "size_class", "rows_latest", "files_latest", "file_formats", "sql_schema",
    "sql_table", "sql_rows", "sql_status", "geometry_status", "map_preview",
    "versions", "first_version_at", "last_version_at", "last_checked_at",
    "check_interval_hours", "status", "source_available", "quality_note",
    "source_url", "page_url", "api_url", "refreshed_at",
]
_FILES_COLS = ["dataset_id", "title", "version_number", "file_name",
               "file_format", "refreshed_at"]


async def refresh(db: AsyncSession) -> dict:
    """Rebuild both tables from the current state of the site."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}

    ds_rows, file_rows = await _dataset_rows(db)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_DATASETS_DDL)
        await conn.execute(_FILES_DDL)
        # Explicit, not left to ALTER DEFAULT PRIVILEGES: the default only
        # covers tables created by the role that set it, and an index the public
        # console cannot read is worth nothing. Idempotent.
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        if role:
            for t in (DATASETS_TABLE, FILES_TABLE):
                await conn.execute(
                    f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")
        # One transaction: a reader never sees the index half-replaced, and a
        # failure leaves the previous index in place rather than an empty table.
        async with conn.transaction():
            await conn.execute(
                f"TRUNCATE public.{_qi(DATASETS_TABLE)}, public.{_qi(FILES_TABLE)}")
            if ds_rows:
                await conn.copy_records_to_table(
                    DATASETS_TABLE, schema_name="public",
                    columns=_DATASETS_COLS, records=ds_rows)
            if file_rows:
                await conn.copy_records_to_table(
                    FILES_TABLE, schema_name="public",
                    columns=_FILES_COLS, records=file_rows)
        await conn.execute(f"ANALYZE public.{_qi(DATASETS_TABLE)}")

    # New tables on the first run ⇒ the /data catalog must not serve a stale list.
    data_catalog.invalidate_catalog_cache()
    logger.info("site_index: %d datasets, %d files", len(ds_rows), len(file_rows))
    return {"datasets": len(ds_rows), "files": len(file_rows)}
