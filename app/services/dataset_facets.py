"""Search facets shared by the home list, the public API and the MCP server:
what KINDS of fields a dataset holds, and what TYPES of files it archives.

Content fields are not classified here. They are the content field-flags that
app/services/field_flags.py computes from column names and stores on
``tracked_datasets.field_flags`` — the same flags the /data console shows as
chips. This module only gives them public names (``locality`` for
``has_locality``) so the three surfaces speak one vocabulary.

File formats are derived from the files a dataset's versions actually hold:
the extension each stored object kept in its key (see
storage_client._safe_filename), the format a list-valued mapping key declares
(``_geojson``, ``_gpkg`` …), and the format a scraper declared per resource in
change_summary. They are stored on ``tracked_datasets.file_formats`` (a list
of the codes below) and kept current by ``sync_file_formats``, which only
re-reads datasets that gained a version since their last sync.

Filter semantics, the same on every surface: several content fields AND
together ("has a locality AND a date"), several file formats OR together
("CSV or Excel") — a dataset rarely holds both a CSV and an XLSX of the same
data, so AND over formats would answer almost nothing.
"""
from __future__ import annotations

import logging
import re
from datetime import datetime, timezone

from sqlalchemy import Text, cast, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset

logger = logging.getLogger(__name__)

# public name -> (field_flags key, Hebrew label). Order is display order.
CONTENT_FIELDS: dict[str, tuple[str, str]] = {
    "locality": ("has_locality", "יישוב"),
    "authority": ("has_authority", "רשות מקומית"),
    "ministry": ("has_ministry", "משרד ממשלתי"),
    "date": ("has_date", "תאריך"),
    "geometry": ("has_geometry", "מידע מרחבי"),
    "parcel": ("has_parcel", "גוש/חלקה"),
    # Computed from the data, not from column names: a GovMap layer with at
    # least one feature outside both Israel and איו"ש (app/services/govmap_abroad.py).
    "abroad": ("has_abroad", "אלמנטים בחו\"ל"),
}

# format code -> label. Order is display order.
FILE_FORMATS: dict[str, str] = {
    "csv": "CSV",
    "xlsx": "Excel",
    "json": "JSON",
    "geojson": "GeoJSON",
    "shp": "Shapefile",
    "gpkg": "GeoPackage",
    "parquet": "Parquet",
    "kml": "KML",
    "pdf": "PDF",
    "docx": "Word",
    "xml": "XML",
    "txt": "TXT",
    "zip": "ZIP",
}

# Extension or declared format (lower-cased) -> format code.
_FORMAT_ALIASES: dict[str, str] = {
    "csv": "csv", "tsv": "csv",
    "xlsx": "xlsx", "xls": "xlsx", "xlsm": "xlsx", "excel": "xlsx", "ods": "xlsx",
    "json": "json",
    "geojson": "geojson",
    "shp": "shp", "shapefile": "shp", "esri shapefile": "shp",
    "gpkg": "gpkg", "geopackage": "gpkg",
    "parquet": "parquet", "geoparquet": "parquet",
    "kml": "kml", "kmz": "kml",
    "pdf": "pdf",
    "docx": "docx", "doc": "docx", "word": "docx",
    "xml": "xml",
    "txt": "txt",
    "zip": "zip",
}

# List-valued mapping keys whose format the key itself declares (mirrors
# app/api/v1._KEY_FORMATS). `_symbology` is left out on purpose: it is the
# style bundle every GovMap layer carries, not the data, and counting it would
# make "ZIP" mean "a GovMap layer".
_KEY_FORMATS = {"_geojson": "geojson", "_gpkg": "gpkg", "_parquet": "parquet",
                "_zip_parts": "zip", "_zip": "zip"}

# Mapping keys that never name a file. `_appendonly_seen` is the big one: a
# list of row hashes that can run to hundreds of thousands of entries.
_SKIP_KEYS = ("_hashes", "_resource_ids", "_appendonly_seen", "_names",
              "_filedates", "_probes", "_symbology", "_append_tables",
              "append_table")

_EXT_RE = re.compile(r"\.([a-z0-9]{2,8})(?:\.gz)?$")


def format_code(value: str | None) -> str | None:
    """A declared format ("CSV", "ESRI Shapefile") or an extension → code, or None."""
    if not value:
        return None
    return _FORMAT_ALIASES.get(str(value).strip().lower().lstrip("."))


def _ext_code(name: str) -> str | None:
    m = _EXT_RE.search(name.lower())
    return format_code(m.group(1)) if m else None


def formats_of_version(mappings: dict | None, change_summary: dict | None) -> set[str]:
    """Format codes of the files one version holds."""
    out: set[str] = set()
    for key, value in (mappings if isinstance(mappings, dict) else {}).items():
        if key in _SKIP_KEYS:
            continue
        if key in _KEY_FORMATS:
            if value:
                out.add(_KEY_FORMATS[key])
            continue
        values = value if isinstance(value, list) else [value]
        for v in values:
            if not isinstance(v, str) or not v:
                continue
            # An R2 value keeps the original extension in its key; a bare ODATA
            # resource id does not, but the mapping key is the resource NAME,
            # which often does ("נתונים.xlsx").
            code = (_ext_code(v) if v.startswith("r2:") else None) or _ext_code(key)
            if code:
                out.add(code)
    cs = change_summary or {}
    for res in (cs.get("resources") or []) + (cs.get("resources_modified") or []):
        if isinstance(res, dict) and (code := format_code(res.get("format"))):
            out.add(code)
    return out


def ordered_formats(codes) -> list[str]:
    """Codes in display order, unknown codes dropped."""
    s = set(codes or ())
    return [c for c in FILE_FORMATS if c in s]


def content_fields_of(field_flags: dict | None) -> list[str]:
    """Public names of the content fields a dataset's flags say it has."""
    flags = field_flags or {}
    return [name for name, (key, _) in CONTENT_FIELDS.items() if flags.get(key) is True]


def parse_content_fields(values) -> list[str]:
    """Normalize user input ('locality', 'has_locality', 'locality,date') to public
    names. Raises ValueError on an unknown name so a typo is not a silent no-op."""
    out: list[str] = []
    for raw in values or ():
        for part in str(raw).split(","):
            name = part.strip().lower()
            if not name:
                continue
            if name.startswith("has_"):
                name = name[4:]
            if name not in CONTENT_FIELDS:
                raise ValueError(
                    f"unknown content field {part.strip()!r}; "
                    f"expected one of: {', '.join(CONTENT_FIELDS)}")
            if name not in out:
                out.append(name)
    return out


def parse_file_formats(values) -> list[str]:
    """Normalize user input ('CSV', 'xls', 'csv,pdf') to format codes; raises
    ValueError on an unknown format."""
    out: list[str] = []
    for raw in values or ():
        for part in str(raw).split(","):
            if not part.strip():
                continue
            code = format_code(part)
            if not code:
                raise ValueError(
                    f"unknown file format {part.strip()!r}; "
                    f"expected one of: {', '.join(FILE_FORMATS)}")
            if code not in out:
                out.append(code)
    return out


def matches(content_fields, file_formats, fields: list[str], formats: list[str]) -> bool:
    """In-memory form of the filter (the home list's cache), over a dataset's
    content_fields_of() names and format codes: every field, any format."""
    if not set(fields) <= set(content_fields or ()):
        return False
    if formats and not set(formats) & set(file_formats or ()):
        return False
    return True


def apply_filters(stmt, fields: list[str], formats: list[str], dialect: str):
    """SQL form of the filter, for a select over TrackedDataset.

    On Postgres both are containment tests (``@>``), which a GIN index on the
    column answers; elsewhere (the SQLite the tests build) the portable JSON
    accessors stand in.
    """
    if dialect == "postgresql":
        if fields:
            stmt = stmt.where(TrackedDataset.field_flags.contains(
                {CONTENT_FIELDS[f][0]: True for f in fields}))
        if formats:
            stmt = stmt.where(or_(*(TrackedDataset.file_formats.contains([c]) for c in formats)))
        return stmt
    for f in fields:
        stmt = stmt.where(TrackedDataset.field_flags[CONTENT_FIELDS[f][0]].as_boolean().is_(True))
    if formats:
        stmt = stmt.where(or_(*(cast(TrackedDataset.file_formats, Text).like(f'%"{c}"%')
                                for c in formats)))
    return stmt


# ── keeping file_formats current ────────────────────────────────────────────

_BATCH = 200


async def _formats_for(db: AsyncSession, ids: list) -> dict:
    """{dataset_id: set(codes)} over every version of the given datasets.

    The mapping is trimmed in SQL (``- _SKIP_KEYS``) before it leaves the
    database, so a row-archive's hash list never reaches this process."""
    rows = (await db.execute(
        text(
            "SELECT tracked_dataset_id, resource_mappings - CAST(:skip AS text[]), "
            "jsonb_build_object('resources', change_summary->'resources', "
            "'resources_modified', change_summary->'resources_modified') "
            "FROM version_index WHERE tracked_dataset_id = ANY(CAST(:ids AS uuid[]))"
        ),
        {"skip": list(_SKIP_KEYS), "ids": ids},
    )).all()
    out: dict = {i: set() for i in ids}
    for did, mappings, cs in rows:
        out.setdefault(did, set()).update(formats_of_version(mappings, cs))
    return out


async def sync_file_formats(db: AsyncSession, *, full: bool = False, limit: int = 5000) -> dict:
    """Recompute file_formats for datasets that gained a version since their
    last sync (or every dataset with ``full``). Postgres only — it trims the
    JSONB server-side. Returns stats."""
    if full:
        q = select(TrackedDataset.id)
    else:
        q = select(TrackedDataset.id).where(text(
            "file_formats_at IS NULL OR EXISTS (SELECT 1 FROM version_index v "
            "WHERE v.tracked_dataset_id = tracked_datasets.id "
            "AND v.detected_at > tracked_datasets.file_formats_at)"))
    ids = list((await db.execute(q.limit(limit))).scalars().all())
    changed = 0
    for i in range(0, len(ids), _BATCH):
        chunk = ids[i:i + _BATCH]
        computed = await _formats_for(db, chunk)
        now = datetime.now(timezone.utc)
        current = dict((await db.execute(
            select(TrackedDataset.id, TrackedDataset.file_formats)
            .where(TrackedDataset.id.in_(chunk)))).all())
        for did in chunk:
            new = ordered_formats(computed.get(did))
            if new != list(current.get(did) or []):
                changed += 1
            await db.execute(
                TrackedDataset.__table__.update()
                .where(TrackedDataset.__table__.c.id == did)
                # updated_at pinned: a derived column is not an edit of the dataset.
                .values(file_formats=new, file_formats_at=now,
                        updated_at=TrackedDataset.__table__.c.updated_at))
        await db.commit()
    stats = {"datasets_scanned": len(ids), "datasets_changed": changed}
    if ids:
        logger.info("dataset_facets.sync_file_formats: %s", stats)
    return stats
