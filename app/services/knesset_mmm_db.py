"""MMM (מרכז המחקר והמידע) document METADATA → the ``knesset`` SQL schema.

The MMM corpus is tracked as an OVER scraper dataset (``knesset-mmm-*``): each
version carries the PDFs (zipped on R2) plus a "נתוני הסורק" CSV cataloging
every document (rid, title, doc_type, date, author, approver, requested_by,
keywords, abstract, incident/pdf links). This module mirrors that CSV into
``knesset.mmm_documents`` so the /knesset SQL console, the MMM tab search and
the MCP ``run_sql`` can query the catalog alongside the ODATA tables.

Source of truth = the dataset's LATEST version (the scraper re-emits the full
catalog each poll). Sync is versioned: ``sync_state.last_id`` stores the last
loaded version_number, so the tick-time check is a single cheap SELECT until a
new version lands — and after the first read that SELECT is served from a
process-local cache, so a steady-state tick never touches the append DB at all
(_loaded_version). Only metadata is mirrored — the PDFs stay on
fs.knesset.gov.il / R2.
"""
from __future__ import annotations

import csv
import io
import logging
import re
from datetime import date, datetime, timezone

import httpx
from sqlalchemy import select

from app.services import append_store
from app.services.knesset_db import PG_SCHEMA, _qi, _qtable

logger = logging.getLogger(__name__)

TABLE = "mmm_documents"
ENTITY_SET = "MMM_Documents"  # synthetic name for sync_state / the catalog
CSV_RESOURCE_KEY = "נתוני הסורק"

# Process-local cache of the loaded version_number — see _loaded_version().
_loaded_version_cache: int | None = None

# CSV header → (column, pg_type). `date` is re-parsed into a real DATE column
# (date_text keeps the original string).
COLUMNS: list[tuple[str, str]] = [
    ("rid", "integer"),
    ("title", "text"),
    ("doc_type", "text"),
    ("date", "date"),
    ("date_text", "text"),
    ("date_hebrew", "text"),
    ("author", "text"),
    ("approver", "text"),
    ("requested_by", "text"),
    ("keywords", "text"),
    ("abstract", "text"),
    ("incident_url", "text"),
    ("pdf_url", "text"),
    ("attachment_filename", "text"),
    ("attachment_url", "text"),
]

_HEB_MONTHS = {
    "ינואר": 1, "פברואר": 2, "מרץ": 3, "מרס": 3, "אפריל": 4, "מאי": 5,
    "יוני": 6, "יולי": 7, "אוגוסט": 8, "ספטמבר": 9, "אוקטובר": 10,
    "נובמבר": 11, "דצמבר": 12,
}


def parse_hebrew_date(s: str | None) -> date | None:
    """'07 ביולי 2026' → date(2026, 7, 7)."""
    if not s:
        return None
    m = re.search(r"(\d{1,2})\s+ב?([א-ת]+)\s+(\d{4})", s)
    if not m:
        return None
    month = _HEB_MONTHS.get(m.group(2))
    if not month:
        return None
    try:
        return date(int(m.group(3)), month, int(m.group(1)))
    except ValueError:
        return None


async def _latest_csv_source() -> tuple[int, str] | None:
    """(version_number, public CSV url) of the newest MMM version, or None."""
    from app.database import async_session
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex
    from app.services.storage_client import is_storage_value, storage_client

    async with async_session() as db:
        rows = (await db.execute(
            select(VersionIndex.version_number, VersionIndex.resource_mappings)
            .join(TrackedDataset, TrackedDataset.id == VersionIndex.tracked_dataset_id)
            .where(TrackedDataset.ckan_name.like("knesset-mmm%"))
            .order_by(VersionIndex.detected_at.desc())
            .limit(5)
        )).all()
    for vnum, mappings in rows:
        mapped = (mappings or {}).get(CSV_RESOURCE_KEY)
        if mapped and is_storage_value(mapped):
            return int(vnum), storage_client.public_url(mapped)
    return None


async def _loaded_version() -> int | None:
    """version_number currently in knesset.mmm_documents, cached in process.

    sync_if_due() runs on the 3-minute knesset scheduler tick, and this SELECT
    used to be its unconditional cost: one append-DB round-trip every 3 minutes
    forever, which on Neon is enough on its own to keep the compute from ever
    scaling to zero (the 5-minute idle window never elapses — see the note on
    knesset_db._next_due_at for the measured price of that).

    The loaded version only changes when THIS process writes it, so a
    process-local cache is exact rather than merely cheap: any other writer
    would be a second dyno, and its write would be a no-op replay of the same
    CSV. None = not read yet; a miss costs the same single SELECT as before."""
    global _loaded_version_cache
    if _loaded_version_cache is not None:
        return _loaded_version_cache
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            v = await conn.fetchval(
                f"SELECT last_id FROM {_qtable('sync_state')} WHERE table_name = $1", TABLE)
        except Exception:  # schema/sync_state not created yet
            return None
    _loaded_version_cache = int(v) if v is not None else None
    return _loaded_version_cache


async def _ensure_table(conn) -> None:
    defs = [f"{_qi(c)} {t} PRIMARY KEY" if c == "rid" else f"{_qi(c)} {t}"
            for c, t in COLUMNS]
    defs.append('"_synced_at" timestamptz NOT NULL DEFAULT now()')
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(PG_SCHEMA)}")
    await conn.execute(
        f"CREATE TABLE IF NOT EXISTS {_qtable(TABLE)} ({', '.join(defs)})")


def _row_values(r: dict) -> list | None:
    try:
        rid = int((r.get("rid") or "").strip())
    except ValueError:
        return None
    d_text = (r.get("date") or "").strip()
    vals: list = []
    for col, _t in COLUMNS:
        if col == "rid":
            vals.append(rid)
        elif col == "date":
            vals.append(parse_hebrew_date(d_text))
        elif col == "date_text":
            vals.append(d_text or None)
        else:
            v = (r.get(col) or "").strip()
            vals.append(v.replace("\x00", "") or None)
    return vals


async def sync_if_due(force: bool = False) -> dict:
    """Load the MMM catalog CSV when a version newer than the loaded one exists
    (or on force). Cheap no-op otherwise — and on the no-op path it now costs
    ZERO append-DB queries, only the app-DB version lookup."""
    global _loaded_version_cache
    src = await _latest_csv_source()
    if not src:
        return {"skipped": "no MMM version with a catalog CSV"}
    vnum, url = src
    if force:
        _loaded_version_cache = None   # admin re-load: re-read the real state
    loaded = await _loaded_version()
    if not force and loaded is not None and loaded >= vnum:
        return {"skipped": f"version {vnum} already loaded"}

    async with httpx.AsyncClient(timeout=httpx.Timeout(180.0, connect=20.0),
                                 follow_redirects=True) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig")
    rows = [v for r in csv.DictReader(io.StringIO(text)) if (v := _row_values(r))]
    if not rows:
        return {"skipped": "catalog CSV empty"}

    cols = [c for c, _ in COLUMNS]
    cols_sql = ", ".join(_qi(c) for c in cols)
    update_sql = ", ".join(f"{_qi(c)}=EXCLUDED.{_qi(c)}" for c in cols if c != "rid")
    n = len(cols)
    max_rows = max(1, append_store._MAX_PARAMS // n)

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_table(conn)
        total = 0
        for i in range(0, len(rows), max_rows):
            chunk = rows[i:i + max_rows]
            groups, params = [], []
            for vals in chunk:
                ph = ", ".join(f"${len(params) + j + 1}" for j in range(n))
                groups.append(f"({ph})")
                params.extend(vals)
            await conn.execute(
                f"INSERT INTO {_qtable(TABLE)} ({cols_sql}) VALUES {', '.join(groups)} "
                f'ON CONFLICT ("rid") DO UPDATE SET {update_sql}, "_synced_at"=now()',
                *params)
            total += len(chunk)
        count = int(await conn.fetchval(f"SELECT count(*) FROM {_qtable(TABLE)}"))
        now = datetime.now(timezone.utc)
        import json as _json
        await conn.execute(f"""
            INSERT INTO {_qtable('sync_state')}
                (table_name, entity_set, columns, full_loaded, last_id,
                 source_count, total_rows, status, error, last_synced_at, updated_at)
            VALUES ($1, $2, $3, true, $4, $5, $6, 'ok', NULL, $7, now())
            ON CONFLICT (table_name) DO UPDATE SET
                columns = EXCLUDED.columns, full_loaded = true,
                last_id = EXCLUDED.last_id, source_count = EXCLUDED.source_count,
                total_rows = EXCLUDED.total_rows, status = 'ok', error = NULL,
                last_synced_at = EXCLUDED.last_synced_at, updated_at = now()
        """, TABLE, ENTITY_SET, _json.dumps([[c, t] for c, t in COLUMNS]),
            vnum, len(rows), count, now)
    _loaded_version_cache = vnum   # committed above — keep the cache exact
    logger.info("knesset_mmm_db: loaded catalog v%d — %d rows (%d in table)",
                vnum, total, count)
    return {"loaded_version": vnum, "rows": total, "total": count}


async def search(q: str | None, author: str | None, doc_type: str | None,
                 year_from: int | None, year_to: int | None,
                 limit: int = 20, offset: int = 0) -> dict:
    """Parameterized metadata search for the MMM tab (title/keywords/abstract)."""
    conds, params = [], []
    if (q or "").strip():
        params.append(f"%{q.strip()}%")
        n = len(params)
        conds.append(f"(title ILIKE ${n} OR keywords ILIKE ${n} OR abstract ILIKE ${n})")
    if (author or "").strip():
        params.append(f"%{author.strip()}%")
        conds.append(f"(author ILIKE ${len(params)} OR approver ILIKE ${len(params)})")
    if (doc_type or "").strip():
        params.append(doc_type.strip())
        conds.append(f"doc_type = ${len(params)}")
    if year_from is not None:
        params.append(int(year_from))
        conds.append(f'extract(year FROM "date") >= ${len(params)}')
    if year_to is not None:
        params.append(int(year_to))
        conds.append(f'extract(year FROM "date") <= ${len(params)}')
    where = ("WHERE " + " AND ".join(conds)) if conds else ""
    limit = max(1, min(int(limit or 20), 50))
    offset = max(int(offset or 0), 0)

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        total = int(await conn.fetchval(
            f"SELECT count(*) FROM {_qtable(TABLE)} {where}", *params))
        rows = await conn.fetch(
            f'SELECT rid, title, doc_type, "date", date_text, author, approver, '
            f"requested_by, keywords, abstract, incident_url, pdf_url "
            f"FROM {_qtable(TABLE)} {where} "
            f'ORDER BY "date" DESC NULLS LAST, rid DESC LIMIT {limit} OFFSET {offset}',
            *params)
    items = []
    for r in rows:
        d = dict(r)
        if d.get("date") is not None:
            d["date"] = str(d["date"])
        items.append(d)
    return {"total": total, "items": items, "limit": limit, "offset": offset}


async def facets() -> dict:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        types = await conn.fetch(
            f"SELECT doc_type, count(*) AS n FROM {_qtable(TABLE)} "
            f"WHERE doc_type IS NOT NULL GROUP BY doc_type ORDER BY n DESC")
        years = await conn.fetchrow(
            f'SELECT min(extract(year FROM "date"))::int AS y0, '
            f'max(extract(year FROM "date"))::int AS y1, count(*) AS total '
            f"FROM {_qtable(TABLE)}")
    return {"doc_types": [{"doc_type": r["doc_type"], "count": int(r["n"])} for r in types],
            "year_min": years["y0"], "year_max": years["y1"], "total": int(years["total"])}
