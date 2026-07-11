"""Public API of the Knesset ODATA mirror — the /knesset page's backend.

All read endpoints are public (the data is parliamentary public record); the
SQL path has the same defense-in-depth as /api/append/{id}/sql (READ ONLY tx,
single SELECT, statement_timeout, row cap) and /api/knesset-db is metered by
the per-IP data budget middleware. Sync management is admin-only.

Endpoints:
  GET  /api/knesset-db/status      → compact sync stats (page header)
  GET  /api/knesset-db/tables      → all tables + schema + Hebrew descriptions
  POST /api/knesset-db/sql         → {sql} run read-only, ≤1000 rows
  GET  /api/knesset-db/export.csv  → ?sql=… streamed CSV (≤200k rows)
  POST /api/knesset-db/sync        → admin: kick a sync pass now (optional table/reset)
"""
import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from app.auth.dependencies import get_admin_user
from app.config import settings
from app.rate_limit import limiter
from app.services import knesset_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knesset-db", tags=["knesset-db"])


class SqlBody(BaseModel):
    sql: str


class SyncBody(BaseModel):
    table: str | None = None   # limit the pass to one table
    reset: bool = False        # force a fresh full walk of that table


def _require_enabled() -> None:
    if not knesset_db.is_configured():
        raise HTTPException(status_code=409, detail="Knesset DB mirror is not configured")


@router.get("/status")
async def status():
    return await knesset_db.status_summary()


@router.get("/tables")
@limiter.limit("30/minute")
async def tables(request: Request):
    _require_enabled()
    try:
        return {"tables": await knesset_db.list_tables()}
    except Exception as e:  # noqa: BLE001 — surface init errors readably
        logger.exception("knesset-db /tables failed")
        raise HTTPException(status_code=503, detail=f"{type(e).__name__}: {e}")


@router.post("/sql")
@limiter.limit("20/minute")
async def sql(request: Request, body: SqlBody):
    _require_enabled()
    try:
        return await knesset_db.run_sql(body.sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors go to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


@router.get("/export.csv")
@limiter.limit("6/minute")
async def export_csv(request: Request, sql: str):
    _require_enabled()
    try:
        stream = knesset_db.iter_sql_csv(sql)
        # Pull the first chunk eagerly so validation/SQL errors become a clean
        # 400 instead of a broken download.
        first = await anext(stream)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except StopAsyncIteration:
        first = "﻿\r\n"
        async def _empty():
            return
            yield  # pragma: no cover
        stream = _empty()
    except Exception as e:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")

    async def _chain():
        yield first
        async for chunk in stream:
            yield chunk

    return StreamingResponse(
        _chain(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="knesset_query.csv"'},
    )


# ── Committee-protocol batches (the /knesset "אצוות" tab) ────────────────────
# Filter by committee / Knesset number and pull ALL matching protocol files as
# one streamed ZIP (fetched live from fs.knesset.gov.il — OVER stores only the
# links). A links-CSV manifest is offered as the cheap alternative.

BATCH_MAX_FILES = 2000          # per-ZIP cap; the UI says to narrow the filter
BATCH_MAX_FILE_BYTES = 100 * 1024 * 1024  # skip pathological single files
CSV_MAX_ROWS = 50_000


def _batch_filters(knesset_num: int | None, committee_id: int | None, q: str | None):
    if knesset_num is None and committee_id is None and not (q or "").strip():
        raise HTTPException(status_code=400,
                            detail="נדרש סינון: מספר כנסת, ועדה או טקסט בשם הוועדה")
    return knesset_num, committee_id, (q or "").strip() or None


@router.get("/protocols/facets")
@limiter.limit("30/minute")
async def protocol_facets(request: Request):
    _require_enabled()
    return await knesset_db.protocol_facets()


@router.get("/protocols/count")
@limiter.limit("60/minute")
async def protocol_count(request: Request, knesset_num: int | None = None,
                         committee_id: int | None = None, q: str | None = None):
    _require_enabled()
    kn, cid, qq = _batch_filters(knesset_num, committee_id, q)
    n = await knesset_db.protocol_count(kn, cid, qq)
    return {"files": n, "zip_max_files": BATCH_MAX_FILES}


def _safe_name(s: str, fallback: str) -> str:
    s = "".join(ch if ch not in '\\/:*?"<>|' else "_" for ch in (s or "").strip())
    return (s or fallback)[:80]


class _ZipBuf:
    """Unseekable in-memory sink for zipfile: collects written bytes so the
    streaming generator can drain them after each member. No seek() on purpose —
    zipfile then emits data descriptors, valid for streaming consumers."""
    def __init__(self):
        self._chunks: list[bytes] = []
        self._pos = 0

    def write(self, b) -> int:
        b = bytes(b)
        self._chunks.append(b)
        self._pos += len(b)
        return len(b)

    def tell(self) -> int:
        return self._pos

    def flush(self) -> None:
        pass

    def drain(self) -> bytes:
        out = b"".join(self._chunks)
        self._chunks = []
        return out


async def _zip_stream(rows: list[dict]):
    """Fetch each protocol from the Knesset file server and stream a ZIP.
    Sequential (one file in memory at a time); failures are collected into
    _errors.txt instead of aborting a half-sent download."""
    import io
    import zipfile

    import httpx

    buf = _ZipBuf()
    zf = zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED)
    manifest = io.StringIO()
    manifest.write("date,knesset,committee,session_id,document_id,filename,url\r\n")
    errors: list[str] = []
    seen_names: set[str] = set()

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0, connect=20.0), follow_redirects=True,
        headers={"User-Agent": "over.org.il protocol batches (+https://over.org.il/knesset)"},
    ) as client:
        for r in rows:
            url = (r.get("filepath") or "").strip()
            if not url:
                continue
            base = url.rsplit("/", 1)[-1] or f"{r['document_id']}.bin"
            date = str(r.get("startdate") or "")[:10]
            folder = _safe_name(r.get("committee_name") or "", "committee")
            name = f"{folder}/{date}_{_safe_name(base, str(r['document_id']))}"
            if name in seen_names:
                name = f"{folder}/{date}_{r['document_id']}_{_safe_name(base, 'doc')}"
            seen_names.add(name)
            try:
                resp = await client.get(url)
                resp.raise_for_status()
                data = resp.content
                if len(data) > BATCH_MAX_FILE_BYTES:
                    raise ValueError(f"file too large ({len(data)} bytes)")
                zf.writestr(name, data)
                manifest.write(
                    f"{date},{r.get('knessetnum') or ''},"
                    f"\"{(r.get('committee_name') or '').replace(chr(34), chr(34)*2)}\","
                    f"{r['session_id']},{r['document_id']},\"{name}\",{url}\r\n")
            except Exception as e:  # noqa: BLE001 — keep the batch going
                errors.append(f"{url}\t{type(e).__name__}: {e}")
            chunk = buf.drain()
            if chunk:
                yield chunk

    zf.writestr("_index.csv", "﻿" + manifest.getvalue())
    if errors:
        zf.writestr("_errors.txt", "\n".join(errors))
    zf.close()
    tail = buf.drain()
    if tail:
        yield tail


@router.get("/protocols/batch.zip")
@limiter.limit("2/minute")
async def protocol_batch_zip(request: Request, knesset_num: int | None = None,
                             committee_id: int | None = None, q: str | None = None):
    """Stream a ZIP of all protocol files matching the filter (newest first,
    capped at BATCH_MAX_FILES). The files come live from fs.knesset.gov.il —
    a large batch takes minutes; the browser shows a progressing download."""
    _require_enabled()
    kn, cid, qq = _batch_filters(knesset_num, committee_id, q)
    n = await knesset_db.protocol_count(kn, cid, qq)
    if n == 0:
        raise HTTPException(status_code=404, detail="אין פרוטוקולים בסינון הזה")
    if n > BATCH_MAX_FILES:
        raise HTTPException(status_code=400, detail=(
            f"האצווה גדולה מדי ({n:,} קבצים; המקסימום {BATCH_MAX_FILES:,}). "
            f"צמצמו לפי ועדה או כנסת, או הורידו את רשימת הקישורים (CSV)."))
    rows = await knesset_db.protocol_batch_rows(kn, cid, qq, limit=BATCH_MAX_FILES)
    parts = ["protocols"]
    if kn is not None:
        parts.append(f"knesset{kn}")
    if cid is not None:
        parts.append(f"committee{cid}")
    fname = "_".join(parts) + ".zip"
    return StreamingResponse(
        _zip_stream(rows),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


@router.get("/protocols/links.csv")
@limiter.limit("10/minute")
async def protocol_links_csv(request: Request, knesset_num: int | None = None,
                             committee_id: int | None = None, q: str | None = None):
    """The batch as a link manifest only (date, committee, session, file URL) —
    for very large selections or download managers."""
    _require_enabled()
    kn, cid, qq = _batch_filters(knesset_num, committee_id, q)
    rows = await knesset_db.protocol_batch_rows(kn, cid, qq, limit=CSV_MAX_ROWS)

    def _gen():
        yield "﻿date,knesset,committee,session_id,document_id,format,url\r\n"
        for r in rows:
            comm = (r.get("committee_name") or "").replace('"', '""')
            yield (f"{str(r.get('startdate') or '')[:10]},{r.get('knessetnum') or ''},"
                   f"\"{comm}\",{r['session_id']},{r['document_id']},"
                   f"{r.get('applicationdesc') or ''},{r.get('filepath') or ''}\r\n")

    return StreamingResponse(
        _gen(), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="knesset_protocol_links.csv"'},
    )


@router.post("/sync")
async def sync(body: SyncBody, admin=Depends(get_admin_user)):
    """Kick a sync pass in the background right now (admin). With reset=true the
    named table is re-walked from scratch (rows re-upsert; nothing is lost)."""
    _require_enabled()
    if body.reset:
        if not body.table:
            raise HTTPException(status_code=400, detail="reset requires a table name")
        await knesset_db.reset_table(body.table)

    async def _run():
        try:
            res = await knesset_db.sync_tick(
                budget_seconds=settings.knesset_db_tick_budget_seconds,
                sync_interval_hours=settings.knesset_db_sync_interval_hours,
                only_table=body.table,
            )
            logger.info("knesset-db manual sync: %s", res)
        except Exception:  # noqa: BLE001
            logger.exception("knesset-db manual sync failed")

    asyncio.create_task(_run())
    return {"started": True, "table": body.table, "reset": body.reset}
