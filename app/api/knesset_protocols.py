"""Protocol search over the Knesset ODATA mirror (Neon ``knesset`` schema).

Backs the "חיפוש פרוטוקולים" tab on the /knesset page. Committee protocols are
documents of ``grouptypeid = 23`` ("פרוטוקול ועדה") in
``knesset.kns_documentcommitteesession``, joined to their session and committee:

    doc.committeesessionid → session.id ,  session.committeeid → committee.id

The user's required filters are **committee name** and **Knesset number**, plus
a free-text query over document/committee/session text. Every query is
parameterized asyncpg (no string interpolation of user input) and read-only.

Two data-shaping rules (a committee protocol is stored in several file formats):
  * **Documents only** — exclude ``applicationdesc`` ``PIC`` (scanned ``.tif``
    image) and ``VDO`` (session recording) and image/video extensions, so the
    results are readable documents (DOC/DOCX/PDF/RTF).
  * **De-duplicated** — the SAME protocol appears as DOC + PDF + PIC rows; we
    collapse to ONE row per ``(session, document name)``, preferring DOC → PDF.

This lives in its own module (not knesset_db.py) so the protocol search is
decoupled from the SQL-console feature that owns that file.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel

from app.rate_limit import limiter
from app.services import append_store

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/knesset-protocols", tags=["knesset-protocols"])

_SCHEMA = "knesset"
_PROTOCOL_GROUP_TYPE_ID = 23

# Documents only: drop scanned images (PIC/.tif), session recordings (VDO) and
# any image/video file, keeping DOC/DOCX/PDF/RTF/HTML.
_DOC_ONLY = (
    "(d.applicationdesc IS NULL OR d.applicationdesc NOT IN ('PIC','VDO','PPT')) "
    "AND lower(d.filepath) !~ "
    "'\\.(tif|tiff|jpg|jpeg|png|gif|bmp|wmv|avi|mp3|mp4|mov|ppt|pptx)$'"
)
# One protocol identity (a document may exist in several formats).
_DEDUP = "(d.committeesessionid, coalesce(d.documentname, ''))"
# Format preference when collapsing formats to one row.
_FMT_PREF = (
    "CASE upper(coalesce(d.applicationdesc,'')) "
    "WHEN 'DOC' THEN 1 WHEN 'DOCX' THEN 2 WHEN 'PDF' THEN 3 "
    "WHEN 'RTF' THEN 4 ELSE 5 END"
)


class ProtocolRow(BaseModel):
    document_id: int
    document_name: str | None = None
    application: str | None = None
    file_url: str | None = None
    last_updated: str | None = None
    session_id: int | None = None
    session_number: int | None = None
    session_date: str | None = None
    session_location: str | None = None
    session_note: str | None = None
    knesset_num: int | None = None
    committee_id: int | None = None
    committee_name: str | None = None
    committee_type: str | None = None


class SearchResponse(BaseModel):
    total: int
    limit: int
    offset: int
    rows: list[ProtocolRow]


def _build_conds(
    knesset: int | None, committee_id: int | None, committee: str | None, q: str | None,
) -> tuple[list[str], list]:
    """Shared WHERE builder — returns (conditions, params). ``$1`` is always the
    protocol group-type id."""
    conds = [
        "d.grouptypeid = $1",
        "d.filepath IS NOT NULL",
        "d.filepath <> ''",
        _DOC_ONLY,
    ]
    params: list = [_PROTOCOL_GROUP_TYPE_ID]
    if knesset is not None:
        params.append(knesset)
        conds.append(f"s.knessetnum = ${len(params)}")
    if committee_id is not None:
        params.append(committee_id)
        conds.append(f"c.id = ${len(params)}")
    if committee and committee.strip():
        params.append(f"%{committee.strip()}%")
        conds.append(f"c.name ILIKE ${len(params)}")
    if q and q.strip():
        params.append(f"%{q.strip()}%")
        i = len(params)
        conds.append(
            f"(d.documentname ILIKE ${i} OR c.name ILIKE ${i} "
            f"OR s.location ILIKE ${i} OR s.note ILIKE ${i})"
        )
    return conds, params


_JOIN = f"""
    FROM {_SCHEMA}.kns_documentcommitteesession d
    JOIN {_SCHEMA}.kns_committeesession s ON s.id = d.committeesessionid
    JOIN {_SCHEMA}.kns_committee c ON c.id = s.committeeid
"""


def _require_db() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Knesset DB mirror is not configured")


@router.get("/knessets")
@limiter.limit("60/minute")
async def knessets(request: Request):
    """Knesset numbers that have committee protocols, with a (de-duplicated,
    documents-only) protocol count — populates the מספר כנסת dropdown."""
    _require_db()
    pool = await append_store.get_pool()
    conds, params = _build_conds(None, None, None, None)
    conds.append("s.knessetnum IS NOT NULL")
    sql = f"""
        SELECT s.knessetnum AS knesset,
               COUNT(DISTINCT {_DEDUP})::bigint AS doc_count
        {_JOIN}
        WHERE {' AND '.join(conds)}
        GROUP BY s.knessetnum
        ORDER BY s.knessetnum DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return {"knessets": [{"knesset": r["knesset"], "doc_count": r["doc_count"]} for r in rows]}


@router.get("/committees")
@limiter.limit("60/minute")
async def committees(
    request: Request,
    knesset: int | None = Query(None, description="limit to one Knesset"),
    q: str | None = Query(None, description="filter committee names (ILIKE)"),
    limit: int = Query(1000, ge=1, le=3000),
):
    """Committees that have protocols — populates the שם ועדה autocomplete."""
    _require_db()
    pool = await append_store.get_pool()
    conds, params = _build_conds(knesset, None, None, None)
    if q and q.strip():
        params.append(f"%{q.strip()}%")
        conds.append(f"c.name ILIKE ${len(params)}")
    params.append(limit)
    # Group by NAME (not committee id): the same committee is re-created every
    # Knesset with a new id, so grouping by id would list "ועדת החוקה" ~20
    # times. Distinct names — with counts summed across the scope — give a clean
    # autocomplete; the search filters by name (ILIKE) anyway.
    sql = f"""
        SELECT c.name AS committee_name,
               max(c.committeetypedesc) AS committee_type,
               COUNT(DISTINCT {_DEDUP})::bigint AS doc_count
        {_JOIN}
        WHERE {' AND '.join(conds)}
        GROUP BY c.name
        ORDER BY doc_count DESC, c.name ASC
        LIMIT ${len(params)}
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return {"committees": [{
        "name": r["committee_name"], "committee_type": r["committee_type"],
        "doc_count": r["doc_count"],
    } for r in rows]}


@router.get("/search", response_model=SearchResponse)
@limiter.limit("60/minute")
async def search(
    request: Request,
    q: str | None = Query(None, description="free text over document/committee/session"),
    knesset: int | None = Query(None, description="filter by Knesset number"),
    committee_id: int | None = Query(None, description="exact committee id"),
    committee: str | None = Query(None, description="committee name (ILIKE)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Search committee protocols (documents only, one row per protocol) with
    the required committee-name + Knesset filters plus free text. Each row links
    to the file on fs.knesset.gov.il and carries session metadata for the
    expandable detail view."""
    _require_db()
    pool = await append_store.get_pool()
    conds, params = _build_conds(knesset, committee_id, committee, q)
    where = " AND ".join(conds)

    # De-dup formats to one row per protocol (prefer DOC → DOCX → PDF).
    docs_cte = f"""
        SELECT DISTINCT ON {_DEDUP}
            d.id AS document_id, d.documentname, d.applicationdesc, d.filepath,
            d.lastupdateddate,
            s.id AS session_id, s.number AS session_number, s.startdate AS session_date,
            s.location AS session_location, s.note AS session_note, s.knessetnum,
            c.id AS committee_id, c.name AS committee_name, c.committeetypedesc
        {_JOIN}
        WHERE {where}
        ORDER BY {_DEDUP[1:-1]}, {_FMT_PREF}
    """
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*)::bigint FROM ({docs_cte}) t", *params)
        rows = await conn.fetch(
            f"""
            SELECT * FROM ({docs_cte}) t
            ORDER BY t.session_date DESC NULLS LAST, t.document_id DESC
            LIMIT ${len(params) + 1} OFFSET ${len(params) + 2}
            """,
            *params, limit, offset,
        )

    def _iso(v):
        return v.isoformat() if v is not None else None

    return SearchResponse(
        total=int(total or 0), limit=limit, offset=offset,
        rows=[ProtocolRow(
            document_id=r["document_id"], document_name=r["documentname"],
            application=r["applicationdesc"], file_url=r["filepath"],
            last_updated=_iso(r["lastupdateddate"]),
            session_id=r["session_id"], session_number=r["session_number"],
            session_date=_iso(r["session_date"]),
            session_location=r["session_location"], session_note=r["session_note"],
            knesset_num=r["knessetnum"], committee_id=r["committee_id"],
            committee_name=r["committee_name"], committee_type=r["committeetypedesc"],
        ) for r in rows],
    )
