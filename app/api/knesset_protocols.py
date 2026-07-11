"""Protocol search over the Knesset ODATA mirror (Neon ``knesset`` schema).

Backs the "חיפוש פרוטוקולים" tab on the /knesset page. Committee protocols are
documents of ``grouptypeid = 23`` ("פרוטוקול ועדה") in
``knesset.kns_documentcommitteesession``, joined to their session and committee:

    doc.committeesessionid → session.id ,  session.committeeid → committee.id

The user's required filters are **committee name** and **Knesset number**, plus
a free-text query over document/committee/session text. Every query is
parameterized asyncpg (no string interpolation of user input) and read-only.

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


def _require_db() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Knesset DB mirror is not configured")


class ProtocolRow(BaseModel):
    document_id: int
    document_name: str | None = None
    application: str | None = None
    file_url: str | None = None
    last_updated: str | None = None
    session_id: int | None = None
    session_number: int | None = None
    session_date: str | None = None
    knesset_num: int | None = None
    committee_id: int | None = None
    committee_name: str | None = None
    committee_type: str | None = None


class SearchResponse(BaseModel):
    total: int
    limit: int
    offset: int
    rows: list[ProtocolRow]


@router.get("/knessets")
@limiter.limit("60/minute")
async def knessets(request: Request):
    """Knesset numbers that have committee protocols, with a protocol count —
    populates the מספר כנסת dropdown."""
    _require_db()
    pool = await append_store.get_pool()
    sql = f"""
        SELECT s.knessetnum AS knesset, COUNT(*)::bigint AS doc_count
        FROM {_SCHEMA}.kns_documentcommitteesession d
        JOIN {_SCHEMA}.kns_committeesession s ON s.id = d.committeesessionid
        WHERE d.grouptypeid = $1 AND d.filepath IS NOT NULL AND d.filepath <> ''
              AND s.knessetnum IS NOT NULL
        GROUP BY s.knessetnum
        ORDER BY s.knessetnum DESC
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, _PROTOCOL_GROUP_TYPE_ID)
    return {"knessets": [{"knesset": r["knesset"], "doc_count": r["doc_count"]} for r in rows]}


@router.get("/committees")
@limiter.limit("60/minute")
async def committees(
    request: Request,
    knesset: int | None = Query(None, description="limit to one Knesset"),
    q: str | None = Query(None, description="filter committee names (ILIKE)"),
    limit: int = Query(500, ge=1, le=2000),
):
    """Committees that have protocols — populates the שם ועדה dropdown /
    autocomplete. Scope to a Knesset (recommended) so the list is short."""
    _require_db()
    pool = await append_store.get_pool()
    conds = ["d.grouptypeid = $1", "d.filepath IS NOT NULL", "d.filepath <> ''"]
    params: list = [_PROTOCOL_GROUP_TYPE_ID]
    if knesset is not None:
        params.append(knesset)
        conds.append(f"s.knessetnum = ${len(params)}")
    if q and q.strip():
        params.append(f"%{q.strip()}%")
        conds.append(f"c.name ILIKE ${len(params)}")
    params.append(limit)
    sql = f"""
        SELECT c.id AS committee_id, c.name AS committee_name,
               c.committeetypedesc AS committee_type, s.knessetnum AS knesset,
               COUNT(*)::bigint AS doc_count
        FROM {_SCHEMA}.kns_documentcommitteesession d
        JOIN {_SCHEMA}.kns_committeesession s ON s.id = d.committeesessionid
        JOIN {_SCHEMA}.kns_committee c ON c.id = s.committeeid
        WHERE {' AND '.join(conds)}
        GROUP BY c.id, c.name, c.committeetypedesc, s.knessetnum
        ORDER BY doc_count DESC, c.name ASC
        LIMIT ${len(params)}
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch(sql, *params)
    return {"committees": [{
        "committee_id": r["committee_id"], "name": r["committee_name"],
        "committee_type": r["committee_type"], "knesset": r["knesset"],
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
    """Search committee protocols with the required committee-name + Knesset
    filters (plus free text). Returns one row per protocol document with a
    direct link to the file on fs.knesset.gov.il."""
    _require_db()
    pool = await append_store.get_pool()

    conds = ["d.grouptypeid = $1", "d.filepath IS NOT NULL", "d.filepath <> ''"]
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
    where = " AND ".join(conds)

    base_from = f"""
        FROM {_SCHEMA}.kns_documentcommitteesession d
        JOIN {_SCHEMA}.kns_committeesession s ON s.id = d.committeesessionid
        JOIN {_SCHEMA}.kns_committee c ON c.id = s.committeeid
        WHERE {where}
    """
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT COUNT(*)::bigint {base_from}", *params)
        rows = await conn.fetch(
            f"""
            SELECT d.id AS document_id, d.documentname, d.applicationdesc,
                   d.filepath, d.lastupdateddate,
                   s.id AS session_id, s.number AS session_number,
                   s.startdate AS session_date, s.knessetnum,
                   c.id AS committee_id, c.name AS committee_name,
                   c.committeetypedesc
            {base_from}
            ORDER BY s.startdate DESC NULLS LAST, d.id DESC
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
            session_date=_iso(r["session_date"]), knesset_num=r["knessetnum"],
            committee_id=r["committee_id"], committee_name=r["committee_name"],
            committee_type=r["committeetypedesc"],
        ) for r in rows],
    )
