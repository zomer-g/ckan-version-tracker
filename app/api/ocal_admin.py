"""Admin API for the migrated יומן לעם (Ocal) — sources, people, organizations,
site content and exception review, over the dedicated ocal DB.

All endpoints require an OVER admin (get_admin_user). This is the management
surface that lets the corpus be curated inside OVER so the legacy Ocal Node
admin can be retired. The auto-import triggers (candidates/import/scan) live in
app/api/admin.py; everything else is here.
"""
import logging
import uuid

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from app.auth.dependencies import get_admin_user
from app.config import settings
from app.models.user import User
from app.rate_limit import limiter
from app.services import ocal_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/ocal", tags=["ocal-admin"])


def _uuid(s: str) -> uuid.UUID:
    try:
        return uuid.UUID(str(s))
    except (ValueError, TypeError):
        raise HTTPException(status_code=400, detail="invalid id")


def _rows(rows) -> list[dict]:
    return [dict(r) for r in rows]


# ── sources ───────────────────────────────────────────────────────────────────

@router.get("/sources")
@limiter.limit("60/minute")
async def list_sources(
    request: Request,
    q: str | None = Query(None),
    enabled: bool | None = Query(None),
    reviewed: bool | None = Query(None),
    limit: int = Query(200, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_admin_user),
):
    where = ["1=1"]
    args: list = []
    if q:
        args.append(f"%{q}%")
        where.append(
            f"(s.name ILIKE ${len(args)} OR p.name ILIKE ${len(args)} OR o.name ILIKE ${len(args)})")
    if enabled is not None:
        args.append(enabled)
        where.append(f"s.is_enabled = ${len(args)}")
    if reviewed is not None:
        where.append("s.reviewed_at IS NOT NULL" if reviewed else "s.reviewed_at IS NULL")
    args.extend([limit, offset])
    rows = await ocal_db.fetch(
        "SELECT s.id, s.name, s.color, s.is_enabled, s.total_events, "
        "       s.first_event_date, s.last_event_date, s.resource_id, s.dataset_url, "
        "       s.sync_status, s.last_sync_at, s.reviewed_at, s.review_notes, "
        "       p.name AS person_name, o.name AS organization_name, "
        "       s.person_id, s.organization_id "
        "FROM diary_sources s "
        "LEFT JOIN people p ON p.id = s.person_id "
        "LEFT JOIN organizations o ON o.id = s.organization_id "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY s.reviewed_at IS NOT NULL, s.total_events DESC "
        f"LIMIT ${len(args) - 1} OFFSET ${len(args)}",
        *args)
    total = await ocal_db.fetchval("SELECT count(*) FROM diary_sources")
    return {"sources": _rows(rows), "total": int(total or 0)}


class SourcePatch(BaseModel):
    name: str | None = None
    is_enabled: bool | None = None
    person_id: str | None = None
    organization_id: str | None = None
    review_notes: str | None = None
    color: str | None = None


@router.patch("/sources/{source_id}")
@limiter.limit("30/minute")
async def patch_source(request: Request, source_id: str, body: SourcePatch,
                       user: User = Depends(get_admin_user)):
    sid = _uuid(source_id)
    sets, args = [], []
    for col, val in (("name", body.name), ("is_enabled", body.is_enabled),
                     ("review_notes", body.review_notes), ("color", body.color)):
        if val is not None:
            args.append(val)
            sets.append(f"{col} = ${len(args)}")
    for col, val in (("person_id", body.person_id), ("organization_id", body.organization_id)):
        if val is not None:
            args.append(_uuid(val) if val else None)
            sets.append(f"{col} = ${len(args)}")
    if not sets:
        raise HTTPException(400, "no fields to update")
    args.append(sid)
    await ocal_db.execute(
        f"UPDATE diary_sources SET {', '.join(sets)}, updated_at=now() WHERE id=${len(args)}", *args)
    return {"updated": True}


@router.post("/sources/{source_id}/review")
@limiter.limit("30/minute")
async def review_source(request: Request, source_id: str, user: User = Depends(get_admin_user)):
    await ocal_db.execute(
        "UPDATE diary_sources SET reviewed_at=now(), updated_at=now() WHERE id=$1", _uuid(source_id))
    return {"reviewed": True}


@router.post("/sources/{source_id}/unreview")
@limiter.limit("30/minute")
async def unreview_source(request: Request, source_id: str, user: User = Depends(get_admin_user)):
    await ocal_db.execute(
        "UPDATE diary_sources SET reviewed_at=NULL, updated_at=now() WHERE id=$1", _uuid(source_id))
    return {"reviewed": False}


@router.delete("/sources/{source_id}")
@limiter.limit("20/minute")
async def delete_source(request: Request, source_id: str, user: User = Depends(get_admin_user)):
    # diary_events cascade-delete on the FK; event_entities cascade on events.
    n = await ocal_db.execute("DELETE FROM diary_sources WHERE id=$1", _uuid(source_id))
    logger.info("ocal admin: source %s deleted by %s (%s)", source_id, user.email, n)
    return {"deleted": True}


@router.post("/sources/{source_id}/reimport")
@limiter.limit("6/minute")
async def reimport_source(request: Request, source_id: str,
                          clear: bool = Query(False),
                          user: User = Depends(get_admin_user)):
    """Re-fetch the source's odata resource and upsert its events again. ``clear``
    first deletes the source's existing events (a clean re-sync)."""
    row = await ocal_db.fetchrow(
        "SELECT resource_id FROM diary_sources WHERE id=$1", _uuid(source_id))
    if not row or not row["resource_id"]:
        raise HTTPException(404, "source has no resource_id to re-import")
    if clear:
        await ocal_db.execute("DELETE FROM diary_events WHERE source_id=$1", _uuid(source_id))
    from app.services import ocal_import
    try:
        res = await ocal_import.import_resource(row["resource_id"], force=True, enrich=True)
    except Exception as e:  # noqa: BLE001
        raise HTTPException(500, str(e))
    logger.info("ocal admin: source %s re-imported by %s", source_id, user.email)
    return res


@router.post("/sources/{source_id}/enrich")
@limiter.limit("10/minute")
async def enrich_source_ep(request: Request, source_id: str,
                           resync: bool = Query(True),
                           ai: bool = Query(False),
                           user: User = Depends(get_admin_user)):
    """Re-run enrichment for one source. ``ai=true`` also runs Stage 3 AI-NER
    (paid LLM) — the on-demand action that matches the legacy Ocal admin button.
    Ignored (no-op) when no LLM key is configured."""
    from app.services import ocal_enrich
    res = await ocal_enrich.enrich_source(_uuid(source_id), is_resync=resync, run_ai=ai)
    logger.info("ocal admin: source %s enriched by %s (ai=%s)", source_id, user.email, ai)
    return res


@router.get("/ai-ner/status")
@limiter.limit("30/minute")
async def ai_ner_status(request: Request, user: User = Depends(get_admin_user)):
    """Report whether the paid AI-NER stage can run (which LLM provider is set)."""
    from app.services import ocal_enrich
    return {
        "enabled": settings.ocal_ai_ner_enabled,
        "available": ocal_enrich.ai_ner_available(),
        "provider": ocal_enrich.ai_ner_provider(),
        "auto": settings.ocal_ai_ner_auto,
        "batch": settings.ocal_ai_ner_batch,
    }


# ── exceptions (auto-rejected candidates) ────────────────────────────────────

@router.get("/exceptions")
@limiter.limit("30/minute")
async def list_exceptions(request: Request, limit: int = Query(200, ge=1, le=1000),
                          user: User = Depends(get_admin_user)):
    rows = await ocal_db.fetch(
        "SELECT resource_id, dataset_title, resource_format, resource_name, "
        "       exception_reason, moved_at "
        "FROM diary_exceptions ORDER BY moved_at DESC LIMIT $1", limit)
    return {"exceptions": _rows(rows), "count": len(rows)}


@router.delete("/exceptions/{resource_id}")
@limiter.limit("30/minute")
async def clear_exception(request: Request, resource_id: str,
                          user: User = Depends(get_admin_user)):
    """Un-reject a resource so the next scan re-evaluates it."""
    await ocal_db.execute("DELETE FROM diary_exceptions WHERE resource_id=$1", resource_id)
    return {"cleared": True}


# ── people ────────────────────────────────────────────────────────────────────

@router.get("/people")
@limiter.limit("60/minute")
async def list_people(request: Request, q: str | None = Query(None),
                      limit: int = Query(500, ge=1, le=2000),
                      user: User = Depends(get_admin_user)):
    if q:
        rows = await ocal_db.fetch(
            "SELECT p.id, p.name, p.wikipedia_link, p.notes, p.organization_id, "
            "o.name AS organization_name, "
            "(SELECT count(*) FROM diary_sources ds WHERE ds.person_id=p.id) AS source_count "
            "FROM people p LEFT JOIN organizations o ON o.id=p.organization_id "
            "WHERE p.name ILIKE $1 ORDER BY p.name LIMIT $2", f"%{q}%", limit)
    else:
        rows = await ocal_db.fetch(
            "SELECT p.id, p.name, p.wikipedia_link, p.notes, p.organization_id, "
            "o.name AS organization_name, "
            "(SELECT count(*) FROM diary_sources ds WHERE ds.person_id=p.id) AS source_count "
            "FROM people p LEFT JOIN organizations o ON o.id=p.organization_id "
            "ORDER BY p.name LIMIT $1", limit)
    return {"people": _rows(rows), "count": len(rows)}


class PersonBody(BaseModel):
    name: str
    organization_id: str | None = None
    wikipedia_link: str | None = None
    notes: str | None = None


@router.post("/people")
@limiter.limit("30/minute")
async def create_person(request: Request, body: PersonBody, user: User = Depends(get_admin_user)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "name is required")
    row = await ocal_db.fetchrow(
        "INSERT INTO people (name, organization_id, wikipedia_link, notes) "
        "VALUES ($1,$2,$3,$4) RETURNING id",
        name, _uuid(body.organization_id) if body.organization_id else None,
        body.wikipedia_link, body.notes)
    return {"id": str(row["id"])}


@router.patch("/people/{person_id}")
@limiter.limit("30/minute")
async def patch_person(request: Request, person_id: str, body: PersonBody,
                       user: User = Depends(get_admin_user)):
    await ocal_db.execute(
        "UPDATE people SET name=$2, organization_id=$3, wikipedia_link=$4, notes=$5, updated_at=now() "
        "WHERE id=$1", _uuid(person_id), body.name.strip(),
        _uuid(body.organization_id) if body.organization_id else None,
        body.wikipedia_link, body.notes)
    return {"updated": True}


@router.delete("/people/{person_id}")
@limiter.limit("20/minute")
async def delete_person(request: Request, person_id: str, user: User = Depends(get_admin_user)):
    await ocal_db.execute("DELETE FROM people WHERE id=$1", _uuid(person_id))
    return {"deleted": True}


class MergeBody(BaseModel):
    source_ids: list[str]
    target_id: str


@router.post("/people/merge")
@limiter.limit("10/minute")
async def merge_people(request: Request, body: MergeBody, user: User = Depends(get_admin_user)):
    """Repoint all references from source people to the target, then delete them."""
    target = _uuid(body.target_id)
    srcs = [_uuid(s) for s in body.source_ids if s and s != body.target_id]
    if not srcs:
        raise HTTPException(400, "no source ids to merge")
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("UPDATE diary_sources SET person_id=$1 WHERE person_id=ANY($2::uuid[])", target, srcs)
            await conn.execute("UPDATE event_entities SET entity_id=$1 WHERE entity_id=ANY($2::uuid[])", target, srcs)
            await conn.execute("UPDATE entity_cross_refs SET target_person_id=$1 WHERE target_person_id=ANY($2::uuid[])", target, srcs)
            await conn.execute("DELETE FROM people WHERE id=ANY($1::uuid[])", srcs)
    logger.info("ocal admin: merged %d people into %s by %s", len(srcs), target, user.email)
    return {"merged": len(srcs), "target_id": str(target)}


class BulkPersonRow(BaseModel):
    name: str
    wikipedia_link: str | None = None
    notes: str | None = None
    organization_name: str | None = None


class BulkPeopleBody(BaseModel):
    rows: list[BulkPersonRow]


@router.post("/people/bulk-import")
@limiter.limit("6/minute")
async def bulk_import_people(request: Request, body: BulkPeopleBody,
                            user: User = Depends(get_admin_user)):
    """Upsert people by name (case-insensitive); auto-create organizations by name.
    Ported from Ocal admin people/bulk-import."""
    if not body.rows:
        raise HTTPException(400, "rows is required")
    created = updated = skipped = 0
    pool = await ocal_db.get_pool()
    async with pool.acquire() as conn:
        orgs = await conn.fetch("SELECT id, name FROM organizations")
        org_by_name = {(o["name"] or "").lower().strip(): o["id"] for o in orgs}
        for r in body.rows:
            name = (r.name or "").strip()
            if not name:
                skipped += 1
                continue
            org_id = None
            if r.organization_name and r.organization_name.strip():
                key = r.organization_name.lower().strip()
                org_id = org_by_name.get(key)
                if org_id is None:
                    o = await conn.fetchrow(
                        "INSERT INTO organizations (name) VALUES ($1) "
                        "ON CONFLICT (name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                        r.organization_name.strip())
                    org_id = o["id"]
                    org_by_name[key] = org_id
            existing = await conn.fetchrow(
                "SELECT id FROM people WHERE lower(name)=lower($1) LIMIT 1", name)
            if existing:
                await conn.execute(
                    "UPDATE people SET wikipedia_link=COALESCE($2,wikipedia_link), "
                    "notes=COALESCE($3,notes), organization_id=COALESCE($4,organization_id), "
                    "updated_at=now() WHERE id=$1",
                    existing["id"], r.wikipedia_link, r.notes, org_id)
                updated += 1
            else:
                await conn.execute(
                    "INSERT INTO people (name, wikipedia_link, notes, organization_id) "
                    "VALUES ($1,$2,$3,$4)", name, r.wikipedia_link, r.notes, org_id)
                created += 1
    logger.info("ocal admin: bulk-import people by %s — created=%d updated=%d skipped=%d",
                user.email, created, updated, skipped)
    return {"created": created, "updated": updated, "skipped": skipped}


# ── organizations ─────────────────────────────────────────────────────────────

@router.get("/organizations")
@limiter.limit("60/minute")
async def list_orgs(request: Request, q: str | None = Query(None),
                    user: User = Depends(get_admin_user)):
    if q:
        rows = await ocal_db.fetch(
            "SELECT id, name, website, description FROM organizations WHERE name ILIKE $1 ORDER BY name LIMIT 1000",
            f"%{q}%")
    else:
        rows = await ocal_db.fetch(
            "SELECT id, name, website, description FROM organizations ORDER BY name LIMIT 1000")
    return {"organizations": _rows(rows), "count": len(rows)}


class OrgBody(BaseModel):
    name: str
    website: str | None = None
    description: str | None = None


@router.post("/organizations")
@limiter.limit("30/minute")
async def create_org(request: Request, body: OrgBody, user: User = Depends(get_admin_user)):
    name = (body.name or "").strip()
    if not name:
        raise HTTPException(400, "name is required")
    row = await ocal_db.fetchrow(
        "INSERT INTO organizations (name, website, description) VALUES ($1,$2,$3) "
        "ON CONFLICT (name) DO UPDATE SET website=EXCLUDED.website, description=EXCLUDED.description "
        "RETURNING id", name, body.website, body.description)
    return {"id": str(row["id"])}


@router.patch("/organizations/{org_id}")
@limiter.limit("30/minute")
async def patch_org(request: Request, org_id: str, body: OrgBody, user: User = Depends(get_admin_user)):
    await ocal_db.execute(
        "UPDATE organizations SET name=$2, website=$3, description=$4, updated_at=now() WHERE id=$1",
        _uuid(org_id), body.name.strip(), body.website, body.description)
    return {"updated": True}


@router.delete("/organizations/{org_id}")
@limiter.limit("20/minute")
async def delete_org(request: Request, org_id: str, user: User = Depends(get_admin_user)):
    await ocal_db.execute("DELETE FROM organizations WHERE id=$1", _uuid(org_id))
    return {"deleted": True}


# ── site content ──────────────────────────────────────────────────────────────

@router.get("/content")
@limiter.limit("60/minute")
async def get_content(request: Request, user: User = Depends(get_admin_user)):
    rows = await ocal_db.fetch("SELECT key, value, updated_at FROM site_content ORDER BY key")
    return {"content": _rows(rows)}


class ContentBody(BaseModel):
    value: str


@router.put("/content/{key}")
@limiter.limit("30/minute")
async def put_content(request: Request, key: str, body: ContentBody,
                      user: User = Depends(get_admin_user)):
    await ocal_db.execute(
        "INSERT INTO site_content (key, value, updated_at) VALUES ($1,$2,now()) "
        "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=now()",
        key, body.value)
    logger.info("ocal admin: content %s updated by %s", key, user.email)
    return {"updated": True}
