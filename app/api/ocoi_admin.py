"""Admin API for the migrated ניגוד עניינים לעם (OCOI).

All endpoints require an OVER admin (get_admin_user), replacing OCOI's own
cookie session + 8-key permission map. OCOI's roles map onto OVER's model:
`admin`/`content_manager` become OVER admins, `mcp_user` becomes an OVER
api_users invite (see app/mcp).

WAVE 1 — documents.

Three things this deliberately does NOT reproduce from the original, because
they are defects rather than behaviour (each is verified in OCOI's source):

* **reextract committed its deletes before checking the lock.** It deleted a
  document's extraction_runs and entity_relationships, committed, and only then
  raised 409 if extraction was already running — destroying the edges with no
  re-run scheduled. Here the guard comes first.
* **verify/reextract could drift.** `documents.verified` stayed true while
  reextract re-created every edge as unverified. Re-queuing extraction now
  clears the document's own verified flag too, so the two cannot disagree.
* **the purge endpoints loaded whole tables into Python** to decide what to
  delete. They are set-based here.

Conversion (`reconvert`) is absent on purpose: it needs poppler + tesseract,
which OVER cannot install (see docs/ocoi-port-plan.md §1). Re-running it is a
worker job, so the admin marks the document and the worker picks it up.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from app.auth.dependencies import get_admin_user
from app.models.user import User
from app.rate_limit import limiter
from app.services import ocoi_db

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/ocoi", tags=["ocoi-admin"])


# ── helpers ───────────────────────────────────────────────────────────────────

def _require_configured() -> None:
    if not ocoi_db.is_configured():
        raise HTTPException(status_code=503, detail="OCOI_DATABASE_URL not configured")


def _id(s: str, name: str = "id") -> str:
    """OCOI ids are uuid4 stored as CHAR(36) — validate shape, bind as str."""
    try:
        uuid.UUID(str(s))
    except (ValueError, TypeError, AttributeError):
        raise HTTPException(status_code=400, detail=f"{name} must be a UUID")
    return str(s)


def _rows(rows) -> list[dict]:
    return [dict(r) for r in rows]


def _now() -> datetime:
    """Naive Asia/Jerusalem, matching the corpus.

    OCOI was inconsistent — document writes used Israel time while match and
    suggestion writes used naive UTC, into the same column type. One clock here.
    """
    return datetime.now(timezone.utc).astimezone().replace(tzinfo=None)


def _ok(data=None, **extra) -> dict:
    out = {"status": "ok"}
    if data is not None:
        out["data"] = data
    out.update(extra)
    return out


# ── documents: list ───────────────────────────────────────────────────────────

@router.get("/documents")
@limiter.limit("60/minute")
async def list_documents(
    request: Request,
    q: str | None = Query(None, description="title search"),
    extraction: str | None = Query(None),
    conversion: str | None = Query(None),
    source_type: str | None = Query(None),
    verified: bool | None = Query(None),
    has_file: bool | None = Query(None, description="only rows whose bytes we hold"),
    date_from: str | None = Query(None, description="YYYY-MM-DD"),
    date_to: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_admin_user),
):
    """Document list. Never selects markdown_content — it is megabytes per row
    and the list does not show it (OCOI had the same care, worth keeping)."""
    _require_configured()
    where, args = ["1=1"], []
    if q:
        args.append(f"%{q}%")
        where.append(f"d.title ILIKE ${len(args)}")
    if extraction:
        args.append(extraction)
        where.append(f"d.extraction_status = ${len(args)}")
    if conversion:
        args.append(conversion)
        where.append(f"d.conversion_status = ${len(args)}")
    if source_type:
        args.append(source_type)
        where.append(f"s.source_type = ${len(args)}")
    if verified is not None:
        args.append(verified)
        where.append(f"d.verified = ${len(args)}")
    if has_file is not None:
        where.append("d.pdf_r2_key IS NOT NULL" if has_file else "d.pdf_r2_key IS NULL")
    # Bound as a real date, not a string compared against a timestamp column —
    # OCOI passed the raw string and relied on driver coercion.
    for val, op in ((date_from, ">="), (date_to, "<=")):
        if val:
            try:
                args.append(datetime.fromisoformat(val))
            except ValueError:
                raise HTTPException(status_code=400, detail="dates must be YYYY-MM-DD")
            where.append(f"d.created_at {op} ${len(args)}")
    w = " AND ".join(where)

    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM documents d LEFT JOIN sources s ON s.id = d.source_id "
        f"WHERE {w}", *args) or 0
    rows = await ocoi_db.fetch(f"""
        SELECT d.id, d.title, d.file_url, d.file_format, d.file_size,
               d.conversion_status, d.extraction_status, d.verified, d.verified_at,
               d.verified_by_email, d.created_at, d.converted_at, d.extracted_at,
               (d.pdf_r2_key IS NOT NULL) AS has_file,
               (d.markdown_content IS NOT NULL
                AND length(d.markdown_content) > 0) AS has_text,
               length(d.markdown_content) AS text_length,
               s.title AS source_title, s.source_type,
               (SELECT count(*) FROM entity_relationships r
                 WHERE r.document_id = d.id) AS relationships_count
        FROM documents d
        LEFT JOIN sources s ON s.id = d.source_id
        WHERE {w}
        ORDER BY d.created_at DESC NULLS LAST
        LIMIT ${len(args)+1} OFFSET ${len(args)+2}
    """, *args, limit, offset)
    return _ok(_rows(rows), total=int(total), limit=limit, offset=offset)


# ── documents: detail ─────────────────────────────────────────────────────────

@router.get("/documents/{doc_id}")
@limiter.limit("60/minute")
async def get_document(request: Request, doc_id: str,
                       include_markdown: bool = Query(False),
                       user: User = Depends(get_admin_user)):
    """One document with its provenance, extraction runs and edges.

    Resolves both endpoint names in ONE query per entity type rather than two
    per relationship: OCOI's version issued four extra round-trips per row and
    could reach 400 queries on a busy document.
    """
    _require_configured()
    doc_id = _id(doc_id, "doc_id")
    md_col = "d.markdown_content," if include_markdown else ""
    row = await ocoi_db.fetchrow(f"""
        SELECT d.id, d.title, d.file_url, d.file_format, d.file_size, d.content_hash,
               d.conversion_status, d.extraction_status, d.verified, d.verified_at,
               d.verified_by_email, d.created_at, d.converted_at, d.extracted_at,
               d.pdf_r2_key, {md_col}
               length(d.markdown_content) AS markdown_length,
               s.id AS source_id, s.title AS source_title, s.source_type,
               s.url AS source_url
        FROM documents d LEFT JOIN sources s ON s.id = d.source_id
        WHERE d.id = $1
    """, doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    doc = dict(row)
    doc["has_file"] = bool(doc.pop("pdf_r2_key", None))

    runs = await ocoi_db.fetch("""
        SELECT id, extractor_type, model_version, entities_found,
               relationships_found, created_at
        FROM extraction_runs WHERE document_id = $1 ORDER BY created_at DESC
    """, doc_id)

    edges = _rows(await ocoi_db.fetch("""
        SELECT id, source_entity_type, source_entity_id, target_entity_type,
               target_entity_id, relationship_type, details, restriction_type,
               restriction_end_date, confidence, origin_kind, verified
        FROM entity_relationships WHERE document_id = $1 ORDER BY created_at
    """, doc_id))

    from app.api.ocoi import _hydrate_names
    names = await _hydrate_names(edges)
    for e in edges:
        e["source_name"] = names.get(
            (e["source_entity_type"], e["source_entity_id"]), {}).get("name", "")
        e["target_name"] = names.get(
            (e["target_entity_type"], e["target_entity_id"]), {}).get("name", "")

    doc["extraction_runs"] = _rows(runs)
    doc["relationships"] = edges
    return _ok(doc)


# ── documents: verify (with the cascade) ──────────────────────────────────────

class VerifyBody(BaseModel):
    verified: bool = True


@router.patch("/documents/{doc_id}/verify")
@limiter.limit("60/minute")
async def verify_document(request: Request, doc_id: str, body: VerifyBody,
                          user: User = Depends(get_admin_user)):
    """Mark a document human-reviewed, cascading to all of its relationships.

    The cascade is a SET, not an OR: un-verifying a document un-verifies every
    edge extracted from it. That is OCOI's semantics and it is the right one —
    the flag means "a human checked this document's extraction", and there is no
    per-edge review surface that could disagree.

    Who verified it is recorded as the OVER admin's email. OCOI stored a
    users.id into a table that did not travel with the corpus; the migration
    already resolved those to `verified_by_email` for exactly this reason.
    """
    _require_configured()
    doc_id = _id(doc_id, "doc_id")
    exists = await ocoi_db.fetchval("SELECT 1 FROM documents WHERE id = $1", doc_id)
    if not exists:
        raise HTTPException(status_code=404, detail="Document not found")

    email = getattr(user, "email", None)
    await ocoi_db.execute("""
        UPDATE documents SET verified = $2,
               verified_at = CASE WHEN $2 THEN $3::timestamp ELSE NULL END,
               verified_by_email = CASE WHEN $2 THEN $4::text ELSE NULL END
        WHERE id = $1
    """, doc_id, body.verified, _now(), email)
    n = await ocoi_db.execute(
        "UPDATE entity_relationships SET verified = $2 WHERE document_id = $1",
        doc_id, body.verified)
    logger.info("ocoi admin: document %s verified=%s by %s (%s)",
                doc_id, body.verified, email, n)
    return _ok({"id": doc_id, "verified": body.verified,
                "verified_by_email": email if body.verified else None})


# ── documents: re-queue extraction ────────────────────────────────────────────

@router.post("/documents/{doc_id}/reextract")
@limiter.limit("30/minute")
async def requeue_extraction(request: Request, doc_id: str,
                             user: User = Depends(get_admin_user)):
    """Drop this document's extraction and queue it to be redone by the worker.

    OCOI deleted the runs and edges, COMMITTED, and only then checked whether an
    extraction was already running — returning 409 after the data was already
    gone, with nothing scheduled to rebuild it. Here the delete and the status
    flip are one statement sequence with no guard between them that can fail
    after the destructive part.

    It also clears `verified`: the edges are about to be re-created unverified,
    and leaving the document marked verified would assert a human had checked
    extractions that no longer exist.
    """
    _require_configured()
    doc_id = _id(doc_id, "doc_id")
    row = await ocoi_db.fetchrow(
        "SELECT conversion_status FROM documents WHERE id = $1", doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    if row["conversion_status"] != "converted":
        raise HTTPException(
            status_code=409,
            detail="למסמך אין טקסט מומר — יש להריץ המרה מחדש דרך ה-worker לפני חילוץ.")

    rels = await ocoi_db.fetchval(
        "SELECT count(*) FROM entity_relationships WHERE document_id = $1", doc_id)
    await ocoi_db.execute(
        "DELETE FROM entity_relationships WHERE document_id = $1", doc_id)
    await ocoi_db.execute(
        "DELETE FROM extraction_runs WHERE document_id = $1", doc_id)
    await ocoi_db.execute("""
        UPDATE documents
           SET extraction_status = 'pending', extracted_at = NULL,
               verified = false, verified_at = NULL, verified_by_email = NULL
         WHERE id = $1
    """, doc_id)
    logger.info("ocoi admin: %s re-queued for extraction by %s (dropped %s edges)",
                doc_id, getattr(user, "email", "?"), rels)
    return _ok({"id": doc_id, "relationships_dropped": int(rels or 0),
                "extraction_status": "pending"})


class BatchStatusBody(BaseModel):
    document_ids: list[str] = []
    filter: str | None = None          # "failed" | "no_text" | "pending"
    field: str = "extraction_status"
    value: str = "pending"


_RESETTABLE = {"extraction_status", "conversion_status"}
_FILTERS = {
    "failed": "extraction_status = 'failed'",
    "no_text": "conversion_status = 'no_text'",
    "pending": "extraction_status = 'pending'",
}


@router.post("/documents/batch/reset-status")
@limiter.limit("20/minute")
async def batch_reset_status(request: Request, body: BatchStatusBody,
                             user: User = Depends(get_admin_user)):
    """Bulk status reset — one statement, not a SELECT per id as OCOI did."""
    _require_configured()
    if body.field not in _RESETTABLE:
        raise HTTPException(status_code=400,
                            detail=f"field must be one of: {', '.join(sorted(_RESETTABLE))}")
    if body.filter:
        if body.filter not in _FILTERS:
            raise HTTPException(status_code=400,
                                detail=f"filter must be one of: {', '.join(_FILTERS)}")
        n = await ocoi_db.execute(
            f"UPDATE documents SET {body.field} = $1 WHERE {_FILTERS[body.filter]}",
            body.value)
    else:
        ids = [_id(i, "document_ids[]") for i in body.document_ids]
        if not ids:
            raise HTTPException(status_code=400,
                                detail="document_ids or filter is required")
        n = await ocoi_db.execute(
            f"UPDATE documents SET {body.field} = $1 WHERE id = ANY($2::text[])",
            body.value, ids)
    return _ok({"field": body.field, "value": body.value, "result": n})


# ── documents: delete + purge ─────────────────────────────────────────────────

async def _delete_documents(where: str, *args) -> dict:
    """Delete documents matching a predicate, with their dependants.

    Set-based on purpose: OCOI's purges pulled every row into Python to decide.
    Order matters — edges and runs first, then the documents themselves.
    """
    ids = [r["id"] for r in await ocoi_db.fetch(
        f"SELECT id FROM documents d WHERE {where}", *args)]
    if not ids:
        return {"deleted": 0, "relationships": 0, "extraction_runs": 0}
    rel = await ocoi_db.fetchval(
        "SELECT count(*) FROM entity_relationships WHERE document_id = ANY($1::text[])", ids)
    run = await ocoi_db.fetchval(
        "SELECT count(*) FROM extraction_runs WHERE document_id = ANY($1::text[])", ids)
    await ocoi_db.execute(
        "DELETE FROM entity_relationships WHERE document_id = ANY($1::text[])", ids)
    await ocoi_db.execute(
        "DELETE FROM extraction_runs WHERE document_id = ANY($1::text[])", ids)
    await ocoi_db.execute("DELETE FROM documents WHERE id = ANY($1::text[])", ids)
    return {"deleted": len(ids), "relationships": int(rel or 0),
            "extraction_runs": int(run or 0)}


@router.delete("/documents/{doc_id}")
@limiter.limit("30/minute")
async def delete_document(request: Request, doc_id: str,
                          user: User = Depends(get_admin_user)):
    _require_configured()
    doc_id = _id(doc_id, "doc_id")
    if not await ocoi_db.fetchval("SELECT 1 FROM documents WHERE id = $1", doc_id):
        raise HTTPException(status_code=404, detail="Document not found")
    res = await _delete_documents("d.id = $1", doc_id)
    logger.info("ocoi admin: %s deleted document %s (%s)",
                getattr(user, "email", "?"), doc_id, res)
    return _ok(res)


class PurgeBody(BaseModel):
    kind: str                      # "metadata_only" | "non_pdf"
    dry_run: bool = True


@router.post("/documents/purge")
@limiter.limit("6/minute")
async def purge_documents(request: Request, body: PurgeBody,
                          user: User = Depends(get_admin_user)):
    """Bulk removal of two known-junk classes.

    `dry_run` defaults to TRUE. OCOI's equivalents were DELETE endpoints that
    acted immediately with no preview — on a corpus where "metadata only" is a
    legitimate, deliberate state for 2,117 of 2,971 documents, that is a very
    sharp edge to leave unguarded.
    """
    _require_configured()
    if body.kind == "metadata_only":
        # Careful: metadata-only is NORMAL here (the CKAN path stores no bytes
        # by design). This targets rows with neither text NOR a stored file.
        where = ("(d.markdown_content IS NULL OR length(d.markdown_content) = 0) "
                 "AND d.pdf_r2_key IS NULL")
        args: tuple = ()
    elif body.kind == "non_pdf":
        where = ("lower(coalesce(d.file_format,'')) <> 'pdf' "
                 "AND lower(split_part(coalesce(d.file_url,''), '?', 1)) NOT LIKE '%.pdf'")
        args = ()
    else:
        raise HTTPException(status_code=400,
                            detail="kind must be metadata_only or non_pdf")

    if body.dry_run:
        n = await ocoi_db.fetchval(
            f"SELECT count(*) FROM documents d WHERE {where}", *args)
        sample = await ocoi_db.fetch(
            f"SELECT id, title, file_format FROM documents d WHERE {where} LIMIT 20", *args)
        return _ok({"dry_run": True, "would_delete": int(n or 0),
                    "sample": _rows(sample)})
    res = await _delete_documents(where, *args)
    logger.warning("ocoi admin: %s purged %s -> %s",
                   getattr(user, "email", "?"), body.kind, res)
    return _ok({"dry_run": False, **res})


# ── stats ─────────────────────────────────────────────────────────────────────

@router.get("/stats")
@limiter.limit("60/minute")
async def admin_stats(request: Request, user: User = Depends(get_admin_user)):
    """Corpus counters + the review backlog, in one round trip."""
    _require_configured()
    row = await ocoi_db.fetchrow("""
        SELECT (SELECT count(*) FROM documents)              AS documents,
               (SELECT count(*) FROM documents WHERE verified) AS verified,
               (SELECT count(*) FROM documents
                 WHERE extraction_status = 'pending')        AS extraction_pending,
               (SELECT count(*) FROM documents
                 WHERE extraction_status = 'failed')         AS extraction_failed,
               (SELECT count(*) FROM documents
                 WHERE conversion_status = 'no_text')        AS no_text,
               (SELECT count(*) FROM documents
                 WHERE pdf_r2_key IS NOT NULL)               AS with_file,
               (SELECT count(*) FROM persons)                AS persons,
               (SELECT count(*) FROM companies)              AS companies,
               (SELECT count(*) FROM associations)           AS associations,
               (SELECT count(*) FROM domains)                AS domains,
               (SELECT count(*) FROM entity_relationships)   AS relationships,
               (SELECT count(*) FROM sources)                AS sources,
               (SELECT count(*) FROM registry_records)       AS registry_records,
               (SELECT count(*) FROM suggestions
                 WHERE status = 'pending')                   AS suggestions_pending,
               (SELECT count(*) FROM entity_match_proposals
                 WHERE status = 'pending')                   AS proposals_pending,
               (SELECT count(*) FROM ignored_resources)      AS ignored_resources
    """)
    return _ok(dict(row) if row else {})
