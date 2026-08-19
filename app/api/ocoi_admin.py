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
from datetime import datetime

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


# Naive Asia/Jerusalem, matching the corpus. OCOI was inconsistent — document
# writes used Israel time while match and suggestion writes used naive UTC, into
# the same column type. One clock here, and it comes from ocoi_db rather than
# the container clock (see the note there).
_now = ocoi_db.now_local


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


# ═══════════════════════════════════════════════════════════════════════════
# WAVE 2 — entities, relationships, merges, audit
# ═══════════════════════════════════════════════════════════════════════════

import json as _json  # noqa: E402
import re as _re      # noqa: E402

_ENTITY_TABLES = {
    "person": "persons",
    "company": "companies",
    "association": "associations",
    "domain": "domains",
}
_ENTITY_TYPES = tuple(_ENTITY_TABLES)
# Types the merge machinery accepts. `domain` is excluded exactly as in OCOI:
# domain names are short topical labels, and fuzzy-merging them is unsafe.
_MERGEABLE = ("person", "company", "association")

_EDITABLE = {
    "person": ("name_hebrew", "name_english", "title", "position", "ministry"),
    "company": ("name_hebrew", "name_english", "registration_number",
                "company_type", "status"),
    "association": ("name_hebrew", "name_english", "registration_number", "status"),
    "domain": ("name_hebrew", "name_english", "description"),
}

# ONE placeholder predicate, used by create, by the audit report and by the
# audit cleanup. OCOI had two that disagreed: the report used a SQL regex while
# cleanup used a Python helper that ALSO caught "nan", "undefined" and
# quote-only names — so cleanup deleted a strict superset of what the operator
# had been shown. An audit you cannot trust to preview its own deletions is
# worse than no audit.
_PLACEHOLDER_WORDS = frozenset({"null", "none", "n/a", "nan", "undefined", "-"})
_PLACEHOLDER_CHARS = _re.compile(r"^[\*_\-–—=\.,\s'\"`׳״]+$")


def _is_placeholder(name) -> bool:
    if name is None:
        return True
    s = " ".join(str(name).split())
    if not s:
        return True
    if s.lower() in _PLACEHOLDER_WORDS:
        return True
    return bool(_PLACEHOLDER_CHARS.match(s))


def _etype(t: str) -> str:
    if t not in _ENTITY_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"entity_type must be one of: {', '.join(_ENTITY_TYPES)}")
    return t


def _aliases_of(raw) -> list[str]:
    return ocoi_db.decode_aliases(raw)


def _dump_aliases(items) -> str | None:
    clean = sorted({str(x).strip() for x in items if str(x).strip()})
    return _json.dumps(clean, ensure_ascii=False) if clean else None


# NOTE: the merge route MUST be registered before /entities/{entity_type},
# or Starlette matches "merge" as an entity type and the endpoint answers
# 400 for every call. Route order is load-bearing here — the same trap the
# original hit twice (see its comment about two GET /users handlers).
# ── merge ─────────────────────────────────────────────────────────────────────

async def _merge_entities(keep_type: str, keep_id: str,
                          merge_type: str, merge_id: str) -> dict:
    """Fold `merge` into `keep`. Order is load-bearing.

    Step 1 is the one that cannot move: `ix_rel_compound` is UNIQUE over
    (src_type, src_id, tgt_type, tgt_id, rel_type, document_id), so the moment
    both entities carry the same edge on the same document — which is precisely
    what makes them look like duplicates — the UPDATE in step 2 would violate it
    and abort the whole transaction. The colliding row on the MERGE side is
    deleted first; the keep side's copy survives, so no edge is lost.

    Beyond OCOI's version this also repoints `entity_match_proposals` and
    `suggestions` (it left both dangling at ids that no longer existed),
    preserves `hidden` (merging a hidden duplicate into a visible keeper
    silently un-hid the name), and carries a registry match across when the
    keeper has none (the loser's was simply discarded).
    """
    def _n(tag) -> int:
        """asyncpg returns the command tag, e.g. "UPDATE 3" / "DELETE 0"."""
        try:
            return int(str(tag).rsplit(" ", 1)[-1])
        except (ValueError, AttributeError, TypeError):
            return 0

    ktab, mtab = _ENTITY_TABLES[keep_type], _ENTITY_TABLES[merge_type]
    keep = await ocoi_db.fetchrow(f"SELECT * FROM {ktab} WHERE id = $1", keep_id)
    if keep is None:
        raise HTTPException(status_code=404, detail="keep entity not found")
    merge = await ocoi_db.fetchrow(f"SELECT * FROM {mtab} WHERE id = $1", merge_id)
    if merge is None:
        raise HTTPException(status_code=404, detail="merge entity not found")

    # 1. collision pre-delete — MUST precede the UPDATEs
    dup_tag = await ocoi_db.execute("""
        WITH doomed AS (
          SELECT m.id FROM entity_relationships m
          JOIN entity_relationships k
            ON k.source_entity_type = $1 AND k.source_entity_id = $2
           AND k.target_entity_type = m.target_entity_type
           AND k.target_entity_id   = m.target_entity_id
           AND k.relationship_type  = m.relationship_type
           AND k.document_id        = m.document_id
          WHERE m.source_entity_type = $3 AND m.source_entity_id = $4
          UNION
          SELECT m.id FROM entity_relationships m
          JOIN entity_relationships k
            ON k.target_entity_type = $1 AND k.target_entity_id = $2
           AND k.source_entity_type = m.source_entity_type
           AND k.source_entity_id   = m.source_entity_id
           AND k.relationship_type  = m.relationship_type
           AND k.document_id        = m.document_id
          WHERE m.target_entity_type = $3 AND m.target_entity_id = $4
        )
        DELETE FROM entity_relationships WHERE id IN (SELECT id FROM doomed)
    """, keep_type, keep_id, merge_type, merge_id)

    # 2 + 3. repoint both sides
    moved_src = await ocoi_db.execute("""
        UPDATE entity_relationships SET source_entity_type = $1, source_entity_id = $2
         WHERE source_entity_type = $3 AND source_entity_id = $4""",
        keep_type, keep_id, merge_type, merge_id)
    moved_tgt = await ocoi_db.execute("""
        UPDATE entity_relationships SET target_entity_type = $1, target_entity_id = $2
         WHERE target_entity_type = $3 AND target_entity_id = $4""",
        keep_type, keep_id, merge_type, merge_id)

    # 4. self-loops
    await ocoi_db.execute("""
        DELETE FROM entity_relationships
         WHERE source_entity_type = $1 AND source_entity_id = $2
           AND target_entity_type = $1 AND target_entity_id = $2""", keep_type, keep_id)

    # 5. aliases — the loser's name and aliases fold into the keeper
    aliases = set(_aliases_of(keep["aliases"]))
    aliases.add((merge["name_hebrew"] or "").strip())
    aliases.update(_aliases_of(merge["aliases"]))
    aliases.discard((keep["name_hebrew"] or "").strip())
    aliases = {a for a in aliases if a and not _is_placeholder(a)}

    sets = ["aliases = $2"]
    vals: list = [keep_id, _dump_aliases(aliases)]
    # preserve hidden, and inherit a registry match if we lack one
    if merge.get("hidden") and not keep.get("hidden"):
        vals.append(True)
        sets.append(f"hidden = ${len(vals)}")
    if (keep_type == merge_type and keep_type in ("company", "association")
            and not keep.get("registration_number")
            and merge.get("registration_number")):
        vals.append(merge["registration_number"])
        sets.append(f"registration_number = ${len(vals)}")
        vals.append(merge.get("match_confidence"))
        sets.append(f"match_confidence = ${len(vals)}")
        vals.append(merge.get("registry_record_id"))
        sets.append(f"registry_record_id = ${len(vals)}")
    await ocoi_db.execute(
        f"UPDATE {ktab} SET {', '.join(sets)} WHERE id = $1", *vals)

    # 6. repoint referrers OCOI left dangling, then drop the loser
    # Only PENDING proposals go: the losing id is gone, so they can never be
    # acted on. Resolved ones stay — they record that a human made a decision,
    # and the list renders a vanished side as "(נמחק)". Deleting them all is
    # what erased the very approval that triggered the merge.
    await ocoi_db.execute("""
        DELETE FROM entity_match_proposals
         WHERE status = 'pending'
           AND ((entity_type = $1 AND entity_id = $2)
             OR (target_type = $1 AND target_id = $2))""", merge_type, merge_id)
    await ocoi_db.execute("""
        UPDATE suggestions SET target_kind = $1, target_id = $2
         WHERE target_kind = $3 AND target_id = $4""",
        keep_type, keep_id, merge_type, merge_id)
    await ocoi_db.execute(f"DELETE FROM {mtab} WHERE id = $1", merge_id)

    return {
        "kept_id": keep_id, "kept_type": keep_type,
        "merged_id": merge_id, "merged_type": merge_type,
        "moved_source": _n(moved_src), "moved_target": _n(moved_tgt),
        "duplicate_edges_removed": _n(dup_tag),
        "aliases": sorted(aliases),
    }


class MergeBody(BaseModel):
    keep_type: str
    keep_id: str
    merge_type: str | None = None      # defaults to keep_type
    merge_id: str


@router.post("/entities/merge")
@limiter.limit("20/minute")
async def merge_entities(request: Request, body: MergeBody,
                         user: User = Depends(get_admin_user)):
    """Merge one entity into another. Cross-type is allowed (keep_type may
    differ from merge_type), which is how a person mis-extracted as a company
    gets folded back."""
    _require_configured()
    kt = _etype(body.keep_type)
    mt = _etype(body.merge_type or body.keep_type)
    kid = _id(body.keep_id, "keep_id")
    mid = _id(body.merge_id, "merge_id")
    if kt == mt and kid == mid:
        raise HTTPException(status_code=400, detail="cannot merge an entity into itself")
    res = await _merge_entities(kt, kid, mt, mid)
    logger.info("ocoi admin: %s merged %s %s into %s %s -> %s",
                getattr(user, "email", "?"), mt, mid, kt, kid, res)
    return _ok(res)


# ── entities: list / create / update / delete ─────────────────────────────────

@router.get("/entities/{entity_type}")
@limiter.limit("60/minute")
async def list_entities(
    request: Request, entity_type: str,
    q: str | None = Query(None),
    hidden: bool | None = Query(None),
    unmatched: bool | None = Query(None, description="no registry match yet"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_admin_user),
):
    _require_configured()
    t = _etype(entity_type)
    table = _ENTITY_TABLES[t]
    cols = ", ".join(_EDITABLE[t])
    where, args = ["1=1"], []
    if q:
        args.append(f"%{q}%")
        where.append(
            f"(name_hebrew ILIKE ${len(args)} OR coalesce(aliases,'') ILIKE ${len(args)})")
    if hidden is not None:
        where.append("hidden IS TRUE" if hidden else "hidden IS NOT TRUE")
    if unmatched is not None and t in ("company", "association"):
        where.append("registration_number IS NULL" if unmatched
                     else "registration_number IS NOT NULL")
    w = " AND ".join(where)
    total = await ocoi_db.fetchval(f"SELECT count(*) FROM {table} WHERE {w}", *args) or 0
    rows = await ocoi_db.fetch(f"""
        SELECT id, {cols}, aliases, hidden, created_at,
               (SELECT count(*) FROM entity_relationships r
                 WHERE (r.source_entity_type = '{t}' AND r.source_entity_id = {table}.id)
                    OR (r.target_entity_type = '{t}' AND r.target_entity_id = {table}.id)
               ) AS connections
        FROM {table} WHERE {w}
        ORDER BY name_hebrew
        LIMIT ${len(args)+1} OFFSET ${len(args)+2}
    """, *args, limit, offset)
    out = []
    for r in rows:
        d = dict(r)
        d["aliases"] = _aliases_of(d.get("aliases"))
        d["entity_type"] = t
        out.append(d)
    return _ok(out, total=int(total), limit=limit, offset=offset)


class EntityBody(BaseModel):
    name_hebrew: str | None = None
    name_english: str | None = None
    title: str | None = None
    position: str | None = None
    ministry: str | None = None
    registration_number: str | None = None
    company_type: str | None = None
    status: str | None = None
    description: str | None = None
    aliases: list[str] | None = None
    hidden: bool | None = None


@router.post("/entities/{entity_type}")
@limiter.limit("30/minute")
async def create_entity(request: Request, entity_type: str, body: EntityBody,
                        user: User = Depends(get_admin_user)):
    """Create an entity.

    Applies the placeholder check OCOI's manual-create path skipped: its
    `upsert_*` helpers hid junk names, but the admin POST did not, so a name
    typed as "***" entered the corpus visible.
    """
    _require_configured()
    t = _etype(entity_type)
    name = (body.name_hebrew or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name_hebrew is required")
    if _is_placeholder(name):
        raise HTTPException(status_code=400,
                            detail="השם נראה כמציין מקום (ריק / null / סימנים בלבד)")
    dup = await ocoi_db.fetchval(
        f"SELECT id FROM {_ENTITY_TABLES[t]} WHERE name_hebrew = $1 LIMIT 1", name)
    if dup:
        raise HTTPException(status_code=409, detail=f"כבר קיימת ישות בשם הזה ({dup})")
    new_id = str(uuid.uuid4())
    cols, vals = ["id", "name_hebrew"], [new_id, name]
    for f in _EDITABLE[t]:
        if f == "name_hebrew":
            continue
        v = getattr(body, f, None)
        if v is not None:
            cols.append(f)
            vals.append(v)
    if body.aliases is not None:
        cols.append("aliases")
        vals.append(_dump_aliases(body.aliases))
    ph = ", ".join(f"${i+1}" for i in range(len(vals)))
    await ocoi_db.execute(
        f"INSERT INTO {_ENTITY_TABLES[t]} ({', '.join(cols)}) VALUES ({ph})", *vals)
    return _ok({"id": new_id, "entity_type": t, "name_hebrew": name})


@router.patch("/entities/{entity_type}/{entity_id}")
@limiter.limit("30/minute")
async def update_entity(request: Request, entity_type: str, entity_id: str,
                        body: EntityBody, keep_alias: bool = Query(False),
                        user: User = Depends(get_admin_user)):
    """Update an entity.

    `keep_alias=true` folds the OLD name into the alias list on rename. OCOI had
    an ordering bug: it wrote the kept alias first and then let an explicit
    `aliases` field in the same request overwrite it, silently losing the old
    name. Here the two are merged, so sending both keeps both.
    """
    _require_configured()
    t = _etype(entity_type)
    eid = _id(entity_id, "entity_id")
    table = _ENTITY_TABLES[t]
    cur = await ocoi_db.fetchrow(
        f"SELECT name_hebrew, aliases FROM {table} WHERE id = $1", eid)
    if cur is None:
        raise HTTPException(status_code=404, detail="Entity not found")

    new_name = None
    if body.name_hebrew is not None:
        new_name = body.name_hebrew.strip()
        if not new_name:
            raise HTTPException(status_code=400, detail="name_hebrew cannot be empty")
        if _is_placeholder(new_name):
            raise HTTPException(status_code=400, detail="השם נראה כמציין מקום")

    sets, args = [], []
    for f in _EDITABLE[t]:
        v = getattr(body, f, None)
        if v is None:
            continue
        args.append(v.strip() if isinstance(v, str) else v)
        sets.append(f"{f} = ${len(args)}")
    if body.hidden is not None:
        args.append(body.hidden)
        sets.append(f"hidden = ${len(args)}")

    renaming = bool(new_name and new_name != cur["name_hebrew"])
    if body.aliases is not None or (keep_alias and renaming):
        merged = set(body.aliases if body.aliases is not None
                     else _aliases_of(cur["aliases"]))
        if keep_alias and renaming:
            merged.add(cur["name_hebrew"])
        merged.discard(new_name or cur["name_hebrew"])
        args.append(_dump_aliases(merged))
        sets.append(f"aliases = ${len(args)}")

    if not sets:
        return _ok({"id": eid, "changed": False})
    args.append(eid)
    await ocoi_db.execute(
        f"UPDATE {table} SET {', '.join(sets)} WHERE id = ${len(args)}", *args)
    return _ok({"id": eid, "changed": True})


@router.delete("/entities/{entity_type}/{entity_id}")
@limiter.limit("30/minute")
async def delete_entity(request: Request, entity_type: str, entity_id: str,
                        user: User = Depends(get_admin_user)):
    """Delete an entity AND everything pointing at it.

    OCOI deleted relationships for person/company/association but NOT for
    domain — `DELETE /domains/{id}` removed only the row. That one omission is
    the main manufacturer of the orphan-relationship class its own audit
    endpoint then existed to clean up. It also left `entity_match_proposals`
    and `suggestions` dangling for every type. One path here, all four types,
    all referrers.
    """
    _require_configured()
    t = _etype(entity_type)
    eid = _id(entity_id, "entity_id")
    if not await ocoi_db.fetchval(
            f"SELECT 1 FROM {_ENTITY_TABLES[t]} WHERE id = $1", eid):
        raise HTTPException(status_code=404, detail="Entity not found")
    rel = await ocoi_db.fetchval("""
        SELECT count(*) FROM entity_relationships
         WHERE (source_entity_type = $1 AND source_entity_id = $2)
            OR (target_entity_type = $1 AND target_entity_id = $2)""", t, eid)
    await ocoi_db.execute("""
        DELETE FROM entity_relationships
         WHERE (source_entity_type = $1 AND source_entity_id = $2)
            OR (target_entity_type = $1 AND target_entity_id = $2)""", t, eid)
    await ocoi_db.execute("""
        DELETE FROM entity_match_proposals
         WHERE (entity_type = $1 AND entity_id = $2)
            OR (target_type = $1 AND target_id = $2)""", t, eid)
    await ocoi_db.execute(
        "DELETE FROM suggestions WHERE target_kind = $1 AND target_id = $2", t, eid)
    await ocoi_db.execute(f"DELETE FROM {_ENTITY_TABLES[t]} WHERE id = $1", eid)
    logger.info("ocoi admin: %s deleted %s %s (%s edges)",
                getattr(user, "email", "?"), t, eid, rel)
    return _ok({"id": eid, "relationships_deleted": int(rel or 0)})


# ── relationships ─────────────────────────────────────────────────────────────

@router.get("/relationships")
@limiter.limit("60/minute")
async def list_relationships(
    request: Request,
    q: str | None = Query(None, description="matches entity names or relationship type"),
    origin_kind: str | None = Query(None),
    verified: bool | None = Query(None),
    document_id: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    user: User = Depends(get_admin_user),
):
    """Relationship list with both endpoint names resolved.

    OCOI issued FOUR extra queries per row here (two name lookups, the document,
    the source) — 401 round trips at limit=100 — and its `q` searched only
    `relationship_type` despite the UI implying it searched names. Here names
    come from one hydration pass per entity type, and `q` really does match
    names.
    """
    _require_configured()
    where, args = ["1=1"], []
    if origin_kind:
        args.append(origin_kind)
        where.append(f"r.origin_kind = ${len(args)}")
    if verified is not None:
        args.append(verified)
        where.append(f"r.verified = ${len(args)}")
    if document_id:
        args.append(_id(document_id, "document_id"))
        where.append(f"r.document_id = ${len(args)}")
    if q:
        # Name search without a polymorphic join: resolve matching ids per type
        # first, then filter the edge table on them.
        like = f"%{q}%"
        ids: list[str] = []
        for t, tab in _ENTITY_TABLES.items():
            rows = await ocoi_db.fetch(
                f"SELECT id FROM {tab} WHERE name_hebrew ILIKE $1 LIMIT 500", like)
            ids.extend(r["id"] for r in rows)
        args.append(like)
        rel_clause = f"r.relationship_type ILIKE ${len(args)}"
        if ids:
            args.append(ids)
            where.append(f"({rel_clause} OR r.source_entity_id = ANY(${len(args)}::text[])"
                         f" OR r.target_entity_id = ANY(${len(args)}::text[]))")
        else:
            where.append(rel_clause)
    w = " AND ".join(where)

    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM entity_relationships r WHERE {w}", *args) or 0
    rows = _rows(await ocoi_db.fetch(f"""
        SELECT r.id, r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id, r.relationship_type,
               r.details, r.restriction_type, r.confidence, r.origin_kind,
               r.verified, r.document_id, r.created_at,
               d.title AS document_title
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE {w}
        ORDER BY r.created_at DESC NULLS LAST
        LIMIT ${len(args)+1} OFFSET ${len(args)+2}
    """, *args, limit, offset))

    from app.api.ocoi import _hydrate_names
    names = await _hydrate_names(rows)
    for e in rows:
        e["source_name"] = names.get(
            (e["source_entity_type"], e["source_entity_id"]), {}).get("name", "")
        e["target_name"] = names.get(
            (e["target_entity_type"], e["target_entity_id"]), {}).get("name", "")
    return _ok(rows, total=int(total), limit=limit, offset=offset)


class RelBody(BaseModel):
    source_entity_type: str
    source_entity_id: str
    target_entity_type: str
    target_entity_id: str
    relationship_type: str
    document_id: str
    details: str | None = None
    restriction_type: str | None = None
    confidence: float = 0.5
    origin_kind: str = "coi_declaration"


@router.post("/relationships")
@limiter.limit("30/minute")
async def create_relationship(request: Request, body: RelBody,
                              user: User = Depends(get_admin_user)):
    """Create an edge, refusing duplicates and dangling endpoints.

    OCOI inserted blind: a repeat violated `ix_rel_compound` and surfaced as an
    unexplained 500, and nothing checked that either endpoint existed, so a typo
    produced an orphan its own audit tool later reported.
    """
    _require_configured()
    st, tt = _etype(body.source_entity_type), _etype(body.target_entity_type)
    sid = _id(body.source_entity_id, "source_entity_id")
    tid = _id(body.target_entity_id, "target_entity_id")
    did = _id(body.document_id, "document_id")
    for t, i, lbl in ((st, sid, "source"), (tt, tid, "target")):
        if not await ocoi_db.fetchval(
                f"SELECT 1 FROM {_ENTITY_TABLES[t]} WHERE id = $1", i):
            raise HTTPException(status_code=400, detail=f"{lbl} entity does not exist")
    if not await ocoi_db.fetchval("SELECT 1 FROM documents WHERE id = $1", did):
        raise HTTPException(status_code=400, detail="document does not exist")
    if st == tt and sid == tid:
        raise HTTPException(status_code=400, detail="an entity cannot relate to itself")
    dup = await ocoi_db.fetchval("""
        SELECT id FROM entity_relationships
         WHERE source_entity_type=$1 AND source_entity_id=$2
           AND target_entity_type=$3 AND target_entity_id=$4
           AND relationship_type=$5 AND document_id=$6 LIMIT 1""",
        st, sid, tt, tid, body.relationship_type, did)
    if dup:
        raise HTTPException(status_code=409, detail=f"קשר זהה כבר קיים ({dup})")
    new_id = str(uuid.uuid4())
    await ocoi_db.execute("""
        INSERT INTO entity_relationships
          (id, source_entity_type, source_entity_id, target_entity_type,
           target_entity_id, relationship_type, details, restriction_type,
           document_id, confidence, origin_kind, verified, created_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,false,$12)""",
        new_id, st, sid, tt, tid, body.relationship_type, body.details,
        body.restriction_type, did, body.confidence, body.origin_kind, _now())
    return _ok({"id": new_id})


class RelIdsBody(BaseModel):
    ids: list[str]


@router.post("/relationships/bulk-delete")
@limiter.limit("20/minute")
async def bulk_delete_relationships(request: Request, body: RelIdsBody,
                                    user: User = Depends(get_admin_user)):
    """Delete many edges. Reports what was ACTUALLY deleted — OCOI returned the
    length of the request, so a list of already-gone ids reported success."""
    _require_configured()
    ids = [_id(i, "ids[]") for i in body.ids]
    if not ids:
        raise HTTPException(status_code=400, detail="ids is required")
    before = await ocoi_db.fetchval(
        "SELECT count(*) FROM entity_relationships WHERE id = ANY($1::text[])", ids)
    await ocoi_db.execute(
        "DELETE FROM entity_relationships WHERE id = ANY($1::text[])", ids)
    return _ok({"requested": len(ids), "deleted": int(before or 0)})


@router.delete("/relationships/{rel_id}")
@limiter.limit("30/minute")
async def delete_relationship(request: Request, rel_id: str,
                              user: User = Depends(get_admin_user)):
    _require_configured()
    rid = _id(rel_id, "rel_id")
    if not await ocoi_db.fetchval(
            "SELECT 1 FROM entity_relationships WHERE id = $1", rid):
        raise HTTPException(status_code=404, detail="Relationship not found")
    await ocoi_db.execute("DELETE FROM entity_relationships WHERE id = $1", rid)
    return _ok({"id": rid})


# ── audit ─────────────────────────────────────────────────────────────────────

@router.get("/audit")
@limiter.limit("10/minute")
async def audit(request: Request, limit: int = Query(100, ge=1, le=1000),
                user: User = Depends(get_admin_user)):
    """Placeholder-named entities and orphaned relationships.

    Uses THE SAME predicate the cleanup uses (see `_is_placeholder`). OCOI's
    report and its cleanup disagreed — cleanup deleted a strict superset of what
    the operator was shown, which makes the preview actively misleading.

    Bounded by `limit` per bucket; OCOI returned every id unbounded.
    """
    _require_configured()
    garbage: dict[str, dict] = {}
    orphans: dict[str, dict] = {}
    for t, tab in _ENTITY_TABLES.items():
        rows = await ocoi_db.fetch(
            f"SELECT id, name_hebrew FROM {tab} ORDER BY name_hebrew LIMIT 20000")
        bad = [{"id": r["id"], "name": r["name_hebrew"]}
               for r in rows if _is_placeholder(r["name_hebrew"])]
        garbage[t] = {"count": len(bad), "items": bad[:limit]}

        orow = await ocoi_db.fetch(f"""
            SELECT id, cnt FROM (
              SELECT r.source_entity_id AS id, count(*) AS cnt
                FROM entity_relationships r
                LEFT JOIN {tab} e ON e.id = r.source_entity_id
               WHERE r.source_entity_type = $1 AND e.id IS NULL
               GROUP BY r.source_entity_id
              UNION ALL
              SELECT r.target_entity_id, count(*)
                FROM entity_relationships r
                LEFT JOIN {tab} e ON e.id = r.target_entity_id
               WHERE r.target_entity_type = $1 AND e.id IS NULL
               GROUP BY r.target_entity_id
            ) x ORDER BY cnt DESC LIMIT $2""", t, limit)
        tot = await ocoi_db.fetchval(f"""
            SELECT count(*) FROM entity_relationships r
             LEFT JOIN {tab} s ON s.id = r.source_entity_id
             LEFT JOIN {tab} g ON g.id = r.target_entity_id
             WHERE (r.source_entity_type = $1 AND s.id IS NULL)
                OR (r.target_entity_type = $1 AND g.id IS NULL)""", t)
        orphans[t] = {"relationships": int(tot or 0), "ids": _rows(orow)}
    return _ok({"placeholder_entities": garbage, "orphan_relationships": orphans})


class CleanupBody(BaseModel):
    placeholder_entities: bool = True
    orphan_relationships: bool = True
    dry_run: bool = True


@router.post("/audit/cleanup")
@limiter.limit("6/minute")
async def audit_cleanup(request: Request, body: CleanupBody,
                        user: User = Depends(get_admin_user)):
    """Delete what /audit reports — same predicate, so the preview is honest.
    `dry_run` defaults to true."""
    _require_configured()
    out: dict = {"dry_run": body.dry_run, "placeholder_entities": {},
                 "orphan_relationships": {}}
    for t, tab in _ENTITY_TABLES.items():
        if body.placeholder_entities:
            rows = await ocoi_db.fetch(f"SELECT id, name_hebrew FROM {tab}")
            ids = [r["id"] for r in rows if _is_placeholder(r["name_hebrew"])]
            rel = 0
            if ids:
                rel = await ocoi_db.fetchval("""
                    SELECT count(*) FROM entity_relationships
                     WHERE (source_entity_type=$1 AND source_entity_id = ANY($2::text[]))
                        OR (target_entity_type=$1 AND target_entity_id = ANY($2::text[]))
                """, t, ids) or 0
                if not body.dry_run:
                    await ocoi_db.execute("""
                        DELETE FROM entity_relationships
                         WHERE (source_entity_type=$1 AND source_entity_id = ANY($2::text[]))
                            OR (target_entity_type=$1 AND target_entity_id = ANY($2::text[]))
                    """, t, ids)
                    await ocoi_db.execute("""
                        DELETE FROM entity_match_proposals
                         WHERE (entity_type=$1 AND entity_id = ANY($2::text[]))
                            OR (target_type=$1 AND target_id = ANY($2::text[]))""", t, ids)
                    await ocoi_db.execute(
                        f"DELETE FROM {tab} WHERE id = ANY($1::text[])", ids)
            out["placeholder_entities"][t] = {"entities": len(ids),
                                              "relationships": int(rel)}
        if body.orphan_relationships:
            n = await ocoi_db.fetchval(f"""
                SELECT count(*) FROM entity_relationships r
                 LEFT JOIN {tab} s ON s.id = r.source_entity_id
                 LEFT JOIN {tab} g ON g.id = r.target_entity_id
                 WHERE (r.source_entity_type = $1 AND s.id IS NULL)
                    OR (r.target_entity_type = $1 AND g.id IS NULL)""", t) or 0
            if not body.dry_run and n:
                await ocoi_db.execute(f"""
                    DELETE FROM entity_relationships r
                     WHERE (r.source_entity_type = $1
                            AND NOT EXISTS (SELECT 1 FROM {tab} e WHERE e.id = r.source_entity_id))
                        OR (r.target_entity_type = $1
                            AND NOT EXISTS (SELECT 1 FROM {tab} e WHERE e.id = r.target_entity_id))
                """, t)
            out["orphan_relationships"][t] = int(n)
    if not body.dry_run:
        logger.warning("ocoi admin: %s ran audit cleanup -> %s",
                       getattr(user, "email", "?"), out)
    return _ok(out)


# ═══════════════════════════════════════════════════════════════════════════
# WAVE 3 — duplicate proposals, clusters, jobs
# ═══════════════════════════════════════════════════════════════════════════

from fastapi import BackgroundTasks  # noqa: E402

from app.services import ocoi_match  # noqa: E402

_PROPOSAL_STATUSES = ("pending", "approved", "rejected", "dismissed")


# NOTE: every literal /matches/... path below is declared BEFORE the
# /matches/{proposal_id}/... ones. Same trap as /entities/merge — Starlette
# matches in registration order and would read "clusters" as a proposal id.

@router.get("/jobs")
@limiter.limit("60/minute")
async def list_jobs(request: Request, user: User = Depends(get_admin_user)):
    """State of every long-running job.

    Lives in a table rather than a module dict: OCOI's status endpoint answered
    from whichever process happened to serve it, so a poll could report "not
    running" for a job that was, and a redeploy mid-run left the flag stuck true
    with nothing able to clear it.
    """
    _require_configured()
    return _ok(await ocoi_match.job_status())


@router.post("/jobs/{kind}/reset")
@limiter.limit("20/minute")
async def reset_job(request: Request, kind: str,
                    user: User = Depends(get_admin_user)):
    """Force-clear a stuck slot — the escape hatch OCOI needed and lacked."""
    _require_configured()
    await ocoi_match.reset_job(kind)
    logger.warning("ocoi admin: %s reset job %s", getattr(user, "email", "?"), kind)
    return _ok({"kind": kind, "status": "idle"})


class ScanBody(BaseModel):
    kinds: list[str] | None = None


@router.post("/matches/scan")
@limiter.limit("6/minute")
async def start_duplicate_scan(request: Request, body: ScanBody,
                               background: BackgroundTasks,
                               user: User = Depends(get_admin_user)):
    """Start the duplicate scan. 409 if one is already running."""
    _require_configured()
    kinds = tuple(k for k in (body.kinds or ocoi_match.SCAN_KINDS)
                  if k in ocoi_match.SCAN_KINDS)
    if not kinds:
        raise HTTPException(
            status_code=400,
            detail=f"kinds must be from: {', '.join(ocoi_match.SCAN_KINDS)}")
    if not await ocoi_match.claim_job(ocoi_match.JOB_SCAN):
        raise HTTPException(status_code=409, detail="סריקת כפילויות כבר רצה")
    background.add_task(ocoi_match.run_duplicate_scan, kinds)
    logger.info("ocoi admin: %s started duplicate scan %s",
                getattr(user, "email", "?"), kinds)
    return _ok({"started": True, "kinds": list(kinds)})


@router.get("/matches/clusters")
@limiter.limit("30/minute")
async def match_clusters(request: Request,
                         entity_type: str | None = Query(None),
                         min_score: float = Query(0.85, ge=0.0, le=1.0),
                         limit: int = Query(30, ge=1, le=500),
                         user: User = Depends(get_admin_user)):
    """Connected components of pending duplicate proposals.

    A cluster is the useful review unit: three rows that are all the same
    official arrive as three pairwise proposals, and approving them one at a
    time is both slower and easy to get wrong.
    """
    _require_configured()
    if entity_type:
        _etype(entity_type)
    clusters, meta = await ocoi_match.build_clusters(entity_type, min_score, limit)
    return _ok(clusters, **meta)


class ClusterMergeBody(BaseModel):
    entity_type: str
    canonical_id: str
    member_ids: list[str]


@router.post("/matches/clusters/merge")
@limiter.limit("20/minute")
async def merge_cluster(request: Request, body: ClusterMergeBody,
                        user: User = Depends(get_admin_user)):
    """Fold every member of a cluster into the canonical row.

    Members that fail individually are reported rather than aborting the rest —
    a cluster is a review decision, and losing the whole decision because one
    member was already merged elsewhere would be needlessly brittle.
    """
    _require_configured()
    t = _etype(body.entity_type)
    if t not in ocoi_match.SCAN_KINDS:
        raise HTTPException(status_code=400,
                            detail=f"{t} is not mergeable")
    keep = _id(body.canonical_id, "canonical_id")
    members = [_id(m, "member_ids[]") for m in body.member_ids if m != keep]
    if not members:
        raise HTTPException(status_code=400, detail="member_ids is required")

    merged, failed = [], []
    for mid in members:
        try:
            await _merge_entities(t, keep, t, mid)
            merged.append(mid)
        except HTTPException as e:
            failed.append({"id": mid, "error": e.detail})
        except Exception as e:  # noqa: BLE001
            logger.exception("ocoi cluster merge: %s failed", mid)
            failed.append({"id": mid, "error": str(e)[:200]})

    # Close every proposal that touched the cluster, so the next scan and the
    # cluster view do not re-offer a decision the admin already made.
    ids = [keep] + merged
    if ids:
        await ocoi_db.execute("""
            UPDATE entity_match_proposals
               SET status = 'approved', reviewed_by_email = $2, reviewed_at = $3
             WHERE proposal_kind = 'duplicate' AND status = 'pending'
               AND entity_type = $1
               AND (entity_id = ANY($4::text[]) OR target_id = ANY($4::text[]))
        """, t, getattr(user, "email", None), _now(), ids)
    logger.info("ocoi admin: %s merged cluster of %s into %s (%s failed)",
                getattr(user, "email", "?"), len(merged), keep, len(failed))
    return _ok({"canonical_id": keep, "merged": merged, "failed": failed})


@router.get("/matches")
@limiter.limit("60/minute")
async def list_proposals(request: Request,
                         status: str | None = Query("pending"),
                         entity_type: str | None = Query(None),
                         min_score: float | None = Query(None, ge=0.0, le=1.0),
                         limit: int = Query(50, ge=1, le=200),
                         offset: int = Query(0, ge=0),
                         user: User = Depends(get_admin_user)):
    """Duplicate proposals with both sides hydrated. `status=all` disables the
    filter (OCOI silently ignored an unknown status instead — a filter that
    quietly does nothing is worse than one that refuses)."""
    _require_configured()
    where, args = ["proposal_kind = 'duplicate'"], []
    if status and status != "all":
        if status not in _PROPOSAL_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"status must be one of: {', '.join(_PROPOSAL_STATUSES)}, all")
        args.append(status)
        where.append(f"status = ${len(args)}")
    if entity_type:
        args.append(_etype(entity_type))
        where.append(f"entity_type = ${len(args)}")
    if min_score is not None:
        args.append(min_score)
        where.append(f"score >= ${len(args)}")
    w = " AND ".join(where)

    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM entity_match_proposals WHERE {w}", *args) or 0
    rows = _rows(await ocoi_db.fetch(f"""
        SELECT id, entity_type, entity_id, target_type, target_id, score,
               reasons, status, reviewed_by_email, reviewed_at, created_at
        FROM entity_match_proposals WHERE {w}
        ORDER BY score DESC, created_at DESC
        LIMIT ${len(args)+1} OFFSET ${len(args)+2}
    """, *args, limit, offset))

    # hydrate both sides, one query per entity type
    want: dict[str, set[str]] = {}
    for r in rows:
        want.setdefault(r["entity_type"], set()).add(r["entity_id"])
        want.setdefault(r["target_type"], set()).add(r["target_id"])
    info: dict[tuple, dict] = {}
    for t, ids in want.items():
        if t not in _ENTITY_TABLES:
            continue
        for e in await ocoi_db.fetch(
                f"SELECT id, name_hebrew, aliases FROM {_ENTITY_TABLES[t]} "
                f"WHERE id = ANY($1::text[])", list(ids)):
            info[(t, e["id"])] = {"id": e["id"], "type": t,
                                  "name": e["name_hebrew"] or "",
                                  "aliases": _aliases_of(e["aliases"])}
    for r in rows:
        r["reasons"] = ocoi_match._reasons(r.get("reasons"))
        r["score"] = float(r["score"]) if r["score"] is not None else None
        r["left"] = info.get((r["entity_type"], r["entity_id"]),
                             {"id": r["entity_id"], "type": r["entity_type"],
                              "name": "(נמחק)", "aliases": []})
        r["right"] = info.get((r["target_type"], r["target_id"]),
                              {"id": r["target_id"], "type": r["target_type"],
                               "name": "(נמחק)", "aliases": []})
    return _ok(rows, total=int(total), limit=limit, offset=offset)


class ReviewBody(BaseModel):
    action: str            # approve | reject | dismiss


@router.post("/matches/{proposal_id}/review")
@limiter.limit("60/minute")
async def review_proposal(request: Request, proposal_id: str, body: ReviewBody,
                          user: User = Depends(get_admin_user)):
    """Approve (merge), reject (not the same) or dismiss (revisit later).

    Approving merges `target_id` INTO `entity_id` — the same direction OCOI
    used, so a reviewer's mental model carries over. Every other pending
    proposal touching the row that disappears is dismissed in the same breath,
    or the cluster view would keep offering a decision about an id that is gone.
    """
    _require_configured()
    pid = _id(proposal_id, "proposal_id")
    if body.action not in ("approve", "reject", "dismiss"):
        raise HTTPException(status_code=400,
                            detail="action must be approve, reject or dismiss")
    p = await ocoi_db.fetchrow(
        "SELECT * FROM entity_match_proposals WHERE id = $1", pid)
    if p is None:
        raise HTTPException(status_code=404, detail="Proposal not found")
    if p["status"] != "pending":
        raise HTTPException(status_code=409,
                            detail=f"ההצעה כבר טופלה ({p['status']})")

    new_status = {"approve": "approved", "reject": "rejected",
                  "dismiss": "dismissed"}[body.action]
    # Stamp the decision FIRST. The merge below clears pending proposals that
    # reference the disappearing row, and this proposal is one of them — writing
    # the status afterwards updated nothing and lost the audit record.
    await ocoi_db.execute("""
        UPDATE entity_match_proposals
           SET status = $2, reviewed_at = $3, reviewed_by_email = $4
         WHERE id = $1""", pid, new_status, _now(), getattr(user, "email", None))

    result = {}
    if body.action == "approve":
        if p["proposal_kind"] != "duplicate":
            raise HTTPException(status_code=400,
                                detail="only duplicate proposals can be merged")
        result = await _merge_entities(p["entity_type"], p["entity_id"],
                                       p["target_type"], p["target_id"])
        await ocoi_db.execute("""
            UPDATE entity_match_proposals
               SET status = 'dismissed', reviewed_at = $3, reviewed_by_email = $4
             WHERE id <> $1 AND status = 'pending'
               AND ((entity_type = $5 AND entity_id = $2)
                 OR (target_type = $5 AND target_id = $2))
        """, pid, p["target_id"], _now(), getattr(user, "email", None),
            p["target_type"])

    logger.info("ocoi admin: %s %s proposal %s",
                getattr(user, "email", "?"), new_status, pid)
    return _ok({"id": pid, "status": new_status, **result})


class CleanupProposalsBody(BaseModel):
    entity_type: str
    reasons_any: list[str] | None = None
    dry_run: bool = True


@router.post("/matches/cleanup")
@limiter.limit("10/minute")
async def cleanup_proposals(request: Request, body: CleanupProposalsBody,
                            user: User = Depends(get_admin_user)):
    """Delete pending proposals matching a reason substring.

    A hard delete, so the pairs become re-proposable on the next scan — that is
    the point: it is for discarding a batch produced by a rule that turned out
    to be wrong, not for recording a review decision. `dry_run` defaults to true
    because OCOI's version deleted immediately.
    """
    _require_configured()
    t = _etype(body.entity_type)
    where = ["proposal_kind = 'duplicate'", "status = 'pending'", "entity_type = $1"]
    args: list = [t]
    if body.reasons_any:
        ors = []
        for rsn in body.reasons_any:
            if not rsn:
                continue
            args.append(f"%{rsn}%")
            ors.append(f"reasons ILIKE ${len(args)}")
        if ors:
            where.append("(" + " OR ".join(ors) + ")")
    w = " AND ".join(where)
    n = await ocoi_db.fetchval(
        f"SELECT count(*) FROM entity_match_proposals WHERE {w}", *args) or 0
    if not body.dry_run and n:
        await ocoi_db.execute(
            f"DELETE FROM entity_match_proposals WHERE {w}", *args)
    return _ok({"dry_run": body.dry_run, "matched": int(n)})


# ═══════════════════════════════════════════════════════════════════════════
# WAVE 4 — registry, ignore list, public suggestions, site content
# ═══════════════════════════════════════════════════════════════════════════

from app.services import ocoi_registry  # noqa: E402

_SUGGESTION_STATUSES = ("pending", "approved", "rejected")
# The keys the public site reads. An unknown key is refused rather than silently
# stored, so a typo cannot create a row nothing will ever render.
_CONTENT_KEYS = ("header_links", "footer_text", "about_content", "extraction_prompt")


# ── registry ──────────────────────────────────────────────────────────────────

@router.get("/registry/sources")
@limiter.limit("60/minute")
async def registry_sources(request: Request, user: User = Depends(get_admin_user)):
    """The five mirrored registries and how fresh each one is."""
    _require_configured()
    state = {r["source_type"]: dict(r) for r in await ocoi_db.fetch(
        "SELECT source_type, last_synced_at, record_count, sync_status, error_message "
        "FROM registry_sync_status")}
    held = {r["source_type"]: r["n"] for r in await ocoi_db.fetch(
        "SELECT source_type, count(*) AS n FROM registry_records GROUP BY source_type")}
    out = []
    for key, cfg in ocoi_registry.REGISTRY_SOURCES.items():
        s = state.get(key, {})
        out.append({
            "key": key, "label": cfg["label"], "entity_type": cfg["entity_type"],
            "rows_held": int(held.get(key, 0)),
            "last_synced_at": s.get("last_synced_at"),
            "sync_status": s.get("sync_status") or "never",
            "error_message": s.get("error_message"),
            # A source can be permanently unusable at the origin rather than
            # merely un-synced; the UI must be able to tell those apart.
            "enabled": cfg.get("enabled", True),
            "note": cfg.get("note"),
        })
    return _ok(out)


class RegistrySyncBody(BaseModel):
    sources: list[str] | None = None


@router.post("/registry/sync")
@limiter.limit("6/minute")
async def registry_sync(request: Request, body: RegistrySyncBody,
                        background: BackgroundTasks,
                        user: User = Depends(get_admin_user)):
    """Mirror one or more registries. Runs on OVER — data.gov.il is not the
    Cloudflare-blocked host; only odata.org.il is."""
    _require_configured()
    srcs = tuple(s for s in (body.sources or ocoi_registry._ENABLED)
                 if s in ocoi_registry.REGISTRY_SOURCES
                 and ocoi_registry.REGISTRY_SOURCES[s].get("enabled", True))
    if not srcs:
        raise HTTPException(
            status_code=400,
            detail=f"sources must be from: {', '.join(ocoi_registry._ENABLED)}")
    if not await ocoi_match.claim_job(ocoi_registry.JOB_SYNC):
        raise HTTPException(status_code=409, detail="סנכרון מאגרים כבר רץ")
    background.add_task(ocoi_registry.run_sync_all, srcs)
    logger.info("ocoi admin: %s started registry sync %s",
                getattr(user, "email", "?"), srcs)
    return _ok({"started": True, "sources": list(srcs)})


class RegistryMatchBody(BaseModel):
    limit: int | None = None


@router.post("/registry/match")
@limiter.limit("6/minute")
async def registry_match(request: Request, body: RegistryMatchBody,
                         background: BackgroundTasks,
                         user: User = Depends(get_admin_user)):
    """Attach registration numbers to companies/associations that lack one."""
    _require_configured()
    if not await ocoi_match.claim_job(ocoi_registry.JOB_MATCH):
        raise HTTPException(status_code=409, detail="התאמת מאגרים כבר רצה")
    background.add_task(ocoi_registry.run_match_all, body.limit)
    return _ok({"started": True, "limit": body.limit})


@router.get("/registry/records")
@limiter.limit("60/minute")
async def registry_records(request: Request,
                           q: str | None = Query(None),
                           source: str | None = Query(None),
                           registration_number: str | None = Query(None),
                           limit: int = Query(50, ge=1, le=200),
                           offset: int = Query(0, ge=0),
                           user: User = Depends(get_admin_user)):
    """Search the mirror. The count is BOUNDED — this table holds ~800k rows and
    an exact count over a substring scan is the 39-second query the public API
    already learned about."""
    _require_configured()
    where, args = ["1=1"], []
    if source:
        args.append(source)
        where.append(f"source_type = ${len(args)}")
    if registration_number:
        args.append(registration_number)
        where.append(f"registration_number = ${len(args)}")
    if q:
        args.append(f"%{q}%")
        where.append(f"name ILIKE ${len(args)}")
    w = " AND ".join(where)
    cap = 10_000
    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM (SELECT 1 FROM registry_records WHERE {w} "
        f"LIMIT {cap + 1}) t", *args) or 0
    rows = await ocoi_db.fetch(
        f"SELECT id, source_type, name, registration_number, status, updated_at "
        f"FROM registry_records WHERE {w} ORDER BY name "
        f"LIMIT ${len(args)+1} OFFSET ${len(args)+2}", *args, limit, offset)
    meta = {"total": min(int(total), cap), "limit": limit, "offset": offset}
    if int(total) > cap:
        meta["total_capped"] = True
    return _ok(_rows(rows), **meta)


# ── ignore list (discovery reads this) ────────────────────────────────────────

@router.get("/ignored")
@limiter.limit("60/minute")
async def list_ignored(request: Request, q: str | None = Query(None),
                       limit: int = Query(50, ge=1, le=200),
                       offset: int = Query(0, ge=0),
                       user: User = Depends(get_admin_user)):
    """URLs discovery will never offer again.

    Populated by hand AND automatically: a push whose bytes duplicate an
    existing document records its URL here, which is what stops the worker
    re-downloading the same declaration forever.
    """
    _require_configured()
    where, args = ["1=1"], []
    if q:
        args.append(f"%{q}%")
        where.append(f"(file_url ILIKE ${len(args)} OR coalesce(title,'') ILIKE ${len(args)})")
    w = " AND ".join(where)
    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM ignored_resources WHERE {w}", *args) or 0
    rows = await ocoi_db.fetch(
        f"SELECT id, file_url, title, source_type, created_at FROM ignored_resources "
        f"WHERE {w} ORDER BY created_at DESC "
        f"LIMIT ${len(args)+1} OFFSET ${len(args)+2}", *args, limit, offset)
    return _ok(_rows(rows), total=int(total), limit=limit, offset=offset)


class IgnoreBody(BaseModel):
    urls: list[str]
    title: str | None = None


@router.post("/ignored")
@limiter.limit("30/minute")
async def add_ignored(request: Request, body: IgnoreBody,
                      user: User = Depends(get_admin_user)):
    _require_configured()
    urls = [u.strip() for u in body.urls if u and u.strip()]
    if not urls:
        raise HTTPException(status_code=400, detail="urls is required")
    await ocoi_db.execute("""
        INSERT INTO ignored_resources (id, file_url, title, source_type, created_at)
        SELECT gen_random_uuid()::text, u, $2, 'manual',
               now() AT TIME ZONE 'Asia/Jerusalem'
          FROM unnest($1::text[]) AS u
        ON CONFLICT (file_url) DO NOTHING
    """, urls, (body.title or "")[:2000])
    n = await ocoi_db.fetchval(
        "SELECT count(*) FROM ignored_resources WHERE file_url = ANY($1::text[])", urls)
    return _ok({"requested": len(urls), "now_ignored": int(n or 0)})


@router.delete("/ignored")
@limiter.limit("30/minute")
async def remove_ignored(request: Request, body: IgnoreBody,
                         user: User = Depends(get_admin_user)):
    """Un-ignore, so discovery may offer these again."""
    _require_configured()
    urls = [u.strip() for u in body.urls if u and u.strip()]
    if not urls:
        raise HTTPException(status_code=400, detail="urls is required")
    before = await ocoi_db.fetchval(
        "SELECT count(*) FROM ignored_resources WHERE file_url = ANY($1::text[])", urls)
    await ocoi_db.execute(
        "DELETE FROM ignored_resources WHERE file_url = ANY($1::text[])", urls)
    return _ok({"removed": int(before or 0)})


# ── public suggestions ────────────────────────────────────────────────────────

@router.get("/suggestions")
@limiter.limit("60/minute")
async def list_suggestions(request: Request,
                           status: str | None = Query("pending"),
                           target_kind: str | None = Query(None),
                           limit: int = Query(50, ge=1, le=200),
                           offset: int = Query(0, ge=0),
                           user: User = Depends(get_admin_user)):
    """The public correction queue. Anyone can POST one from the site."""
    _require_configured()
    where, args = ["1=1"], []
    if status and status != "all":
        if status not in _SUGGESTION_STATUSES:
            raise HTTPException(
                status_code=400,
                detail=f"status must be one of: {', '.join(_SUGGESTION_STATUSES)}, all")
        args.append(status)
        where.append(f"status = ${len(args)}")
    if target_kind:
        args.append(target_kind)
        where.append(f"target_kind = ${len(args)}")
    w = " AND ".join(where)
    total = await ocoi_db.fetchval(
        f"SELECT count(*) FROM suggestions WHERE {w}", *args) or 0
    rows = await ocoi_db.fetch(
        f"SELECT id, target_kind, target_id, field_name, document_id, current_value,"
        f" proposed_value, comment, submitter_email, status, admin_notes,"
        f" resolved_at, created_at FROM suggestions WHERE {w} "
        f"ORDER BY created_at DESC LIMIT ${len(args)+1} OFFSET ${len(args)+2}",
        *args, limit, offset)
    return _ok(_rows(rows), total=int(total), limit=limit, offset=offset)


class SuggestionReviewBody(BaseModel):
    status: str
    admin_notes: str | None = None


@router.patch("/suggestions/{suggestion_id}")
@limiter.limit("60/minute")
async def review_suggestion(request: Request, suggestion_id: str,
                            body: SuggestionReviewBody,
                            user: User = Depends(get_admin_user)):
    """Record a decision on a public correction.

    Approving does NOT apply the change — it is a review flag, exactly as in
    OCOI. Applying it is a separate, deliberate edit through the entity or
    document endpoints, because a submitted "correction" is a claim, not a fact.
    """
    _require_configured()
    sid = _id(suggestion_id, "suggestion_id")
    if body.status not in _SUGGESTION_STATUSES:
        raise HTTPException(
            status_code=400,
            detail=f"status must be one of: {', '.join(_SUGGESTION_STATUSES)}")
    if not await ocoi_db.fetchval("SELECT 1 FROM suggestions WHERE id = $1", sid):
        raise HTTPException(status_code=404, detail="Suggestion not found")
    # resolved_at is decided in Python, not with a CASE on $2: `status` is
    # varchar and the literal 'pending' is text, so reusing the one parameter
    # for both makes Postgres deduce two types for it and refuse the statement.
    resolved = None if body.status == "pending" else _now()
    await ocoi_db.execute("""
        UPDATE suggestions
           SET status = $2, resolved_at = $3,
               admin_notes = COALESCE($4, admin_notes)
         WHERE id = $1""", sid, body.status, resolved,
        (body.admin_notes or None))
    return _ok({"id": sid, "status": body.status})


@router.delete("/suggestions/{suggestion_id}")
@limiter.limit("30/minute")
async def delete_suggestion(request: Request, suggestion_id: str,
                            user: User = Depends(get_admin_user)):
    _require_configured()
    sid = _id(suggestion_id, "suggestion_id")
    if not await ocoi_db.fetchval("SELECT 1 FROM suggestions WHERE id = $1", sid):
        raise HTTPException(status_code=404, detail="Suggestion not found")
    await ocoi_db.execute("DELETE FROM suggestions WHERE id = $1", sid)
    return _ok({"id": sid})


# ── site content (includes the extraction prompt) ─────────────────────────────

@router.get("/content/{key}")
@limiter.limit("60/minute")
async def get_content(request: Request, key: str,
                      user: User = Depends(get_admin_user)):
    _require_configured()
    if key not in _CONTENT_KEYS:
        raise HTTPException(status_code=404,
                            detail=f"unknown key: {', '.join(_CONTENT_KEYS)}")
    row = await ocoi_db.fetchrow(
        "SELECT key, value, updated_at FROM site_content WHERE key = $1", key)
    return _ok(dict(row) if row else {"key": key, "value": "", "updated_at": None})


class ContentBody(BaseModel):
    value: str = ""


@router.put("/content/{key}")
@limiter.limit("30/minute")
async def put_content(request: Request, key: str, body: ContentBody,
                      user: User = Depends(get_admin_user)):
    """Edit site copy, and the extraction prompt.

    The prompt lives here rather than on disk: OCOI kept it in
    `data/extraction_prompt.json` on an ephemeral filesystem, so every admin
    edit was silently reverted by the next deploy. The worker reads it from
    /api/worker/ocoi-config, so an edit here actually reaches extraction.
    """
    _require_configured()
    if key not in _CONTENT_KEYS:
        raise HTTPException(status_code=404,
                            detail=f"unknown key: {', '.join(_CONTENT_KEYS)}")
    await ocoi_db.execute("""
        INSERT INTO site_content (key, value, updated_at) VALUES ($1, $2, $3)
        ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = $3
    """, key, body.value, _now())
    logger.info("ocoi admin: %s updated content %s (%d chars)",
                getattr(user, "email", "?"), key, len(body.value))
    return _ok({"key": key, "length": len(body.value)})
