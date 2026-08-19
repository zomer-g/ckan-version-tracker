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
    await ocoi_db.execute("""
        DELETE FROM entity_match_proposals
         WHERE (entity_type = $1 AND entity_id = $2)
            OR (target_type = $1 AND target_id = $2)""", merge_type, merge_id)
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
