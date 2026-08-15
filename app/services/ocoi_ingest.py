"""Ingestion for ניגוד עניינים לעם (OCOI) — the server half of the worker pipeline.

OCOI's declarations are PDFs on odata.org.il, and odata **Cloudflare-blocks
Render's datacenter IP** (the wall that broke ocal's auto-import for weeks). The
CKAN *metadata* API answers fine from Render; only file downloads 403. So the
work is split exactly as it is for ocal:

    OVER   discovers candidates (metadata only — allowed)
    worker downloads the PDF from a residential IP, converts + OCRs it, runs the
           LLM extraction, and pushes the RESULT back
    OVER   stores the rows, and the file bytes in R2

That split is not an invention: OCOI already shipped `POST /api/v1/push/documents`
plus a `tools/local_processor` that did precisely this, because its own 512MB
dyno could not run the heavy conversion either. This module is that contract
re-homed onto OVER's worker auth and storage.

Conversion and extraction deliberately do NOT happen here. They need poppler +
tesseract (system binaries OVER cannot install) and an LLM call per document;
the worker has both.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from datetime import datetime, timezone

import httpx

from app.services import ocoi_db

logger = logging.getLogger(__name__)

CKAN_BASE = "https://www.odata.org.il"
CKAN_QUERY = "ניגוד עניינים"
# odata rejects nothing on metadata, but a UA is basic manners and OCOI sent
# none at all (every request went out as `python-httpx/x.y`).
_UA = "OVER/1.0 (+https://www.over.org.il) ocoi-ingest"

PDF_FORMATS = {"pdf", "PDF", "application/pdf"}

_CONTENT_TYPES = {
    "pdf": "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "csv": "text/csv",
}


class SkipDocument(Exception):
    """The document is a duplicate or otherwise not worth storing."""


# ── discovery (metadata only — safe from Render) ────────────────────────────

async def discover_candidates(limit: int = 25) -> list[dict]:
    """CKAN resources matching the COI query that we have not imported yet.

    Excludes anything already in `documents` (by file_url) or blacklisted in
    `ignored_resources`, so the list converges instead of re-offering the same
    files forever — the lesson from ocal, where an unrecordable failure made one
    resource a permanent poison pill.
    """
    known: set[str] = set()
    rows = await ocoi_db.fetch("SELECT file_url FROM documents")
    known.update(r["file_url"] for r in rows if r["file_url"])
    rows = await ocoi_db.fetch("SELECT file_url FROM ignored_resources")
    known.update(r["file_url"] for r in rows if r["file_url"])

    out: list[dict] = []
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0), follow_redirects=True,
        headers={"User-Agent": _UA},
    ) as client:
        start = 0
        while len(out) < limit and start < 400:
            r = await client.get(
                f"{CKAN_BASE}/api/3/action/package_search",
                params={"q": CKAN_QUERY, "rows": 50, "start": start},
            )
            r.raise_for_status()
            results = ((r.json() or {}).get("result") or {}).get("results") or []
            if not results:
                break
            for pkg in results:
                for res in pkg.get("resources") or []:
                    fmt = (res.get("format") or "").strip()
                    url = res.get("url")
                    if not url or url in known:
                        continue
                    if fmt not in PDF_FORMATS and not str(url).lower().endswith(".pdf"):
                        continue
                    known.add(url)
                    out.append({
                        "file_url": url,
                        "title": res.get("name") or pkg.get("title") or "",
                        "file_format": (fmt or "pdf").lower(),
                        "source_type": "ckan",
                        "source_id": pkg.get("id") or "",
                        "source_title": pkg.get("title") or "",
                        "source_url": f"{CKAN_BASE}/dataset/{pkg.get('name') or pkg.get('id')}",
                    })
                    if len(out) >= limit:
                        break
                if len(out) >= limit:
                    break
            start += 50
    return out


# ── entity upserts ──────────────────────────────────────────────────────────

_ENTITY_TABLES = {
    "person": "persons",
    "company": "companies",
    "association": "associations",
    "domain": "domains",
}


async def _upsert_entity(etype: str, name: str, extra: dict) -> str | None:
    """Find-or-create an entity by its Hebrew name. Returns the id.

    Matching is on the exact name, as OCOI's own upserts were: fuzzy merging is
    a separate, human-reviewed step (entity_match_proposals), and doing it here
    would silently fuse two different officials who share a name.
    """
    name = (name or "").strip()
    if not name or etype not in _ENTITY_TABLES:
        return None
    table = _ENTITY_TABLES[etype]
    found = await ocoi_db.fetchval(
        f"SELECT id FROM {table} WHERE name_hebrew = $1 LIMIT 1", name)
    if found:
        return found
    new_id = str(uuid.uuid4())
    cols = ["id", "name_hebrew"]
    vals: list = [new_id, name]
    for k in ("name_english", "title", "position", "ministry",
              "company_type", "registration_number", "description"):
        v = extra.get(k)
        if v and _has_column(etype, k):
            cols.append(k)
            vals.append(str(v)[:500])
    ph = ", ".join(f"${i+1}" for i in range(len(vals)))
    await ocoi_db.execute(
        f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({ph}) "
        f"ON CONFLICT DO NOTHING", *vals)
    return await ocoi_db.fetchval(
        f"SELECT id FROM {table} WHERE name_hebrew = $1 LIMIT 1", name) or new_id


_COLUMNS = {
    "person": {"name_english", "title", "position", "ministry"},
    "company": {"name_english", "company_type", "registration_number"},
    "association": {"name_english", "registration_number"},
    "domain": {"name_english", "description"},
}


def _has_column(etype: str, col: str) -> bool:
    return col in _COLUMNS.get(etype, set())


# ── the push ────────────────────────────────────────────────────────────────

def _now() -> datetime:
    # Naive Asia/Jerusalem, matching the rest of the corpus (see ocoi_db).
    return datetime.now(timezone.utc).astimezone().replace(tzinfo=None)


async def _ignore_url(file_url: str, title: str | None, source_type: str) -> None:
    """Blacklist a URL so discovery stops offering it.

    Best-effort: failing to record an exclusion must never turn into failing the
    push that discovered it.
    """
    try:
        await ocoi_db.execute(
            "INSERT INTO ignored_resources (id, file_url, title, source_type, created_at) "
            "VALUES ($1,$2,$3,$4,$5) ON CONFLICT (file_url) DO NOTHING",
            str(uuid.uuid4()), file_url, (title or "")[:2000], source_type, _now())
    except Exception:  # noqa: BLE001
        logger.warning("ocoi: could not record ignored url %s", file_url, exc_info=True)


async def push_document(item: dict, pdf_bytes: bytes | None = None) -> dict:
    """Store one worker-processed document: rows here, file bytes in R2.

    Idempotent on file_url and content_hash, so a worker that retries after a
    network failure cannot create a second copy.
    """
    file_url = (item.get("file_url") or "").strip()
    if not file_url:
        raise ValueError("file_url is required")

    dup = await ocoi_db.fetchval(
        "SELECT id FROM documents WHERE file_url = $1 LIMIT 1", file_url)
    if dup:
        raise SkipDocument(f"duplicate file_url ({dup})")

    content_hash = item.get("content_hash")
    if not content_hash and pdf_bytes:
        content_hash = hashlib.sha256(pdf_bytes).hexdigest()
    if content_hash:
        dup = await ocoi_db.fetchval(
            "SELECT id FROM documents WHERE content_hash = $1 LIMIT 1", content_hash)
        if dup:
            # odata republishes the same declaration under several resource
            # URLs, so a URL we have never seen can carry bytes we already hold.
            # Record the URL as ignored, or discovery offers it on every single
            # pass and the worker re-downloads a file it can never store — the
            # exact loop that made one ocal resource a permanent poison pill.
            await _ignore_url(file_url, item.get("title"),
                              item.get("source_type") or "ckan")
            raise SkipDocument(f"duplicate content_hash ({dup}) — url recorded as ignored")

    # ── source ──
    source_type = item.get("source_type") or "ckan"
    source_id = item.get("source_id") or file_url
    src = await ocoi_db.fetchval(
        "SELECT id FROM sources WHERE source_type = $1 AND source_id = $2 LIMIT 1",
        source_type, source_id)
    if not src:
        src = str(uuid.uuid4())
        await ocoi_db.execute(
            "INSERT INTO sources (id, source_type, source_id, title, url, created_at) "
            "VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING",
            src, source_type, source_id,
            item.get("source_title") or item.get("title") or "",
            item.get("source_url") or file_url, _now())
        src = await ocoi_db.fetchval(
            "SELECT id FROM sources WHERE source_type = $1 AND source_id = $2 LIMIT 1",
            source_type, source_id) or src

    # ── the file goes to R2, never into the append DB ──
    doc_id = str(uuid.uuid4())
    fmt = (item.get("file_format") or "pdf").lower()
    r2_key = None
    if pdf_bytes:
        from app.services.storage_client import storage_client
        key = f"ocoi/documents/{doc_id}.{fmt}"
        try:
            # Same prefix and naming the migration used, so the corpus stays one
            # shape whether a file arrived by migration or by worker.
            if await storage_client.upload_object(
                key, file_content=pdf_bytes,
                content_type=_CONTENT_TYPES.get(fmt, "application/octet-stream"),
            ):
                r2_key = key
        except Exception as e:  # noqa: BLE001 — the rows are worth keeping regardless
            logger.warning("ocoi push: R2 upload failed for %s (%s) — row kept, "
                           "file not stored", file_url, e)

    markdown = item.get("markdown_content")
    await ocoi_db.execute("""
        INSERT INTO documents
            (id, source_id, title, file_format, file_url, file_size, content_hash,
             markdown_content, conversion_status, extraction_status, verified,
             created_at, converted_at, pdf_r2_key)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',false,$10,$11,$12)
    """, doc_id, src, (item.get("title") or "")[:2000], fmt, file_url,
        item.get("file_size"), content_hash, markdown,
        "converted" if markdown else "no_text",
        _now(), _now() if markdown else None, r2_key)

    # ── extraction ──
    extracted = 0
    extraction = item.get("extraction_json")
    if extraction:
        try:
            extracted = await _store_extraction(doc_id, extraction)
            await ocoi_db.execute(
                "UPDATE documents SET extraction_status='completed', extracted_at=$2 "
                "WHERE id=$1", doc_id, _now())
        except Exception as e:  # noqa: BLE001 — the document is still worth keeping
            logger.exception("ocoi push: extraction failed for %s", file_url)
            await ocoi_db.execute(
                "UPDATE documents SET extraction_status='failed' WHERE id=$1", doc_id)
            raise RuntimeError(f"stored the document but extraction failed: {e}") from e

    return {"document_id": doc_id, "relationships": extracted,
            "stored_file": bool(r2_key)}


async def _store_extraction(doc_id: str, extraction: dict) -> int:
    """Persist the worker's LLM output as entities + relationships.

    Tolerant of shape: the worker sends whatever the model produced, and a model
    that returns `{"results": {...}}` or a bare list must not lose a document's
    worth of work over an envelope.
    """
    if isinstance(extraction, str):
        extraction = json.loads(extraction)
    if isinstance(extraction, dict) and "results" in extraction:
        extraction = extraction["results"]
    if not isinstance(extraction, dict):
        return 0

    id_map: dict[tuple[str, str], str] = {}
    for etype, key in (("person", "persons"), ("company", "companies"),
                       ("association", "associations"), ("domain", "domains")):
        for ent in extraction.get(key) or []:
            if not isinstance(ent, dict):
                continue
            name = ent.get("name_hebrew") or ent.get("name")
            eid = await _upsert_entity(etype, name, ent)
            if eid:
                id_map[(etype, (name or "").strip())] = eid

    saved = 0
    for rel in extraction.get("relationships") or []:
        if not isinstance(rel, dict):
            continue
        st = (rel.get("source_type") or "").strip()
        tt = (rel.get("target_type") or "").strip()
        sid = id_map.get((st, (rel.get("source_name") or "").strip()))
        tid = id_map.get((tt, (rel.get("target_name") or "").strip()))
        if not sid or not tid:
            continue
        try:
            conf = float(rel.get("confidence") or 0.5)
        except (TypeError, ValueError):
            conf = 0.5
        # ix_rel_compound makes this idempotent — a re-pushed document cannot
        # duplicate its own edges.
        await ocoi_db.execute("""
            INSERT INTO entity_relationships
                (id, source_entity_type, source_entity_id, target_entity_type,
                 target_entity_id, relationship_type, details, restriction_type,
                 document_id, confidence, origin_kind, verified, created_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'coi_declaration',false,$11)
            ON CONFLICT DO NOTHING
        """, str(uuid.uuid4()), st, sid, tt, tid,
            (rel.get("relationship_type") or "related")[:50],
            rel.get("details"), rel.get("restriction_type"),
            doc_id, conf, _now())
        saved += 1

    n_ent = sum(len(extraction.get(k) or [])
                for k in ("persons", "companies", "associations", "domains"))
    await ocoi_db.execute("""
        INSERT INTO extraction_runs
            (id, document_id, extractor_type, model_version, entities_found,
             relationships_found, raw_output_json, created_at)
        VALUES ($1,$2,'llm',$3,$4,$5,$6,$7)
    """, str(uuid.uuid4()), doc_id,
        (extraction.get("_model") or "deepseek-chat")[:100],
        n_ent, saved, json.dumps(extraction, ensure_ascii=False), _now())
    return saved
