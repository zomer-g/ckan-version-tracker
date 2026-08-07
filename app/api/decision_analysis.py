"""The government-decision analysis page — read side, admin side, publish gate.

Public (rate-limited):
* ``GET /api/decision-analysis`` — the index of PUBLISHED analyses only, as
  ``[{"key","title","subtitle"}]``. The navbar and the rationale page use it to
  decide whether to show the link at all, so an unpublished draft leaves no
  trace in the UI.
* ``GET /api/decision-analysis/{key}`` — the document. 404 while unpublished:
  the gate is server-side, not a hidden route the curious can guess.

Admin (JWT + is_admin):
* ``GET    /api/admin/decision-analysis/{key}`` — the draft, always, plus
  ``published``. This is what the editor loads and what the public page falls
  back to so an admin can preview before publishing.
* ``PUT    /api/admin/decision-analysis/{key}`` — save the whole document and/or
  flip ``published``.
* ``DELETE /api/admin/decision-analysis/{key}`` — drop the edits and revert to
  the bundled default in app/data/decision_1933.py (keeps the row's published
  state; pass the flag explicitly via PUT to unpublish).

The document is stored as one JSONB blob rather than normalised tables: it is a
single hand-authored essay with ordered sections and nested tasks, edited by one
person in one form, so row-per-section would buy nothing but reordering pain.
``_validate_doc`` is therefore the only thing standing between the editor and
the public page — it enforces the shape the frontend renders (see
app/data/decision_1933.py for that shape) and rejects anything else with a 422
naming the offending path.
"""
import json
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_admin_user
from app.data.decision_1933 import DECISION_KEY, DEFAULT_DOC, TASK_STATUSES
from app.database import get_db
from app.models.decision_analysis import DecisionAnalysis
from app.models.user import User
from app.rate_limit import limiter

logger = logging.getLogger(__name__)

# Decisions that have a bundled default. A key outside this set is a 404 — the
# page is a hand-written analysis, not a generic renderer over any decision.
DEFAULT_DOCS: dict[str, dict] = {DECISION_KEY: DEFAULT_DOC}

# Top-level document strings, and the per-section / per-task string fields. Kept
# as explicit tuples so a typo'd key from a hand-edited payload fails loudly
# instead of silently vanishing from the page.
DOC_TEXT_FIELDS = (
    "title",
    "subtitle",
    "intro",
    "decision_number",
    "decision_date",
    "decision_url",
)
SECTION_FIELDS = ("id", "part", "label", "heading", "text")
TASK_TEXT_FIELDS = (
    "id",
    "title",
    "obligation",
    "responsible",
    "due",
    "potential",
    "actual",
    "damage",
)

# A document this large is a mistake, not an essay. Guards the JSONB column and
# the response size against a runaway paste.
MAX_DOC_CHARS = 400_000

router = APIRouter(prefix="/api/decision-analysis", tags=["decision-analysis"])
admin_router = APIRouter(
    prefix="/api/admin/decision-analysis", tags=["admin", "decision-analysis"]
)


class DecisionAnalysisUpsert(BaseModel):
    """Either half may be omitted: save content without touching visibility, or
    publish/unpublish without resending the document."""

    doc: dict | None = None
    published: bool | None = None


def _default_doc(key: str) -> dict:
    doc = DEFAULT_DOCS.get(key)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"unknown decision '{key}'")
    return doc


def _str_field(container: dict, name: str, where: str) -> str:
    value = container.get(name, "")
    if value is None:
        return ""
    if not isinstance(value, str):
        raise HTTPException(
            status_code=422, detail=f"{where}.{name} must be a string"
        )
    return value


def _validate_doc(doc: dict) -> dict:
    """Normalise and validate an incoming document; raise 422 on a bad shape.

    Returns a cleaned copy holding only known fields, so an editor bug can't
    smuggle extra keys into the stored blob.
    """
    if not isinstance(doc, dict):
        raise HTTPException(status_code=422, detail="doc must be an object")

    sections = doc.get("sections")
    if not isinstance(sections, list):
        raise HTTPException(status_code=422, detail="doc.sections must be a list")

    labels = doc.get("labels") or {}
    if not isinstance(labels, dict):
        raise HTTPException(status_code=422, detail="doc.labels must be an object")
    for label_key, label_value in labels.items():
        if not isinstance(label_value, str):
            raise HTTPException(
                status_code=422, detail=f"doc.labels.{label_key} must be a string"
            )

    clean: dict = {"key": _str_field(doc, "key", "doc")}
    for name in DOC_TEXT_FIELDS:
        clean[name] = _str_field(doc, name, "doc")
    clean["labels"] = dict(labels)

    seen_section_ids: set[str] = set()
    seen_task_ids: set[str] = set()
    clean_sections: list[dict] = []

    for s_idx, section in enumerate(sections):
        where = f"doc.sections[{s_idx}]"
        if not isinstance(section, dict):
            raise HTTPException(status_code=422, detail=f"{where} must be an object")

        clean_section = {name: _str_field(section, name, where) for name in SECTION_FIELDS}
        section_id = clean_section["id"].strip()
        if not section_id:
            raise HTTPException(status_code=422, detail=f"{where}.id is required")
        # Ids drive React keys and the in-page anchors; duplicates would make
        # two sections fight over one anchor.
        if section_id in seen_section_ids:
            raise HTTPException(
                status_code=422, detail=f"{where}.id '{section_id}' is duplicated"
            )
        seen_section_ids.add(section_id)
        clean_section["id"] = section_id

        tasks = section.get("tasks") or []
        if not isinstance(tasks, list):
            raise HTTPException(status_code=422, detail=f"{where}.tasks must be a list")

        clean_tasks: list[dict] = []
        for t_idx, task in enumerate(tasks):
            t_where = f"{where}.tasks[{t_idx}]"
            if not isinstance(task, dict):
                raise HTTPException(
                    status_code=422, detail=f"{t_where} must be an object"
                )
            clean_task = {
                name: _str_field(task, name, t_where) for name in TASK_TEXT_FIELDS
            }
            task_id = clean_task["id"].strip()
            if not task_id:
                raise HTTPException(status_code=422, detail=f"{t_where}.id is required")
            if task_id in seen_task_ids:
                raise HTTPException(
                    status_code=422, detail=f"{t_where}.id '{task_id}' is duplicated"
                )
            seen_task_ids.add(task_id)
            clean_task["id"] = task_id

            status = task.get("status") or "unknown"
            if status not in TASK_STATUSES:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        f"{t_where}.status must be one of {', '.join(TASK_STATUSES)}"
                    ),
                )
            clean_task["status"] = status
            clean_tasks.append(clean_task)

        clean_section["tasks"] = clean_tasks
        clean_sections.append(clean_section)

    clean["sections"] = clean_sections

    size = len(json.dumps(clean, ensure_ascii=False))
    if size > MAX_DOC_CHARS:
        raise HTTPException(
            status_code=422,
            detail=f"document is too large ({size} chars, limit {MAX_DOC_CHARS})",
        )
    return clean


async def _load_row(db: AsyncSession, key: str) -> DecisionAnalysis | None:
    return (
        await db.execute(select(DecisionAnalysis).where(DecisionAnalysis.key == key))
    ).scalar_one_or_none()


@router.get("")
@limiter.limit("120/minute")
async def list_published(
    request: Request, db: AsyncSession = Depends(get_db)
) -> list[dict]:
    """Published analyses only — the navbar's cue for whether to show a link."""
    rows = (
        await db.execute(
            select(DecisionAnalysis.key, DecisionAnalysis.doc).where(
                DecisionAnalysis.published.is_(True)
            )
        )
    ).all()
    out: list[dict] = []
    for key, doc in rows:
        if key not in DEFAULT_DOCS:
            continue
        effective = doc or DEFAULT_DOCS[key]
        out.append(
            {
                "key": key,
                "title": effective.get("title", ""),
                "subtitle": effective.get("subtitle", ""),
            }
        )
    return out


@router.get("/{key}")
@limiter.limit("120/minute")
async def get_public(
    request: Request, key: str, db: AsyncSession = Depends(get_db)
) -> dict:
    """The document — only once published. Unpublished is a 404, not a 403, so
    the existence of a draft isn't leaked either."""
    default = _default_doc(key)
    row = await _load_row(db, key)
    if row is None or not row.published:
        raise HTTPException(status_code=404, detail=f"unknown decision '{key}'")
    return {"key": key, "published": True, "doc": row.doc or default}


@admin_router.get("/{key}")
async def get_admin(
    key: str,
    _admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """The draft as the editor sees it — bundled default until someone saves."""
    default = _default_doc(key)
    row = await _load_row(db, key)
    return {
        "key": key,
        "published": bool(row and row.published),
        "is_customized": bool(row and row.doc is not None),
        "updated_by": row.updated_by if row else None,
        "updated_at": row.updated_at.isoformat() if row and row.updated_at else None,
        "doc": (row.doc if row and row.doc is not None else default),
    }


@admin_router.put("/{key}")
async def upsert_admin(
    key: str,
    body: DecisionAnalysisUpsert,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Save the document and/or change its published state."""
    _default_doc(key)
    if body.doc is None and body.published is None:
        raise HTTPException(status_code=422, detail="nothing to update")

    clean = _validate_doc(body.doc) if body.doc is not None else None

    # Plain read-then-write rather than a dialect-specific upsert: this is one
    # row edited from one admin form, so last-write-wins is the behaviour either
    # way, and the ORM path is what the tests can actually drive.
    row = await _load_row(db, key)
    if row is None:
        # A brand-new row starts hidden unless this very call publishes it.
        row = DecisionAnalysis(key=key, published=False)
        db.add(row)
    if clean is not None:
        row.doc = clean
    if body.published is not None:
        row.published = body.published
    row.updated_by = admin.email
    row.updated_at = datetime.now(timezone.utc)
    await db.commit()
    logger.info(
        "decision_analysis upsert %s by %s (doc=%s, published=%s)",
        key,
        admin.email,
        body.doc is not None,
        body.published,
    )
    return {"ok": True}


@admin_router.delete("/{key}")
async def revert_admin(
    key: str,
    admin: User = Depends(get_admin_user),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Drop the stored edits; the page reverts to the bundled default."""
    _default_doc(key)
    row = await _load_row(db, key)
    if row is not None:
        row.doc = None
        row.updated_by = admin.email
        row.updated_at = datetime.now(timezone.utc)
        await db.commit()
    return {"ok": True}
