"""Free-text query API — Hebrew question → validated SQL → results.

  POST /api/nl/query     question → {sql, explanation, result}
  GET  /api/nl/examples  question suggestions derived from the live model

Public and read-only. The SQL this returns was compiled by the server from a
validated query object (app/services/semantic_model.py), never written by a
language model, and it is executed through the SAME read-only path as the
console (append_store.run_readonly_sql: least-privilege role, READ ONLY tx,
statement_timeout, hard row cap).

The response always carries the generated SQL, whether or not it ran. That is a
product requirement, not a debugging affordance: on a transparency site an
answer the reader cannot audit is worth less than no answer, and showing the
query is what lets someone check that "כמה" counted what they meant.

NOTE: no ``from __future__ import annotations`` — with the slowapi
``@limiter.limit`` wrapper it stringifies the endpoint hints and FastAPI then
mis-reads ``body: QueryRequest`` as a query param (422). Same trap as cbs_ask.
"""
import logging
import re
import time

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.rate_limit import limiter
from app.services import append_store, data_catalog, nl_query, semantic_model
from app.services.llm_budget import record_llm_tokens, reserve_llm_call
from app.services.semantic_model import SemanticError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/nl", tags=["nl"])

MAX_QUESTION_CHARS = 400


class QueryRequest(BaseModel):
    q: str
    run: bool = True


def _require_enabled() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")


_HEB_RE = re.compile(r"[֐-׿]")
# Catalog titles are often "<source> — <resource>", and the resource half
# usually repeats the source ("תיקופי מסלקה — מאגר תיקופי מסלקה 2021"). For a
# suggestion the shorter half is the readable one.
_DASH_SPLIT = re.compile(r"\s+[—–-]\s+")


def _clean_title(title: str) -> str:
    parts = [p.strip() for p in _DASH_SPLIT.split(title) if p.strip()]
    if len(parts) > 1:
        # Prefer the half that is not a restatement of the other.
        parts.sort(key=len)
        head, tail = parts[0], parts[-1]
        if head and head[:12] in tail:
            return head
    return (parts[0] if parts else title).strip()


def _family_key(title: str) -> str:
    """Group titles that differ only by a year or a serial, so a source with one
    table per year contributes one suggestion rather than four."""
    return re.sub(r"\d+", "", title).strip()[:24]


def _dim_label(dim: dict) -> str:
    """A short label for the "לפי X" half.

    The profiler's LLM description is a SENTENCE ("תיאור סוג הפריט (למשל 'הצעת
    חוק')") — right for the prompt, wrong for a UI label, where it turns the
    suggestion into a paragraph. Fall back to the column name when the
    description is long or parenthesised."""
    title = (dim.get("title") or "").strip()
    if title and len(title) <= 22 and "(" not in title and "," not in title:
        return title
    return dim["key"]


def _candidates(cands: list) -> list[dict]:
    """The nearby datasets attached to a refusal.

    A refusal that names what IS available turns a dead end into navigation —
    the semantic layer's known failure mode is "out of scope", so this is the
    path users will actually hit, and it has to be useful."""
    return [{"table": c["key"], "title": c["title"], "rows": c.get("rows"),
             "page_url": c.get("page_url") or ""} for c in cands[:5]]


@router.post("/query")
@limiter.limit("20/minute")
async def query(request: Request, body: QueryRequest, db: AsyncSession = Depends(get_db)):
    """Hebrew question → compiled SQL (+ results unless ``run`` is false)."""
    _require_enabled()
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")
    if len(q) > MAX_QUESTION_CHARS:
        # Bounds the input side of the prompt. A 400-char question is already
        # far longer than anything the model helps with.
        raise HTTPException(
            status_code=400,
            detail=f"השאלה ארוכה מדי (עד {MAX_QUESTION_CHARS} תווים)")

    cfg = await nl_query.get_config(db)
    started = time.monotonic()

    async def _reserve() -> bool:
        # Admin overrides from nl_query_config take precedence over config.py,
        # so a budget can be tightened live during an incident.
        return await reserve_llm_call(
            db,
            call_budget=cfg.get("daily_call_budget"),
            token_budget_override=cfg.get("daily_output_token_budget"),
        )

    async def _usage(i: int, o: int) -> None:
        await record_llm_tokens(db, i, o)

    def _ms() -> int:
        return int((time.monotonic() - started) * 1000)

    try:
        res = await nl_query.answer(db, q, reserve=_reserve, on_usage=_usage)
    except nl_query.OutOfScope as e:
        # 200, not 4xx: "אין לי את הנתון" is a real answer the UI renders, and
        # the browser client treats non-2xx as an error banner. The shape is
        # distinguishable by ``answered: false``.
        #
        # Logged as carefully as a success: a refusal can have run two paid
        # tiers before concluding it could not answer, and an admin looking at
        # the bill needs to see that.
        await nl_query.log_query(
            db, question=q, answered=False, stage="refused",
            attempts=">".join(e.attempts) or None, reason=e.message,
            input_tokens=e.usage[0], output_tokens=e.usage[1], duration_ms=_ms())
        return {"answered": False, "reason": e.message, "candidates": _candidates(e.candidates)}
    except SemanticError as e:
        # The model produced a query naming something that is not in the model.
        # Surfaced as a refusal for the same reason — it is one.
        logger.info("nl/query rejected an invalid model output for %r: %s", q, e)
        # The tiers that ran are attached by answer() — an unusable output was
        # still paid for, and a log row that omits its cost makes the most
        # wasteful failure mode look like the cheapest.
        usage = getattr(e, "usage", (0, 0))
        await nl_query.log_query(
            db, question=q, answered=False, stage="invalid",
            attempts=">".join(getattr(e, "attempts", []) or []) or None,
            reason=str(e), input_tokens=usage[0], output_tokens=usage[1],
            duration_ms=_ms())
        return {"answered": False, "reason": f"לא הצלחתי לבנות שאילתה תקינה: {e}",
                "candidates": []}
    except HTTPException:
        raise
    except Exception as e:  # noqa: BLE001 — provider/network failures
        logger.warning("nl/query failed for %r: %s", q, e)
        await nl_query.log_query(db, question=q, answered=False, stage="error",
                                 reason=f"{type(e).__name__}: {e}", duration_ms=_ms())
        raise HTTPException(status_code=502, detail="שגיאה בשירות המענה בשפה חופשית")

    out = {
        "answered": True,
        "sql": res["sql"],
        "query": res["query"],
        "entity": res["entity"],
        "explanation": res["explanation"],
        # "template" / "deepseek" / "anthropic" — the UI labels a model-derived
        # answer differently from a deterministic one, because the confidence
        # genuinely differs and the reader deserves to know which they got.
        "source": res["source"],
        "model": res.get("model"),
        # True when the cheap tier was tried first and could not answer. Exposed
        # so escalation rate is observable from the outside rather than only
        # visible on the invoice.
        "escalated": bool(res.get("escalated")),
        "cached": bool(res.get("cached")),
    }
    usage = res.get("usage") or (0, 0)
    await nl_query.log_query(
        db, question=q, answered=True, stage=res.get("stage") or res["source"],
        attempts=">".join(res.get("attempts") or []) or None,
        model=res.get("model"), escalated=bool(res.get("escalated")),
        entity=res["entity"], sql=res["sql"],
        input_tokens=usage[0], output_tokens=usage[1], duration_ms=_ms())
    if not body.run:
        return out

    try:
        out["result"] = await append_store.run_readonly_sql(
            res["sql"], search_path=data_catalog.CONSOLE_SEARCH_PATH)
    except ValueError as e:
        # Compiled SQL that the DB rejects is a compiler bug, not user error —
        # log it loudly, but still hand back the SQL so the user can fix and run
        # it in the console rather than losing the work.
        logger.error("nl/query compiled invalid SQL for %r: %s\n%s", q, e, res["sql"])
        out["error"] = str(e)
    return out


# ── the guided explorer (no model, no cost) ──────────────────────────────────
# The autopilot — question straight to an answer — was measured at 87% right
# dataset but only 56% correct refusal, i.e. nearly half of out-of-scope
# questions were routed to a plausible but wrong dataset. These two endpoints
# back the replacement: the same retrieval, used to SUGGEST rather than to
# decide. Same catalog knowledge, requirement dropped from "be right" to
# "include the right one" (94% at top-5), and the person makes the call.
#
# No language model, no budget, no daily quota. Everything here is lexical
# scoring over a cached model plus one deterministic join rule.

class SuggestRequest(BaseModel):
    q: str
    limit: int = 8


@router.post("/suggest")
@limiter.limit("60/minute")
async def suggest(request: Request, body: SuggestRequest, db: AsyncSession = Depends(get_db)):
    """Free text → a shortlist of datasets, each with why it matched."""
    _require_enabled()
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")
    if len(q) > MAX_QUESTION_CHARS:
        raise HTTPException(status_code=400,
                            detail=f"הטקסט ארוך מדי (עד {MAX_QUESTION_CHARS} תווים)")

    model = await semantic_model.build_model(db)
    hits = semantic_model.suggest(model, q, k=max(1, min(body.limit, 20)))

    # Best-effort click-through logging: every search is a labelled example
    # waiting for its pick. A logging failure must never fail the search.
    suggest_id = None
    try:
        from sqlalchemy import text as _sqltext
        suggest_id = (await db.execute(_sqltext(
            "INSERT INTO nl_suggest_log "
            "(query, suggestions_count, approximate_count, top_table) "
            "VALUES (:q, :n, :a, :top) RETURNING id"),
            {"q": q, "n": len(hits),
             "a": sum(1 for h in hits if h.get("approximate")),
             "top": hits[0]["entity"]["key"] if hits else None})).scalar_one()
        await db.commit()
    except Exception:  # noqa: BLE001 — pre-migration or a DB blip
        logger.debug("nl/suggest: log write failed", exc_info=True)
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass

    def _why(h: dict) -> str:
        # A prefix-fallback hit is a GUESS, and its reason must say so — the
        # generic wording ("בשם המאגר: שמאות") reads as an exact name match,
        # which is precisely the overclaim the fallback must not make.
        if h.get("approximate"):
            words = ", ".join(h["matched"].get("title") or [])
            return f"דמיון בכתיב בלבד ({words}) — ייתכן שאינו קשור"
        return semantic_model.match_reason(h["matched"])

    return {
        "query": q,
        "suggest_id": suggest_id,
        "total_entities": len(model),
        "suggestions": [
            {
                "table": h["entity"]["key"],
                "schema": h["entity"]["schema"],
                "title": h["entity"]["title"],
                "summary": (h["entity"].get("summary") or "")[:400],
                "rows": h["entity"].get("rows"),
                "score": h["score"],
                "matched": h["matched"],
                "why": _why(h),
                # Found by spelling similarity, not by a real token match. The
                # UI badges these; dropping the flag (the launch bug — caught in
                # live verification, missed by a vacuous test) showed guesses as
                # confident matches.
                "approximate": bool(h.get("approximate")),
                # Whether step 4 (cross with another dataset) is available at all
                # for this one — shown up front so the path is discoverable
                # before the user has invested in choosing.
                "can_join": bool(h["entity"].get("geo_dims")),
                "page_url": h["entity"].get("page_url") or "",
                "source_url": h["entity"].get("source_url") or "",
            }
            for h in hits
        ],
    }


@router.get("/joinable/{table}")
@limiter.limit("60/minute")
async def joinable(table: str, request: Request, q: str = "",
                   db: AsyncSession = Depends(get_db)):
    """Datasets that can be crossed with ``table``, optionally filtered by text.

    Deterministic: both sides must carry a locality or authority column, because
    the canonical settlement code is the only join key in this corpus. ``q``
    narrows the list by the same scorer used for suggestions, so a user who
    knows what they want does not scroll."""
    _require_enabled()
    model = await semantic_model.build_model(db)
    rows = semantic_model.joinable_with(model, table)
    if not rows:
        return {"table": table, "joinable": [], "reason":
                "למאגר הזה אין עמודת יישוב או רשות, ולכן אין לפי מה להצליב אותו."}
    if q.strip():
        ranked = {h["entity"]["key"]: h["score"]
                  for h in semantic_model.suggest(model, q, k=60)}
        rows = [r for r in rows if r["entity"]["key"] in ranked]
        rows.sort(key=lambda r: -ranked[r["entity"]["key"]])
    return {
        "table": table,
        "joinable": [
            {"table": r["entity"]["key"], "schema": r["entity"]["schema"],
             "title": r["entity"]["title"], "rows": r["entity"].get("rows"),
             "via": r["via"]}
            for r in rows[:40]
        ],
    }


class PickRequest(BaseModel):
    suggest_id: int
    table: str
    rank: int
    approximate: bool = False


@router.post("/picked")
@limiter.limit("60/minute")
async def picked(request: Request, body: PickRequest, db: AsyncSession = Depends(get_db)):
    """Record which suggestion the user chose, and where it sat on the list.

    This is the ground truth the benchmark cannot write for itself: the person
    who typed the description saying which dataset they meant. From it the
    admin panel derives recall-in-the-wild and synonym candidates. UPDATE-only
    against a row /suggest created — an id that does not exist writes nothing,
    so the endpoint cannot be used to inject free text."""
    from sqlalchemy import text as _sqltext
    try:
        await db.execute(_sqltext(
            "UPDATE nl_suggest_log SET picked_table = :t, picked_rank = :r, "
            "picked_approximate = :a, picked_at = now() "
            "WHERE id = :id AND picked_table IS NULL"),
            {"t": body.table[:255], "r": max(1, min(body.rank, 50)),
             "a": body.approximate, "id": body.suggest_id})
        await db.commit()
    except Exception:  # noqa: BLE001
        logger.debug("nl/picked: write failed", exc_info=True)
        try:
            await db.rollback()
        except Exception:  # noqa: BLE001
            pass
    return {"ok": True}


class CrossRequest(BaseModel):
    left: str
    right: str


@router.post("/cross")
@limiter.limit("30/minute")
async def cross(request: Request, body: CrossRequest, db: AsyncSession = Depends(get_db)):
    """Two dataset keys → the fan-trap-safe cross SQL, ready for the console.

    This closes the explorer's step 4. The compiler existed and was tested from
    the day joins shipped, but nothing invoked it — clicking a join candidate
    only navigated to the other dataset, silently dropping the first one.

    Deterministic end to end: the only join key is the canonical settlement
    code, each side is aggregated to it BEFORE the join (a row-level join on a
    shared locality multiplies the sides — 100 businesses and 5 springs in one
    town would count as 500), and the result is FULL OUTER so a settlement
    present in only one dataset survives, since that is usually the interesting
    row. No model, no budget; validation raises SemanticError on any pair that
    lacks a locality on either side."""
    _require_enabled()
    model = await semantic_model.build_model(db)
    try:
        entity, clean = semantic_model.validate_query(model, {
            "entity": body.left,
            "measures": ["count"],
            "dimensions": [],
            "filters": [],
            "join": {"entity": body.right, "measures": ["count"]},
        })
        sql = semantic_model.compile_sql(entity, clean, model)
    except SemanticError as e:
        # Not an exception path in the UI — "these two cannot be crossed" is an
        # ordinary answer with a stated reason.
        return {"ok": False, "reason": str(e)}
    return {
        "ok": True,
        "sql": sql,
        "explanation": semantic_model.explain_query(entity, clean, model),
    }


@router.get("/examples")
@limiter.limit("30/minute")
async def examples(request: Request, db: AsyncSession = Depends(get_db)):
    """Suggested questions, generated from the live model.

    Free (no LLM) and grounded — every suggestion names a real table and a real
    groupable column, so clicking one always produces an answer. A free-text box
    with no examples reads as a search box and gets search queries; showing the
    shape that works is most of the onboarding."""
    _require_enabled()
    model = await semantic_model.build_model(db)
    # Biggest tables first: they are the ones people came for, and a suggestion
    # over a 30-row table teaches the shape but wastes the click.
    ranked = sorted(model, key=lambda e: -(e.get("rows") or 0))
    out: list[dict] = []
    seen_families: set[str] = set()
    for ent in ranked:
        title = _clean_title(ent.get("title") or "")
        # An entity whose title is still a raw identifier (KNS_PlenumVoteResult,
        # append_x_9f3a) has no Hebrew name in the catalog. Putting it in a
        # Hebrew suggestion produces "כמה KNS_PlenumVoteResult לפי…", which
        # reads as broken and teaches nothing.
        if not title or not _HEB_RE.search(title):
            continue
        # One suggestion per dataset family. Ranking by row count alone put four
        # years of the same source in a list of eight — technically the biggest
        # tables, useless as a menu.
        family = _family_key(title)
        if family in seen_families:
            continue
        dim = next((d for d in ent["dimensions"]
                    if d.get("groupable") and d["kind"] == "text" and d.get("samples")), None)
        if not dim:
            continue
        seen_families.add(family)
        out.append({
            "question": f'כמה {title} לפי {_dim_label(dim)}',
            "table": ent["key"],
        })
        if len(out) >= 6:
            break
    cfg = await nl_query.get_config(db)
    ladder = nl_query.tiers(cfg)
    return {
        "examples": out,
        "model_size": len(model),
        "enabled": bool(cfg.get("enabled", True)),
        "llm": bool(ladder),
        # The escalation ladder as it is actually configured on this deployment,
        # cheapest first. Reported because "which model answered me" is a fair
        # question on a public transparency site, and because a mis-set key
        # silently changing the ladder is otherwise invisible.
        "tiers": [{"provider": p, "model": m} for p, m in ladder],
    }
