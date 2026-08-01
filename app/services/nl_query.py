"""Free-text Hebrew question → a validated query over the semantic model.

The pipeline, cheapest tier first. Every tier can decline, and declining is a
valid outcome — nothing here ever falls back to guessing.

    question
      ├─ fingerprint cache  → a previous answer for the same question (free)
      ├─ nl_templates.match → a deterministic reading (free)
      ├─ the LLM            → a JSON query, validated against the model (paid)
      └─ out of scope       → an honest refusal + what IS available

The language model never sees the database and never writes SQL. It sees a
retrieved slice of the declared model and answers with a small JSON object;
semantic_model.validate_query rejects anything referencing a field that was not
declared, and semantic_model.compile_sql turns the survivor into SQL. So the
worst a bad model output can do is fail validation.

Cost control lives here rather than at the endpoint because the ordering is the
control: the cache and the template matcher are what keep the paid tier from
running, and on a public site with repeated questions they matter more than the
choice of model.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Awaitable, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.services import semantic_model, nl_templates
from app.services.semantic_model import SemanticError

logger = logging.getLogger(__name__)

DEEPSEEK_MODEL = "deepseek-chat"

# The JSON the model must produce. Mirrors semantic_model.validate_query, which
# is the real gate — this schema only shapes the output, it does not secure it.
QUERY_SCHEMA = {
    "type": "object",
    "properties": {
        "entity": {"type": "string", "description": "מפתח הטבלה בדיוק כפי שמופיע במודל"},
        "measures": {"type": "array", "items": {"type": "string"},
                     "description": 'מדדים מתוך רשימת המדדים של הטבלה, למשל "count" או "sum:עמודה"'},
        "dimensions": {"type": "array", "items": {"type": "string"},
                       "description": "עמודות לקיבוץ (GROUP BY). ריק אם השאלה מבקשת מספר יחיד"},
        "filters": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "field": {"type": "string"},
                    "op": {"type": "string",
                           "enum": sorted(semantic_model.OPS)},
                    "value": {"description": "מחרוזת, מספר, או רשימה עבור in/between"},
                },
                "required": ["field", "op"],
                "additionalProperties": False,
            },
        },
        "enrich": {"type": "array", "items": {"type": "string"},
                   "description": "שדות מאינדקס היישובים להוספה: "
                                  + ", ".join(semantic_model.ENRICH_FIELDS)},
        "order": {
            "type": "object",
            "properties": {"by": {"type": "string", "enum": ["measure", "dimension", ""]},
                           "dir": {"type": "string", "enum": ["asc", "desc"]}},
            "additionalProperties": False,
        },
        "limit": {"type": "integer"},
        "unanswerable": {"type": "string",
                         "description": "אם אף טבלה במודל אינה יכולה לענות — הסבר קצר בעברית "
                                        "מדוע. אחרת מחרוזת ריקה."},
    },
    "required": ["entity", "measures", "dimensions", "filters", "unanswerable"],
    "additionalProperties": False,
}

_TOOL = {
    "name": "over_query",
    "description": "בניית שאילתה מובנית מעל המודל הסמנטי של גרסאות לעם.",
    "input_schema": QUERY_SCHEMA,
}

# Static across every request, so it is the cacheable prefix. Keep it that way:
# interpolating anything per-question here would invalidate the cache on every
# call and quietly triple the input cost.
_SYSTEM = (
    "אתה ממיר שאלות בעברית לשאילתות מובנות מעל קטלוג הנתונים של 'גרסאות לעם' "
    "(over.org.il), אתר שקיפות שמארכב מאגרי מידע ממשלתיים.\n\n"
    "אתה מקבל מודל של כמה טבלאות מועמדות. לכל טבלה: העמודות שלה, סוג כל עמודה, "
    "ודוגמאות לערכים אמיתיים שמופיעים בה. אתה מחזיר אך ורק אובייקט שאילתה מובנה.\n\n"
    "כללים:\n"
    "1. השתמש אך ורק בשמות טבלאות, עמודות ומדדים שמופיעים במודל שסופק, "
    "אות-באות. אל תמציא עמודה, אל תתקן שם, ואל תשתמש בעמודה מטבלה אחרת.\n"
    "2. ערך בפילטר חייב להיות ערך שמופיע ברשימת הדוגמאות של אותה עמודה, אלא אם "
    "מדובר במספר או בתאריך. אם המשתמש כתב ניסוח שונה מעט מהערך במאגר — בחר את "
    "הערך מהמאגר.\n"
    "3. אם השאלה מבקשת מספר יחיד ('כמה…') — dimensions ריק. אם היא מבקשת פילוח "
    "('לפי…', 'בכל…') — שים את עמודת הפילוח ב-dimensions.\n"
    "4. אם אף טבלה מהמודל אינה יכולה לענות על השאלה — מלא unanswerable בהסבר קצר "
    "בעברית והשאר את שאר השדות ריקים. עדיף להשיב 'אין לי את הנתון' מאשר לענות "
    "מטבלה שאינה מתאימה. זהו אתר שקיפות: מספר שגוי גרוע ממספר חסר.\n"
    "5. אל תכתוב SQL. אל תסביר. החזר את האובייקט בלבד."
)


# ── fingerprint cache ────────────────────────────────────────────────────────
# The single biggest cost lever on a public page: the same questions get asked
# over and over, and a repeat costs nothing. Keyed on the NORMALIZED token
# sequence, so punctuation, spacing and a leading clitic do not split the key.
# The model signature is mixed in so a catalog change (a new column, a renamed
# table) invalidates every entry rather than replaying SQL against a shape that
# no longer exists.

def _model_signature(model: list[dict]) -> str:
    payload = "|".join(
        f'{e["key"]}:{",".join(d["key"] for d in e["dimensions"])}'
        for e in sorted(model, key=lambda x: x["key"])
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:12]


def fingerprint(question: str, model_sig: str) -> str:
    toks = " ".join(semantic_model.tokens(question))
    return hashlib.sha256(f"{model_sig}\n{toks}".encode("utf-8")).hexdigest()[:32]


async def cache_get(db: AsyncSession, fp: str) -> dict | None:
    """Look up a previous answer, bumping its hit counter.

    Lives in the APP DB, not the append DB. That is a privacy boundary, not a
    filing preference: every ``public.over_*`` table in the append DB is
    surfaced by data_catalog and is SELECT-able through the public /data
    console, so a cache there would publish every question anyone typed."""
    try:
        row = (await db.execute(text(
            "UPDATE nl_query_cache SET hits = hits + 1, last_hit_at = now() "
            "WHERE fingerprint = :fp "
            "RETURNING entity, query_json, sql, explanation, source, model"),
            {"fp": fp})).mappings().first()
        await db.commit()
    except Exception:  # noqa: BLE001 — a cache miss is always a safe answer
        logger.debug("nl_query: cache read failed", exc_info=True)
        await _quiet_rollback(db)
        return None
    if not row:
        return None
    qj = row["query_json"]
    return {
        "entity": row["entity"],
        "query": json.loads(qj) if isinstance(qj, str) else qj,
        "sql": row["sql"],
        "explanation": row["explanation"],
        "source": row["source"],
        "model": row["model"],
    }


async def cache_put(db: AsyncSession, fp: str, question: str, res: dict) -> None:
    try:
        await db.execute(text(
            "INSERT INTO nl_query_cache "
            "(fingerprint, question, entity, query_json, sql, explanation, source, model) "
            "VALUES (:fp, :q, :e, CAST(:j AS json), :s, :x, :src, :m) "
            "ON CONFLICT (fingerprint) DO NOTHING"),
            {"fp": fp, "q": question, "e": res.get("entity"),
             "j": json.dumps(res.get("query"), ensure_ascii=False),
             "s": res.get("sql"), "x": res.get("explanation"),
             "src": res.get("source"), "m": res.get("model")})
        await db.commit()
    except Exception:  # noqa: BLE001 — never fail a good answer over bookkeeping
        logger.debug("nl_query: cache write failed", exc_info=True)
        await _quiet_rollback(db)


async def _quiet_rollback(db: AsyncSession) -> None:
    try:
        await db.rollback()
    except Exception:  # noqa: BLE001
        pass


async def log_query(db: AsyncSession, **row) -> None:
    """Append one row to the admin log. Best-effort — never fails a request.

    Every question lands here, including the free ones and the refused ones.
    Logging only the paid calls would hide the two numbers that decide whether
    this feature is worth its bill: the cache hit rate and the refusal rate."""
    try:
        await db.execute(text(
            "INSERT INTO nl_query_log "
            "(question, answered, stage, attempts, model, escalated, entity, sql, "
            " reason, input_tokens, output_tokens, duration_ms) "
            "VALUES (:question, :answered, :stage, :attempts, :model, :escalated, "
            "        :entity, :sql, :reason, :input_tokens, :output_tokens, :duration_ms)"),
            {"question": row.get("question", ""), "answered": bool(row.get("answered")),
             "stage": row.get("stage", "error"), "attempts": row.get("attempts"),
             "model": row.get("model"), "escalated": bool(row.get("escalated")),
             "entity": row.get("entity"), "sql": row.get("sql"),
             "reason": (row.get("reason") or None), "input_tokens": int(row.get("input_tokens") or 0),
             "output_tokens": int(row.get("output_tokens") or 0),
             "duration_ms": row.get("duration_ms")})
        await db.commit()
    except Exception:  # noqa: BLE001
        logger.debug("nl_query: log write failed", exc_info=True)
        await _quiet_rollback(db)


# ── runtime config (admin kill switch) ───────────────────────────────────────
# Read on every request, with a short cache so it is not a round-trip per
# question. The TTL is the blast radius of turning something off: 30s means an
# admin who disables a tier because it is burning money sees it stop within
# half a minute, which is the point of having the switch in the DB at all.
_CONFIG_TTL = 30.0
_config_cache: dict | None = None
_config_at = 0.0

def _config_defaults() -> dict:
    """What the switches are before an admin touches anything.

    Computed rather than a constant so config.py stays the DEPLOY default and
    the DB row is the OVERRIDE. Hard-coding True here would have made
    ``nl_query_escalate_on_unanswerable`` dead config the moment the table
    existed — a setting that reads as live and silently does nothing."""
    return {
        "enabled": True, "allow_deepseek": True, "allow_anthropic": True,
        "escalate_on_unanswerable": settings.nl_query_escalate_on_unanswerable,
        "daily_call_budget": None, "daily_output_token_budget": None,
    }


def invalidate_config_cache() -> None:
    """Drop the cached config so the next request re-reads it. Called by the
    admin endpoint right after a write, so a change takes effect immediately
    rather than at the end of the TTL."""
    global _config_cache, _config_at
    _config_cache, _config_at = None, 0.0


async def get_config(db: AsyncSession) -> dict:
    global _config_cache, _config_at
    if _config_cache is not None and time.monotonic() - _config_at < _CONFIG_TTL:
        return _config_cache
    try:
        row = (await db.execute(text(
            "SELECT enabled, allow_deepseek, allow_anthropic, escalate_on_unanswerable, "
            "daily_call_budget, daily_output_token_budget FROM nl_query_config WHERE id = 1"
        ))).mappings().first()
        cfg = dict(row) if row else _config_defaults()
    except Exception:  # noqa: BLE001 — pre-migration, or a DB blip
        # Fail OPEN on the feature but keep every other guard: the per-IP
        # limiter, the day budget from config.py, and the semantic validation
        # are all still in force. Failing closed here would take the feature
        # down for a bookkeeping error.
        logger.debug("nl_query: config read failed; using defaults", exc_info=True)
        await _quiet_rollback(db)
        cfg = _config_defaults()
    _config_cache, _config_at = cfg, time.monotonic()
    return cfg


# ── the paid tier ────────────────────────────────────────────────────────────

def provider() -> str | None:
    """The tier that would be tried FIRST, ignoring admin overrides.

    Kept for callers that just want to know whether a paid path is compiled in
    at all. The live call path is ``tiers(cfg)``."""
    t = tiers()
    return t[0][0] if t else None


def tiers(cfg: dict | None = None) -> list[tuple[str, str]]:
    """The escalation ladder as (provider, model), cheapest first.

    Two independent gates per tier: an API key must be configured (deploy-time)
    AND the admin must not have switched it off (runtime). The admin gate is
    what makes this a cost control — the moment you want to stop paying for a
    tier is the moment you do not want to wait for a redeploy.

    A deployment with one key behaves as a single tier with no escalation.
    Ordering is hard-coded rather than derived from a price table: prices move,
    and the point of the ladder is capability order, which does not."""
    cfg = cfg or {}
    out: list[tuple[str, str]] = []
    if settings.deepseek_api_key and cfg.get("allow_deepseek", True):
        out.append(("deepseek", DEEPSEEK_MODEL))
    if settings.anthropic_api_key and cfg.get("allow_anthropic", True):
        out.append(("anthropic", settings.nl_query_anthropic_model))
    if not settings.nl_query_escalate:
        return out[:1]
    return out


def _user_prompt(question: str, entities: list[dict]) -> str:
    return (f"השאלה: {question}\n\n"
            f"הטבלאות המועמדות:\n{semantic_model.model_prompt(entities)}")


async def _ask_anthropic(question: str, entities: list[dict],
                         model: str | None = None) -> tuple[dict | None, tuple[int, int]]:
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    resp = await client.messages.create(
        model=model or settings.nl_query_anthropic_model,
        max_tokens=1500,
        # Tools render before system, so a breakpoint on the last system block
        # caches BOTH — and both are byte-identical on every request. The
        # per-question part (the retrieved model slice) lives in the user turn,
        # after the breakpoint, where it belongs.
        system=[{"type": "text", "text": _SYSTEM, "cache_control": {"type": "ephemeral", "ttl": "1h"}}],
        tools=[_TOOL],
        tool_choice={"type": "tool", "name": "over_query"},
        messages=[{"role": "user", "content": _user_prompt(question, entities)}],
    )
    usage = (int(getattr(resp.usage, "input_tokens", 0) or 0),
             int(getattr(resp.usage, "output_tokens", 0) or 0))
    out = next((b.input for b in resp.content if getattr(b, "type", None) == "tool_use"), None)
    return out, usage


async def _ask_deepseek(question: str, entities: list[dict],
                        model: str | None = None) -> tuple[dict | None, tuple[int, int]]:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url="https://api.deepseek.com")
    hint = ("החזר JSON יחיד עם המפתחות: entity (string), measures (array of string), "
            "dimensions (array of string), filters (array of {field, op, value}), "
            "enrich (array of string), order ({by, dir}), limit (integer), "
            "unanswerable (string, ריק אם יש תשובה).")
    resp = await client.chat.completions.create(
        model=model or DEEPSEEK_MODEL, temperature=0, max_tokens=1500,
        response_format={"type": "json_object"},
        messages=[{"role": "system", "content": _SYSTEM + "\n" + hint},
                  {"role": "user", "content": _user_prompt(question, entities)}],
    )
    u = getattr(resp, "usage", None)
    usage = (int(getattr(u, "prompt_tokens", 0) or 0), int(getattr(u, "completion_tokens", 0) or 0))
    try:
        data = json.loads(resp.choices[0].message.content or "")
    except (ValueError, TypeError):
        return None, usage
    return (data if isinstance(data, dict) else None), usage


# A model can answer with a fenced block or stray prose around the object even
# under JSON mode; pull the first object out rather than failing the request.
_JSON_OBJ = re.compile(r"\{.*\}", re.DOTALL)


def _coerce(raw: object) -> dict | None:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        m = _JSON_OBJ.search(raw)
        if m:
            try:
                out = json.loads(m.group(0))
                return out if isinstance(out, dict) else None
            except ValueError:
                return None
    return None


class OutOfScope(Exception):
    """No declared entity can answer the question.

    Carries the retrieved candidates so the caller can say what IS available —
    a refusal that names the nearby datasets is useful; a bare "לא נמצא" is not.
    """

    def __init__(self, message: str, candidates: list[dict] | None = None, *,
                 attempts: list[str] | None = None,
                 usage: tuple[int, int] = (0, 0)):
        super().__init__(message)
        self.message = message
        self.candidates = candidates or []
        # A refusal can still have cost money — it may have run two model tiers
        # before concluding it could not answer. The admin log needs that, or
        # refusals look free and the bill does not add up.
        self.attempts = attempts or []
        self.usage = usage


async def answer(
    db: AsyncSession,
    question: str,
    *,
    allow_llm: bool = True,
    use_cache: bool = True,
    reserve: "Callable[[], Awaitable[bool]] | None" = None,
    on_usage: "Callable[[int, int], Awaitable[None]] | None" = None,
) -> dict:
    """Resolve a question to {sql, query, explanation, source, entity}.

    ``allow_llm=False`` runs the free tiers only — used when the budget is
    exhausted, so the feature degrades to "templates still work" instead of
    going dark. Raises OutOfScope when nothing can answer.

    ``reserve`` is awaited immediately before the paid call and nowhere else —
    that ordering is the point. Charging budget at the top of the function would
    make a cache hit or a template match consume quota, and those are the two
    tiers that keep the bill down; they must stay free. ``on_usage`` records the
    tokens the call actually cost, after it returns."""
    cfg = await get_config(db)
    if not cfg.get("enabled", True):
        raise OutOfScope(
            "המענה בשפה חופשית מושבת כרגע. אפשר להשתמש בבונה השאילתות "
            "או בקונסולת ה-SQL.")

    model = await semantic_model.build_model(db)
    if not model:
        raise OutOfScope("קטלוג הנתונים עדיין נטען — נסו שוב בעוד רגע.")
    sig = _model_signature(model)
    fp = fingerprint(question, sig)

    # ``use_cache=False`` skips the READ but keeps the write. That asymmetry is
    # deliberate and exists for the benchmark: once a question is cached, every
    # later run measures the cache rather than the models, so a benchmark that
    # could not bypass the read would report a perfect score for a broken model
    # the second time it ran.
    cached = await cache_get(db, fp) if use_cache else None
    if cached and cached.get("sql"):
        return {**cached, "cached": True, "stage": "cache"}

    # Tier 1 — deterministic.
    tmpl = nl_templates.match(model, question)
    if tmpl is not None:
        try:
            entity, clean = semantic_model.validate_query(model, tmpl)
            res = {
                "entity": entity["key"],
                "query": clean,
                "sql": semantic_model.compile_sql(entity, clean),
                "explanation": semantic_model.explain_query(entity, clean),
                "source": "template",
            }
            await cache_put(db, fp, question, res)
            return {**res, "cached": False, "stage": "template"}
        except SemanticError:
            # The matcher built something the model rejects. That is a matcher
            # bug, not a user error — log it and let the LLM try.
            logger.warning("nl_query: template produced an invalid query for %r", question,
                           exc_info=True)

    candidates = semantic_model.retrieve(model, question, k=settings.nl_query_top_k)
    if not allow_llm:
        raise OutOfScope(
            "השאלה דורשת ניתוח מתקדם, והמכסה היומית של התכונה נוצלה. אפשר לנסח "
            "את השאלה בצורה פשוטה יותר ('כמה X לפי Y'), להשתמש בבונה השאילתות, "
            "או לנסות שוב מחר.", candidates)
    if not candidates:
        raise OutOfScope(
            "לא נמצאה טבלה במודל שיכולה לענות על השאלה. נסו לנסח אותה במונחים "
            "שמופיעים בשמות המאגרים, או לחפש בקטלוג.", [])

    ladder = tiers(cfg)
    if not ladder:
        raise OutOfScope(
            "מענה בשפה חופשית על שאלות מורכבות אינו מוגדר בשרת כרגע. אפשר "
            "להשתמש בבונה השאילתות או בקונסולת ה-SQL.", candidates)

    # ── Tier 2+ — the paid ladder, cheapest model first ──────────────────
    #
    # Escalation fires only on failures we can DETECT: the model returned
    # something unparseable, or a query naming a field that is not in the
    # declared model, or the provider errored. It cannot catch the failure that
    # matters most — a cheap model emitting a query that validates perfectly and
    # answers the wrong question. Nothing here sees that; only an eval set does.
    # So this ladder buys back the coverage a weak model loses, not its accuracy.
    #
    # Each tier reserves budget separately, because each tier costs money. An
    # escalated question therefore consumes two units of quota, which is correct:
    # it did cost twice.
    refusal: str | None = None
    attempted: list[str] = []
    spent = [0, 0]  # accumulated (input, output) across every tier that ran
    for i, (name, model_id) in enumerate(ladder):
        is_last = i == len(ladder) - 1
        attempted.append(name)

        if reserve is not None and not await reserve():
            # Out of quota mid-ladder. If a cheaper tier already produced a
            # refusal reason, that is a better message than a bare quota error.
            raise OutOfScope(refusal or (
                "המכסה היומית של המענה בשפה חופשית נוצלה. שאלות בתבנית פשוטה "
                "('כמה X לפי Y') עדיין עובדות, וכך גם בונה השאילתות וקונסולת ה-SQL."),
                candidates, attempts=attempted[:-1], usage=tuple(spent))

        ask = _ask_deepseek if name == "deepseek" else _ask_anthropic
        try:
            raw, usage = await ask(question, candidates, model_id)
        except Exception as e:  # noqa: BLE001 — provider/network failure
            # A provider outage is exactly what a second tier is for. Only the
            # last one is allowed to surface as an error.
            logger.warning("nl_query: tier %s (%s) failed: %s", name, model_id, e)
            if is_last:
                raise
            refusal = refusal or "שירות המודל הזול לא הגיב"
            continue
        spent[0] += usage[0]
        spent[1] += usage[1]
        if on_usage is not None and any(usage):
            await on_usage(*usage)

        parsed = _coerce(raw)
        if not parsed:
            if not is_last:
                logger.info("nl_query: escalating past %s — unparseable output", name)
                refusal = refusal or "לא הצלחתי לפרש את השאלה."
                continue
            raise OutOfScope("לא הצלחתי לפרש את השאלה. נסו לנסח אותה אחרת.", candidates,
                             attempts=attempted, usage=tuple(spent))

        said_no = (parsed.get("unanswerable") or "").strip()
        if said_no:
            # A weak model's "I can't" is not the same claim as a strong one's.
            # Accepting it verbatim would make the cheapest model the site's
            # coverage ceiling — the exact thing the ladder exists to prevent.
            # It is also the costliest escalation trigger, since out-of-scope is
            # a COMMON outcome here, not a rare one; hence the switch.
            if not is_last and cfg.get("escalate_on_unanswerable",
                                       settings.nl_query_escalate_on_unanswerable):
                logger.info("nl_query: escalating past %s — it declined: %s", name, said_no)
                refusal = said_no
                continue
            raise OutOfScope(said_no, candidates,
                             attempts=attempted, usage=tuple(spent))

        try:
            entity, clean = semantic_model.validate_query(model, parsed)
        except SemanticError as e:
            # The model named something undeclared. On a cheap tier that is a
            # capability signal worth escalating on; on the last tier it is the
            # honest answer.
            if not is_last:
                logger.info("nl_query: escalating past %s — invalid query: %s", name, e)
                refusal = refusal or str(e)
                continue
            raise

        res = {
            "entity": entity["key"],
            "query": clean,
            "sql": semantic_model.compile_sql(entity, clean),
            "explanation": semantic_model.explain_query(entity, clean),
            "source": name,
            "model": model_id,
            # True when a cheaper tier was tried first and could not answer.
            # Surfaced so the cost of escalation is measurable rather than
            # inferred from the bill.
            "escalated": i > 0,
        }
        await cache_put(db, fp, question, res)
        return {**res, "cached": False, "usage": tuple(spent),
                "stage": name, "attempts": attempted}

    raise OutOfScope(refusal or "לא הצלחתי לענות על השאלה.", candidates,
                     attempts=attempted, usage=tuple(spent))
