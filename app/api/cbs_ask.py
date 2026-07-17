"""Natural-language search over the CBS index.

``POST /api/cbs/ask`` takes a free-text question (Hebrew or English), asks an LLM
to parse it into structured filters + a cleaned full-text query (grounded in the
live facets so it maps to real values), runs the SAME FTS search as
/api/cbs/search, and returns a one-line answer + the results. Backs the "שפה
חופשית" mode in the לץ הלמ"ס extension.

Provider is chosen by whichever key is set (DeepSeek preferred, then Anthropic):
  * DEEPSEEK_API_KEY  → DeepSeek ``deepseek-chat`` via the OpenAI-compatible API.
  * ANTHROPIC_API_KEY → Claude ``claude-opus-4-8``.
Neither set ⇒ 503 (feature off), so the rest of the CBS API keeps working.

NOTE: no ``from __future__ import annotations`` here — with the slowapi
``@limiter.limit`` wrapper it stringifies the endpoint hints and FastAPI then
mis-reads ``body: AskRequest`` as a query param (422). Keep hints as real types.
"""
import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.cbs_parse import GEO_LADDER, geo_matrix, parse_question
from app.api.cbs_search_util import RESULT_COLS, build_search
from app.config import settings
from app.database import get_db
from app.rate_limit import limiter
from app.services.llm_budget import reserve_llm_call

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cbs", tags=["cbs"])

ANTHROPIC_MODEL = "claude-opus-4-8"
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# The structured shape both providers must return. Empty string / 0 mean "no
# constraint". Anthropic gets it as a forced-tool input_schema; DeepSeek gets it
# described in the prompt + JSON mode.
_SCHEMA = {
    "type": "object",
    "properties": {
        "query": {"type": "string",
                  "description": "מילות מפתח לחיפוש טקסט חופשי (עברית), ללא מילות-פילטר כמו שנים/רמה גאוגרפית"},
        "geo": {"type": "string", "description": "רמה גאוגרפית — בדיוק מתוך הרשימה שסופקה, או ריק"},
        "file_type": {"type": "string", "description": "סיומת קובץ מתוך הרשימה, או ריק"},
        "section": {"type": "string", "description": "סוג עמוד מתוך הרשימה, או ריק"},
        "year_from": {"type": "integer", "description": "שנת התחלה, או 0"},
        "year_to": {"type": "integer", "description": "שנת סיום, או 0"},
        "answer": {"type": "string",
                   "description": "משפט אחד בעברית: היכן/כיצד למצוא זאת בלמ\"ס (מדריך קצר למשתמש)"},
    },
    "required": ["query", "geo", "file_type", "section", "year_from", "year_to", "answer"],
    "additionalProperties": False,
}

_PARSE_TOOL = {
    "name": "cbs_query",
    "description": "פירוק שאלה חופשית לחיפוש באינדקס הלמ\"ס.",
    "input_schema": _SCHEMA,
}

_SYSTEM = (
    "אתה עוזר חיפוש מעל אינדקס אתר הלשכה המרכזית לסטטיסטיקה (למ\"ס). המשתמש שואל "
    "שאלה חופשית; פרק אותה לפילטרים מובנים ולשאילתת מילות-מפתח.\n"
    "כללים חשובים:\n"
    "1. query = רק מילות התוכן המהותיות (שמות נושא, מקום, שנה) — בלי מילות שאלה "
    "כמו 'האם/יש/כמה/דרך/להגיע/נתונים'.\n"
    "2. השתמש בכמה שפחות פילטרים. הגדר פילטר רק אם הוא נובע ישירות מהשאלה, אחרת "
    "השאר ריק/0. עדיף פחות פילטרים ותוצאות מאשר סינון-יתר ואפס תוצאות.\n"
    "3. file_type — השאר ריק אלא אם המשתמש ציין פורמט מפורשות (למשל 'קובץ אקסל'). "
    "אל תנחש פורמט.\n"
    "4. geo/file_type/section חייבים להיות ערך מדויק מתוך הרשימות שסופקו (או ריק).\n"
    "5. answer = משפט קצר וממוקד בעברית שמנחה איפה למצוא את המידע."
)


class AskRequest(BaseModel):
    q: str
    limit: int = 30


def _provider() -> str | None:
    if settings.deepseek_api_key:
        return "deepseek"
    if settings.anthropic_api_key:
        return "anthropic"
    return None


async def _facets(db: AsyncSession) -> dict:
    async def dj(col: str) -> list[str]:
        r = await db.execute(text(
            f"SELECT DISTINCT elem FROM cbs_index CROSS JOIN LATERAL "
            f"jsonb_array_elements_text({col}) AS elem WHERE {col} IS NOT NULL "
            f"AND jsonb_typeof({col}) = 'array' ORDER BY 1"))
        return [x[0] for x in r]

    async def dc(col: str) -> list[str]:
        r = await db.execute(text(
            f"SELECT DISTINCT {col} FROM cbs_index WHERE {col} IS NOT NULL ORDER BY 1"))
        return [x[0] for x in r]

    return {"geo_levels": await dj("geo_levels"),
            "file_types": await dj("file_types"),
            "sections": await dc("section")}


def _user_prompt(q: str, facets: dict) -> str:
    return (
        f"שאלה: {q}\n\n"
        f"רמות גאוגרפיות זמינות: {', '.join(facets['geo_levels'])}\n"
        f"סוגי קבצים זמינים: {', '.join(facets['file_types'])}\n"
        f"סוגי עמודים (section): {', '.join(facets['sections'])}"
    )


async def _parse_anthropic(q: str, facets: dict) -> dict | None:
    from anthropic import AsyncAnthropic

    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    resp = await client.messages.create(
        model=ANTHROPIC_MODEL, max_tokens=1024, system=_SYSTEM,
        tools=[_PARSE_TOOL], tool_choice={"type": "tool", "name": "cbs_query"},
        messages=[{"role": "user", "content": _user_prompt(q, facets)}],
    )
    return next((b.input for b in resp.content if getattr(b, "type", None) == "tool_use"), None)


async def _parse_deepseek(q: str, facets: dict) -> dict | None:
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.deepseek_api_key, base_url=DEEPSEEK_BASE_URL)
    # DeepSeek JSON mode: needs the word "json" in the prompt + the exact shape.
    schema_hint = (
        "החזר JSON יחיד בלבד עם המפתחות: "
        'query (string), geo (string), file_type (string), section (string), '
        "year_from (integer, 0 אם אין), year_to (integer, 0 אם אין), answer (string). "
        "geo/file_type/section חייבים להיות ערך מדויק מתוך הרשימות (או מחרוזת ריקה)."
    )
    resp = await client.chat.completions.create(
        model=DEEPSEEK_MODEL,
        temperature=0,
        max_tokens=1024,
        response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": _SYSTEM + " " + schema_hint},
            {"role": "user", "content": _user_prompt(q, facets)},
        ],
    )
    content = resp.choices[0].message.content or ""
    try:
        data = json.loads(content)
    except (ValueError, TypeError):
        return None
    if not isinstance(data, dict):
        return None
    # Coerce to the shape _run_search expects (tolerate missing / null keys).
    def _int(v):
        try:
            return int(v or 0)
        except (ValueError, TypeError):
            return 0
    return {
        "query": str(data.get("query") or "").strip(),
        "geo": str(data.get("geo") or "").strip(),
        "file_type": str(data.get("file_type") or "").strip(),
        "section": str(data.get("section") or "").strip(),
        "year_from": _int(data.get("year_from")),
        "year_to": _int(data.get("year_to")),
        "answer": str(data.get("answer") or "").strip(),
    }


async def _run_search(db: AsyncSession, p: dict, limit: int) -> tuple[int, list[dict]]:
    # Map the LLM's parsed shape (query/geo/file_type/section/year_*) onto the
    # shared builder so /ask ranks identically to /search and the MCP tool —
    # same intent-boost, catch-all demotion and recency tie-breakers.
    where, order, params = build_search(
        {
            "q": p.get("query"), "geo": p.get("geo"), "file_type": p.get("file_type"),
            "section": p.get("section"), "year_from": p.get("year_from"),
            "year_to": p.get("year_to"),
        },
        sort="relevance",
    )
    total = (await db.execute(text(f"SELECT count(*) FROM cbs_index{where}"), params)).scalar_one()
    params["limit"] = max(1, min(int(limit), 60))
    rows = (await db.execute(
        text(f"SELECT {RESULT_COLS} FROM cbs_index{where} ORDER BY {order} LIMIT :limit"), params
    )).mappings().all()
    return int(total), [dict(r) for r in rows]


# Order in which filters are relaxed when the LLM over-constrains and the search
# comes back empty. file_type first (the LLM guesses a format the user never
# asked for), then section, then the year window, geo last (geo is usually the
# strongest signal of intent). The text query is never dropped.
_RELAX_ORDER = ("file_type", "section", "year_from", "year_to", "geo")


async def _search_relaxed(db: AsyncSession, parsed: dict, limit: int) -> tuple[int, list[dict], dict]:
    """Run the search; if the full filter set yields nothing, drop filters one by
    one (per _RELAX_ORDER) until results appear. Returns the filters actually used."""
    active = dict(parsed)
    total, results = await _run_search(db, active, limit)
    if total:
        return total, results, active
    for key in _RELAX_ORDER:
        if not active.get(key):
            continue
        active = dict(active)
        active[key] = 0 if key in ("year_from", "year_to") else ""
        total, results = await _run_search(db, active, limit)
        if total:
            return total, results, active
    return total, results, active


async def _parse_question(db: AsyncSession, q: str) -> tuple[str, dict]:
    """Shared LLM parse: free-text question → (provider, structured filters).

    Raises HTTPException on the same conditions as before so /ask and /resolve
    behave identically. Grounds the model in the live facets so it only emits
    real geo/file_type/section values."""
    provider = _provider()
    if not provider:
        raise HTTPException(status_code=503, detail="natural-language search is not configured")
    facets = await _facets(db)
    try:
        parsed = await (_parse_deepseek(q, facets) if provider == "deepseek"
                        else _parse_anthropic(q, facets))
    except Exception as e:  # noqa: BLE001
        logger.warning("cbs/ask LLM error (%s): %s", provider, e)
        raise HTTPException(status_code=502, detail="language model error")
    if not parsed:
        raise HTTPException(status_code=502, detail="could not parse the question")
    return provider, parsed


_FILTER_KEYS = ("query", "geo", "file_type", "section", "year_from", "year_to")


async def _enforce_llm_budget(db: AsyncSession) -> None:
    """Reserve one call against the GLOBAL daily LLM budget, or 429.

    Runs before any LLM call on the public (unauthenticated) endpoints so a
    blocked request never spends money. The budget is global + day-keyed, so it
    can't be sidestepped by rotating IPs the way the per-IP limiter can. See
    app/services/llm_budget.py."""
    if not _provider():
        # No provider configured ⇒ _parse_question will 503 and no LLM is ever
        # called. Don't spend budget on a request that can't cost anything.
        return
    if not await reserve_llm_call(db):
        email = getattr(settings, "api_contact_email", "guy@z-g.co.il")
        raise HTTPException(
            status_code=429,
            headers={"Retry-After": "3600"},
            detail=(
                "החיפוש בשפה חופשית עמוס כרגע — נוצלה מכסת השאילתות היומית של "
                "התכונה. אפשר להשתמש בחיפוש הרגיל, או לנסות שוב מאוחר יותר. "
                f"לשימוש מוגבר בהיקף גדול — נא ליצור קשר בכתובת {email}. "
                "The free natural-language search has reached its daily quota; "
                "use the regular search or try again later."
            ),
        )


@router.post("/ask")
@limiter.limit("20/minute")
async def ask(request: Request, body: AskRequest, db: AsyncSession = Depends(get_db)):
    """LLM-parsed natural-language search over the CBS index."""
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")

    await _enforce_llm_budget(db)
    provider, parsed = await _parse_question(db, q)
    total, results, used = await _search_relaxed(db, parsed, body.limit)
    relaxed = [k for k in _FILTER_KEYS if parsed.get(k) and not used.get(k)]
    return {
        "answer": parsed.get("answer") or "",
        "provider": provider,
        "filters": {k: used.get(k) for k in _FILTER_KEYS},
        "requested_filters": {k: parsed.get(k) for k in _FILTER_KEYS},
        "relaxed": relaxed,
        "total": total,
        "results": results,
    }


# ── resolve: structured "where is this at CBS?" answer ─────────────────────
# Same LLM-parse + relaxed search as /ask, but classifies the top hit into an
# actionable answer_type and surfaces caveats, so a client (website card, MCP,
# extension) can render "here's the file" vs "run this generator" vs "not held by
# CBS — try special processing" instead of a raw result list. Reused by the MCP
# ``resolve`` tool (app/mcp/cbs_server.py) so every surface answers identically.

# Fallback answer text per type; intents/negatives override with their own
# curated guidance (extra.answer).
_ANSWER_TEXT = {
    "no_results": ("לא נמצא מקור מתאים באינדקס הלמ\"ס. ייתכן שהנתון אינו פומבי — "
                   "שקול פנייה לעיבוד מיוחד: info@cbs.gov.il / ibudim@cbs.gov.il."),
    "generator": "המקור הוא מחולל/דשבורד של הלמ\"ס — הפעל אותו ובחר את החתך הרצוי.",
    "data_file": "נמצא קובץ נתונים ישיר להורדה בעמוד המקור בלמ\"ס.",
    "publication": "נמצא פרסום/עמוד רלוונטי באתר הלמ\"ס.",
    "special_processing": ('הנתון קיים בלמ"ס אך אינו מוצר מדף — נדרש עיבוד מיוחד או '
                           'גישה לחדר המחקר: info@cbs.gov.il / ibudim@cbs.gov.il.'),
}


def _classify(top: dict | None, total: int) -> str:
    if total == 0 or top is None:
        return "no_results"
    extra = top.get("extra") or {}
    item_type = top.get("item_type") or ""
    # Two hand-reviewed families record what a crawl can never learn — whether CBS
    # holds something at all. They are OPPOSITE answers and must not be conflated:
    #   intent_negative → CBS does not hold it (or not at that resolution).
    #   intent_special  → CBS DOES hold it, but only via a request / the research
    #                     room. "Ask ibudim@" is a real answer, not a dead end.
    # Both carry an item_type other than 'intent' on purpose: build_search() boosts
    # only 'intent', so neither can float above real results on a weak lexical
    # match — a wrong "CBS doesn't have this" is worse than no answer at all.
    # See CURATION in GOV scraper/cbs_intents_benchmark.py for the review.
    if item_type == "intent_special" or extra.get("verdict") == "special_processing":
        return "special_processing"
    if item_type == "intent_negative" or extra.get("negative"):
        return "not_available"
    if item_type == "intent":
        return "guidance"
    if item_type == "tool" or top.get("section") == "tools":
        return "generator"
    if any(ft in ("xlsx", "xls", "csv") for ft in (top.get("file_types") or [])):
        return "data_file"
    return "publication"


def _primary_link(top: dict) -> str:
    # Intent rows keep the clean navigational target in extra.link (their own
    # ``url`` carries a #intent-… fragment for key-uniqueness); everything else
    # uses its page URL.
    return (top.get("extra") or {}).get("link") or top.get("url") or ""


# ── locality entity detection (gazetteer scan) ─────────────────────────────
# 22 benchmark questions name a place ("כמה עולים יש בבית שמש"). The gazetteer
# (≈1,500 localities + aliases) is scanned in-process against the question —
# cheap, and avoids fragile SQL substring matching over Hebrew word boundaries.
# Cached per-process; the table changes ~once a year.

_gazetteer_cache: list[tuple[str, dict]] | None = None

# A one-letter Hebrew prefix (ב/ל/מ/ו/ש/כ/ה) glued to the name is how places
# actually appear in questions: "בבית שמש", "מנהריה".
_HEB_PREFIXES = "בלמושכה"


async def _load_gazetteer(db: AsyncSession) -> list[tuple[str, dict]]:
    global _gazetteer_cache
    if _gazetteer_cache is not None:
        return _gazetteer_cache
    try:
        rows = (await db.execute(text(
            "SELECT code, name, name_en, aliases, district, subdistrict, population "
            "FROM cbs_gazetteer"
        ))).mappings().all()
    except Exception:  # noqa: BLE001 — table absent (pre-migration) ⇒ no entity chips
        return []
    if not rows:
        # Not loaded yet — do NOT cache emptiness, or the process would stay
        # entity-blind until restart even after /gazetteer/load runs.
        return []
    entries: list[tuple[str, dict]] = []
    for r in rows:
        info = {"code": r["code"], "name": r["name"], "district": r["district"],
                "subdistrict": r["subdistrict"], "population": r["population"]}
        for n in [r["name"], *(r["aliases"] or [])]:
            if n and len(n) >= 3:
                entries.append((n, info))
    # Longest names first so "תל אביב -יפו" wins over an alias "תל אביב"
    # contained in it, and big places break exact-length ties.
    entries.sort(key=lambda e: (-len(e[0]), -(e[1]["population"] or 0)))
    _gazetteer_cache = entries
    return entries


def _find_locality(q: str, entries: list[tuple[str, dict]]) -> dict | None:
    for name, info in entries:
        i = q.find(name)
        if i < 0:
            continue
        # Word-boundary check: the char before must be a space/punct or a
        # single-letter Hebrew prefix; the char after must not be a letter.
        before = q[i - 1] if i > 0 else " "
        after = q[i + len(name)] if i + len(name) < len(q) else " "
        before_ok = not before.isalpha() or (
            before in _HEB_PREFIXES and (i < 2 or not q[i - 2].isalpha())
        )
        if before_ok and not after.isalpha():
            return info
    return None


async def resolve_question(db: AsyncSession, q: str, limit: int = 10) -> dict:
    """Structured resolution of a free-text question to a CBS location.

    Retrieval runs on the RAW question, deliberately WITHOUT the LLM parse that
    /ask uses. Measured on the WhatsApp benchmark (171 ground-truth queries,
    Lamas/eval/cbs_search_eval.py):

        raw question  → hit@10 15.8%, MRR 0.117
        LLM-cleaned   → hit@10 12.9%, MRR 0.096

    The LLM extracts keywords and drops the phrasing — but the intent layer's
    full_text is written in the user's own words, so that phrasing is exactly
    what matches it. Retrieving raw also makes this endpoint free, instant, and
    independent of any LLM key (``/ask`` keeps the LLM path for callers that
    want structured filters). The answer text comes from the matched intent's
    curated guidance, else a per-type fallback — no model needed.
    """
    where, order, params = build_search({"q": q}, sort="relevance")
    total = (await db.execute(text(f"SELECT count(*) FROM cbs_index{where}"), params)).scalar_one()
    params["limit"] = max(1, min(int(limit), 30))
    rows = (await db.execute(
        text(f"SELECT {RESULT_COLS} FROM cbs_index{where} ORDER BY {order} LIMIT :limit"), params
    )).mappings().all()
    results = [dict(r) for r in rows]

    top = results[0] if results else None
    atype = _classify(top, int(total))

    # The "understood" chips: the question's dimensions, parsed without an LLM,
    # + the locality entity from the gazetteer. Presentation-layer only — the
    # retrieval above runs on the raw question (measured better; see docstring).
    understood = parse_question(q)
    understood["geo_entity"] = _find_locality(q, await _load_gazetteer(db))

    # Availability-by-resolution over what was actually found — the community's
    # own answer format ("יש עד נפה, אין א"ס").
    matrix = geo_matrix(results, understood.get("geo_level"))

    caveats: list[str] = []
    geo_available = None
    if top:
        extra = top.get("extra") or {}
        geo_available = extra.get("geo_max")
        # An intent that documents a coarser ceiling than the finest level people
        # ask for is worth calling out — it is the single most common frustration
        # in the source chat ("the finest I found is נפה").
        if geo_available in ("נפה", "מחוז"):
            caveats.append(
                f"הרזולוציה הזמינה במקור זה היא {geo_available} — ייתכן שאין פילוח עדין יותר."
            )
        # Enrichment-derived caveats (migration 038): inclusion threshold and
        # boundary vintage — the two silent traps the benchmark surfaces.
        if top.get("geo_coverage"):
            caveats.append(f"שימו לב לכיסוי: {top['geo_coverage']}.")
        if top.get("geo_vintage"):
            caveats.append(f"היחידות הגאוגרפיות במקור זה לפי {top['geo_vintage']}.")
    req_lvl = understood.get("geo_level")
    if req_lvl and matrix and matrix.get(req_lvl) is False:
        finest = next((l for l in reversed(GEO_LADDER) if matrix.get(l)), None)
        if finest:
            caveats.append(
                f"התבקשה רמת {req_lvl}, אך במקורות שנמצאו הרזולוציה העדינה ביותר היא {finest}."
            )

    # Edition history: if the top result belongs to a yearly series, surface its
    # other editions ("מהדורות קודמות: 2021 · 2019 …") + flag a stale top hit.
    editions: list[dict] = []
    if top and top.get("series_key"):
        ed_rows = (await db.execute(text(
            f"SELECT title, url, edition_year, is_latest_edition FROM cbs_index "
            "WHERE series_key = :k AND url != :u "
            "ORDER BY edition_year DESC NULLS LAST, id DESC LIMIT 6"),
            {"k": top["series_key"], "u": top["url"]},
        )).mappings().all()
        editions = [dict(r) for r in ed_rows]
        if top.get("is_latest_edition") is False:
            newer = next((e for e in editions if e.get("is_latest_edition")), None)
            if newer:
                caveats.append(
                    f"קיימת מהדורה עדכנית יותר ({newer.get('edition_year')}): {newer.get('title')}."
                )

    intent_answer = (top.get("extra") or {}).get("answer") if top else None
    answer = (intent_answer if atype in ("guidance", "not_available") and intent_answer
              else _ANSWER_TEXT.get(atype, ""))

    primary = None
    if top:
        primary = {
            "title": top.get("title"),
            "url": top.get("url"),
            "link": _primary_link(top),
            "item_type": top.get("item_type"),
            "section": top.get("section"),
            "product_form": top.get("product_form"),
            "data_vintage": top.get("data_vintage") or top.get("edition_year"),
            "series_key": top.get("series_key"),
        }

    return {
        "answer": answer,
        "answer_type": atype,
        "provider": "index",  # no LLM in this path — retrieval is the index itself
        "primary": primary,
        "understood": understood,
        "geo_matrix": matrix,
        "editions": editions,
        "geo_available": geo_available,
        "caveats": caveats,
        "filters": {"query": q},
        "total": int(total),
        "results": results,
        "source": "over.org.il — אינדקס הלמ\"ס (cbs.gov.il)",
    }


@router.post("/resolve")
@limiter.limit("20/minute")
async def resolve(request: Request, body: AskRequest, db: AsyncSession = Depends(get_db)):
    """Free-text question → a structured, actionable CBS location."""
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")
    await _enforce_llm_budget(db)
    return await resolve_question(db, q, body.limit)
