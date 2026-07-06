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
"""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.cbs_search_util import or_tsquery
from app.config import settings
from app.database import get_db
from app.rate_limit import limiter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cbs", tags=["cbs"])

ANTHROPIC_MODEL = "claude-opus-4-8"
DEEPSEEK_MODEL = "deepseek-chat"
DEEPSEEK_BASE_URL = "https://api.deepseek.com"

# Same projection the /api/cbs/search + MCP search return.
_COLS = (
    "url, lang, section, series, item_type, title, title_en, summary, "
    "subject_tags, year_start, year_end, geo_levels, file_types, extra, "
    "file_links, last_crawled"
)

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
    "שאלה חופשית; פרק אותה לפילטרים מובנים ולשאילתת מילות-מפתח. חשוב: "
    "geo/file_type/section חייבים להיות ערך מדויק מתוך הרשימות שסופקו (או ריק). "
    "answer = משפט קצר וממוקד בעברית שמנחה איפה למצוא את המידע."
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
    conds, params = [], {}
    q = (p.get("query") or "").strip()
    tsq = or_tsquery(q) if q else ""
    if q:
        if tsq:
            conds.append("(search_vector @@ to_tsquery('simple', :tsq) OR title ILIKE :qlike)")
            params["tsq"] = tsq
        else:
            conds.append("title ILIKE :qlike")
        params["qlike"] = f"%{q}%"
    if p.get("geo"):
        conds.append("geo_levels @> :geo"); params["geo"] = f'["{p["geo"]}"]'
    if p.get("file_type"):
        conds.append("file_types @> :ftype"); params["ftype"] = f'["{p["file_type"]}"]'
    if p.get("section"):
        conds.append("section = :section"); params["section"] = p["section"]
    if p.get("year_from"):
        conds.append("(year_end IS NULL OR year_end >= :yfrom)"); params["yfrom"] = int(p["year_from"])
    if p.get("year_to"):
        conds.append("(year_start IS NULL OR year_start <= :yto)"); params["yto"] = int(p["year_to"])
    where = (" WHERE " + " AND ".join(conds)) if conds else ""

    total = (await db.execute(text(f"SELECT count(*) FROM cbs_index{where}"), params)).scalar_one()
    order = ("ts_rank(search_vector, to_tsquery('simple', :tsq)) DESC, last_crawled DESC NULLS LAST"
             if tsq else "last_crawled DESC NULLS LAST, id DESC")
    params["limit"] = max(1, min(int(limit), 60))
    rows = (await db.execute(
        text(f"SELECT {_COLS} FROM cbs_index{where} ORDER BY {order} LIMIT :limit"), params
    )).mappings().all()
    return int(total), [dict(r) for r in rows]


@router.post("/ask")
@limiter.limit("20/minute")
async def ask(request: Request, body: AskRequest, db: AsyncSession = Depends(get_db)):
    """LLM-parsed natural-language search over the CBS index."""
    provider = _provider()
    if not provider:
        raise HTTPException(status_code=503, detail="natural-language search is not configured")
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")

    facets = await _facets(db)
    try:
        if provider == "deepseek":
            parsed = await _parse_deepseek(q, facets)
        else:
            parsed = await _parse_anthropic(q, facets)
    except Exception as e:  # noqa: BLE001
        logger.warning("cbs/ask LLM error (%s): %s", provider, e)
        raise HTTPException(status_code=502, detail="language model error")

    if not parsed:
        raise HTTPException(status_code=502, detail="could not parse the question")

    total, results = await _run_search(db, parsed, body.limit)
    return {
        "answer": parsed.get("answer") or "",
        "provider": provider,
        "filters": {k: parsed.get(k) for k in
                    ("query", "geo", "file_type", "section", "year_from", "year_to")},
        "total": total,
        "results": results,
    }
