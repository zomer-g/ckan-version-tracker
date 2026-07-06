"""Natural-language search over the CBS index.

``POST /api/cbs/ask`` takes a free-text question (Hebrew or English), asks Claude
to parse it into structured filters + a cleaned full-text query (grounded in the
live facets so it maps to real values), runs the SAME FTS search as
/api/cbs/search, and returns a one-line answer + the results. Backs the "שפה
חופשית" mode in the לץ הלמ"ס extension.

Requires ANTHROPIC_API_KEY (Render dashboard). Empty ⇒ 503 (feature off), so the
rest of the CBS API keeps working without it.
"""
from __future__ import annotations

import logging

from anthropic import AsyncAnthropic
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.rate_limit import limiter

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/cbs", tags=["cbs"])

MODEL = "claude-opus-4-8"

# Same projection the /api/cbs/search + MCP search return.
_COLS = (
    "url, lang, section, series, item_type, title, title_en, summary, "
    "subject_tags, year_start, year_end, geo_levels, file_types, extra, "
    "file_links, last_crawled"
)

# Claude fills this via a forced tool call (most SDK-version-robust structured
# output). Empty string / 0 mean "no constraint".
_PARSE_TOOL = {
    "name": "cbs_query",
    "description": "פירוק שאלה חופשית לחיפוש באינדקס הלמ\"ס.",
    "input_schema": {
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
    },
}

_SYSTEM = (
    "אתה עוזר חיפוש מעל אינדקס אתר הלשכה המרכזית לסטטיסטיקה (למ\"ס). המשתמש שואל "
    "שאלה חופשית; פרק אותה לפילטרים מובנים ולשאילתת מילות-מפתח, והפעל את הכלי "
    "cbs_query. חשוב: geo/file_type/section חייבים להיות ערך מדויק מתוך הרשימות "
    "שסופקו (או ריק). answer = משפט קצר וממוקד בעברית שמנחה איפה למצוא את המידע."
)


class AskRequest(BaseModel):
    q: str
    limit: int = 30


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


async def _run_search(db: AsyncSession, p: dict, limit: int) -> tuple[int, list[dict]]:
    conds, params = [], {}
    q = (p.get("query") or "").strip()
    if q:
        conds.append("(search_vector @@ plainto_tsquery('simple', :q) OR title ILIKE :qlike)")
        params["q"] = q
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
    order = ("ts_rank(search_vector, plainto_tsquery('simple', :q)) DESC, last_crawled DESC NULLS LAST"
             if q else "last_crawled DESC NULLS LAST, id DESC")
    params["limit"] = max(1, min(int(limit), 60))
    rows = (await db.execute(
        text(f"SELECT {_COLS} FROM cbs_index{where} ORDER BY {order} LIMIT :limit"), params
    )).mappings().all()
    return int(total), [dict(r) for r in rows]


@router.post("/ask")
@limiter.limit("20/minute")
async def ask(request: Request, body: AskRequest, db: AsyncSession = Depends(get_db)):
    """LLM-parsed natural-language search over the CBS index."""
    if not settings.anthropic_api_key:
        raise HTTPException(status_code=503, detail="natural-language search is not configured")
    q = (body.q or "").strip()
    if not q:
        raise HTTPException(status_code=400, detail="q is required")

    facets = await _facets(db)
    user = (
        f"שאלה: {q}\n\n"
        f"רמות גאוגרפיות זמינות: {', '.join(facets['geo_levels'])}\n"
        f"סוגי קבצים זמינים: {', '.join(facets['file_types'])}\n"
        f"סוגי עמודים (section): {', '.join(facets['sections'])}"
    )
    client = AsyncAnthropic(api_key=settings.anthropic_api_key)
    try:
        resp = await client.messages.create(
            model=MODEL, max_tokens=1024, system=_SYSTEM,
            tools=[_PARSE_TOOL], tool_choice={"type": "tool", "name": "cbs_query"},
            messages=[{"role": "user", "content": user}],
        )
    except Exception as e:  # noqa: BLE001
        logger.warning("cbs/ask LLM error: %s", e)
        raise HTTPException(status_code=502, detail="language model error")

    parsed = next((b.input for b in resp.content if getattr(b, "type", None) == "tool_use"), None)
    if not parsed:
        raise HTTPException(status_code=502, detail="could not parse the question")

    total, results = await _run_search(db, parsed, body.limit)
    return {
        "answer": parsed.get("answer") or "",
        "filters": {k: parsed.get(k) for k in
                    ("query", "geo", "file_type", "section", "year_from", "year_to")},
        "total": total,
        "results": results,
    }
