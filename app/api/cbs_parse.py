"""Deterministic question parser for /api/cbs/resolve — the "understood" chips.

Parses a free-text CBS question into the seven dimensions of the ultimate-
search-interface ontology (Lamas/cbs-ultimate-search-interface.md): geo level,
time (years / latest / series), product form, metric, population cuts and the
collection operation. NO LLM — retrieval on the raw question measured better
than LLM-cleaned keywords, and the chips must be free/instant on every query.
The locality-entity dimension needs the gazetteer table and is resolved by the
caller (async DB), then merged into the same "understood" dict.

Precision over recall, like cbs_enrich: a chip the user didn't mean erodes
trust in the whole "הבנתי:" row.
"""
from __future__ import annotations

import re

from app.services.cbs_enrich import derive_cuts, derive_metrics, derive_source_op

# Geo levels, coarse→fine. The ladder order powers the availability matrix and
# the "requested finer than available" caveat.
GEO_LADDER = (
    "ארצי", "מחוז", "נפה", "רשות מקומית", "מועצה אזורית", "יישוב",
    "שכונה", "אזור סטטיסטי", "רחוב",
)

_GEO_LEVEL_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"אזור(?:ים)?\s+סטטיסטי|\bא\"ס\b|אג\"ס|אזורים סטטיסטיים", "אזור סטטיסטי"),
    (r"רשו(?:ת|יות)\s+מקומי|עיריי?ה|מועצות מקומיות", "רשות מקומית"),
    (r"מועצ(?:ה|ות)\s+אזורי", "מועצה אזורית"),
    (r"לפי\s+יישוב|ברמת\s+יישוב|בכל\s+יישוב|יישובים", "יישוב"),
    (r"לפי\s+נפ(?:ה|ות)|ברמת\s+נפה|\bנפות\b", "נפה"),
    (r"לפי\s+מחוז|ברמת\s+מחוז|מחוזות", "מחוז"),
    (r"שכונ(?:ה|ות)", "שכונה"),
    (r"רחוב|כתובת|כתובות", "רחוב"),
    (r"ארצי|כלל[\s-]האוכלוסייה|בישראל כולה", "ארצי"),
)
_GEO_LEVEL_RES = [(re.compile(p), lvl) for p, lvl in _GEO_LEVEL_PATTERNS]

_PRODUCT_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"שכב(?:ה|ות|ת)|\bGIS\b|ממ\"ג|קואורדינט|פוליגונ|רסטר|shapefile|\bshp\b", "gis_layer"),
    (r"\bPUF\b|נתוני פרט|קובץ פרט", "puf"),
    (r"מחולל", "generator"),
    (r"דשבורד|לוח מחוונים", "dashboard"),
    (r"\bAPI\b", "api"),
    (r"מתודולוגי|הגדרות|שאלון", "methodology"),
    (r"אקסל|xlsx|קובץ להורדה|להוריד", "data_file"),
)
_PRODUCT_RES = [(re.compile(p, re.IGNORECASE), f) for p, f in _PRODUCT_PATTERNS]

_YEAR_RE = re.compile(r"(?<!\d)(19[4-9]\d|20[0-6]\d)(?!\d)")
_LATEST_RE = re.compile(r"עדכני|אחרון|האחרונ|מעודכן|חדש ביותר")
_SERIES_RE = re.compile(r"לאורך|היסטורי|רב.?שנתי|לכל שנה|שנה בשנה|כל השנים|סדרה עתית|משנת")


def parse_question(q: str) -> dict:
    """The deterministic dimensions of one question (no entity — see caller)."""
    geo_level = next((lvl for rx, lvl in _GEO_LEVEL_RES if rx.search(q)), None)
    product_form = next((f for rx, f in _PRODUCT_RES if rx.search(q)), None)
    years = sorted({int(y) for y in _YEAR_RE.findall(q)})
    understood = {
        "geo_level": geo_level,
        "years": years,
        "latest": bool(_LATEST_RE.search(q)),
        "series": bool(_SERIES_RE.search(q)),
        "product_form": product_form,
        "metrics": derive_metrics({"title": q}),
        "cuts": derive_cuts({"title": q}),
        "source_op": derive_source_op({"title": q}),
        "geo_entity": None,  # filled by the caller from the gazetteer
    }
    return understood


def geo_matrix(results: list[dict], requested: str | None) -> dict:
    """Availability-by-resolution over the top results.

    Returns {level: bool} for every ladder level that is either available in
    the results or explicitly requested — the chat community's own answer
    format ("יש עד רמת נפה, אין א\"ס")."""
    available: set[str] = set()
    for r in results:
        available.update(r.get("geo_levels") or [])
    out = {}
    for lvl in GEO_LADDER:
        if lvl in available or lvl == requested:
            out[lvl] = lvl in available
    return out
