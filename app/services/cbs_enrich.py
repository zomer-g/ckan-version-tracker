"""Derived (enriched) metadata for CBS index rows.

The ultimate-search-interface plan (Lamas/cbs-ultimate-search-interface.md)
identified metadata the benchmark shows users actually filter by — product
form, frequency, source operation, data vintage, series identity, boundary
vintage, coverage threshold, metric types and population cuts — none of which
the crawler emits directly. All of them, however, are derivable from fields
already in ``cbs_index`` (title / summary / extra / file links / item_type),
so enrichment runs server-side over existing rows: no re-crawl needed.

``enrich(row) -> dict`` is pure (no DB, no I/O) so it can run identically in
three places: the ingest path (new crawls get enriched on arrival), the
``POST /api/cbs/enrich`` backfill, and unit tests.

Everything here is heuristic text parsing. The rules prefer PRECISION over
recall: a missing value renders as "no tag" in the UI, while a wrong value
becomes a wrong filter result — so patterns only fire on unambiguous phrasing.
"""
from __future__ import annotations

import re
import unicodedata

# ── product_form ────────────────────────────────────────────────────────────
# The user-facing "what do I get" taxonomy: replaces the site-speak
# section/item_type split. Values (English codes; UI translates):
#   data_file / gis_layer / puf / generator / dashboard / api / database /
#   publication / methodology
# Intent rows (curated guidance) keep product_form NULL — they are pointers,
# not products.

_TOOL_FORMS = {
    "generator": "generator",
    "מחולל": "generator",
    "map_generator": "generator",
    "calculator": "generator",
    "dashboard": "dashboard",
    "api": "api",
    "database": "database",
    "data_series": "database",
    "data_topic": "database",
}

_GIS_RE = re.compile(
    r"שכב(?:ה|ות|ת)\s+(?:גאוגרפי|גיאוגרפי|GIS)|קטלוג השכבות|קבצים גאוגרפיים"
    r"|\bGIS\b|ממ\"ג|גבולות דיגיטלי|רסטר|shapefile|\.shp\b",
    re.IGNORECASE,
)
_PUF_RE = re.compile(r"\bPUF\b|קובץ נתונים לשימוש הציבור|קבצי נתונים לשימוש הציבור|נתוני פרט", re.IGNORECASE)
_METHOD_RE = re.compile(r"מתודולוגי|הגדרות והסברים|שאלון|סיווג אחיד|מילון מונחים|שיטות סטטיסטיות")


def derive_product_form(row: dict) -> str | None:
    item_type = row.get("item_type") or ""
    if item_type.startswith("intent"):
        return None
    if item_type in _TOOL_FORMS:
        return _TOOL_FORMS[item_type]
    if row.get("section") == "tools":
        return "generator"
    text = " ".join(filter(None, (row.get("title"), row.get("summary"))))
    labels = " ".join((fl.get("label") or "") for fl in (row.get("file_links") or []))
    if _PUF_RE.search(text) or _PUF_RE.search(labels):
        return "puf"
    if _GIS_RE.search(text) or _GIS_RE.search(labels):
        return "gis_layer"
    if item_type == "הגדרות והסברים" or _METHOD_RE.search(text):
        return "methodology"
    if row.get("section") == "databank":
        return "database"
    if any(ft in ("xlsx", "xls", "csv") for ft in (row.get("file_types") or [])):
        return "data_file"
    return "publication"


# ── freq (time-axis unit) ───────────────────────────────────────────────────
# Normalised Hebrew values (the whole CBS UI vocabulary is Hebrew — matching
# geo_levels): שנתי / רבעוני / חודשי / דו-שנתי / חד-פעמי.

_FREQ_CANON = {
    "שנתי": "שנתי", "שנתית": "שנתי",
    "רבעוני": "רבעוני", "רבעונית": "רבעוני",
    "חודשי": "חודשי", "חודשית": "חודשי",
    "דו-שנתי": "דו-שנתי", "דו שנתי": "דו-שנתי", "עתי": "חד-פעמי",
    "חד-פעמי": "חד-פעמי", "חד פעמי": "חד-פעמי",
}


def derive_freq(row: dict) -> str | None:
    for v in ((row.get("extra") or {}).get("interval") or []):
        canon = _FREQ_CANON.get(str(v).strip())
        if canon:
            return canon
    title = row.get("title") or ""
    for k, canon in (("ממוצעים חודשיים", "חודשי"), ("נתונים חודשיים", "חודשי"),
                     ("נתונים רבעוניים", "רבעוני")):
        if k in title:
            return canon
    return None


# ── source_op (collection operation) ────────────────────────────────────────
# The named census / survey / register behind the data. Benchmark users say it
# explicitly ("PUF סקר הוצאות 2022", "מהמפקד"). Closed list, first match wins —
# ordered so the census (the strongest brand) beats a generic "סקר".

_SOURCES: tuple[tuple[str, str], ...] = (
    (r"מפקד\s+חקלאות", "מפקד חקלאות"),
    (r"מפקד(?:\s+ה?אוכלוסי\w*)?(?:\s+\d{4})?", "מפקד אוכלוסין"),
    (r"סקר\s+כ(?:ו)?ח\s+אדם", "סקר כוח אדם"),
    (r"ה?סקר\s+ה?חברתי", "הסקר החברתי"),
    (r"סקר\s+הוצאות\s+משק\s+ה?בית", "סקר הוצאות משק הבית"),
    (r"סקר\s+הכנסות", "סקר הכנסות"),
    (r"סקר\s+אמון\s+ה?צרכנים", "סקר אמון הצרכנים"),
    (r"סקר\s+מיומנויות", "סקר מיומנויות בוגרים (PIAAC)"),
    (r"סקר\s+בריאות", "סקר בריאות"),
    (r"מרשם\s+ה?אוכלוסין", "מרשם האוכלוסין"),
    (r"מרשם\s+דירות", "מרשם דירות ומבנים"),
)
_SOURCE_RES = [(re.compile(p), name) for p, name in _SOURCES]


def derive_source_op(row: dict) -> str | None:
    extra = row.get("extra") or {}
    surveys = extra.get("surveys") or []
    if surveys:
        # The crawler's managed-metadata term is authoritative when present.
        return str(surveys[0]).strip() or None
    text = " ".join(filter(None, (row.get("title"), row.get("summary"))))
    for rx, name in _SOURCE_RES:
        if rx.search(text):
            return name
    return None


# ── data_vintage (year of the DATA, not of the publication) ────────────────
# "קובץ הרשויות המקומיות 2021" published in 2023: year_end says 2021 when the
# crawler parsed it, but many titles carry "סוף 2022" / a range whose max is
# the real vintage. Falls back to year_end at the call site, not here.

_YEAR_RE = re.compile(r"(?<!\d)(19[4-9]\d|20[0-6]\d)(?!\d)")


def derive_data_vintage(row: dict) -> int | None:
    years = [int(y) for y in _YEAR_RE.findall(row.get("title") or "")]
    # Titles like "אוקטובר 2022-ספטמבר 2023": the vintage is the max year.
    if years:
        return max(years)
    return None


# ── geo_vintage (boundary vintage — the recurring join trap) ───────────────

def derive_geo_vintage(row: dict) -> str | None:
    text = " ".join(filter(None, (row.get("title"), row.get("summary"))))
    m = re.search(r"גבולות\s+(?:דצמבר\s+)?(\d{4})", text)
    if m:
        return f'א"ס {m.group(1)}'
    if re.search(r"אזורי\s+סקר", text):
        return "אזורי סקר"
    m = re.search(r"אפיון[^.]{0,40}?(\d{4})[^.]{0,30}?גבולות|בגבולות\s+(\d{4})", text)
    if m:
        return f'א"ס {m.group(1) or m.group(2)}'
    return None


# ── geo_coverage (which entities the data actually includes) ───────────────
# "…ביישובים שבהם 5,000 תושבים ויותר" — the answer to "why isn't my town in
# the file". Stored as the human-readable matched phrase.

_COVERAGE_RES = (
    re.compile(r"יישובים\s+(?:שבהם|המונים|בני)\s+([\d,]+)\s+תושבים\s+ויותר"),
    re.compile(r"יישובים\s+של\s+([\d,]+)\+?\s+תושבים"),
)


def derive_geo_coverage(row: dict) -> str | None:
    text = " ".join(filter(None, (row.get("title"), row.get("summary"))))
    for rx in _COVERAGE_RES:
        m = rx.search(text)
        if m:
            return f"יישובים {m.group(1)}+ תושבים בלבד"
    if "ערים הגדולות" in text or "הערים הגדולות" in text:
        return "הערים הגדולות בלבד"
    return None


# ── series identity ────────────────────────────────────────────────────────
# Yearly editions of the same product ("קובץ הרשויות המקומיות בישראל 2021" /
# "…2019" / "…2017") share a series_key = title stripped of every volatile
# token (years, months, edition numbers, punctuation). The frontend already
# proved this heuristic on pinned cards (cbsSeries.ts) — this moves it into
# the DB so "latest edition only" can be a real filter.

_MONTHS = (
    "ינואר", "פברואר", "מרץ", "אפריל", "מאי", "יוני", "יולי", "אוגוסט",
    "ספטמבר", "אוקטובר", "נובמבר", "דצמבר",
)
_EDITION_RE = re.compile(r"מס(?:פר|')\s*\d+|כרך\s*\d+|חלק\s*[אבגד]\b")
_MONTH_RE = re.compile("|".join(_MONTHS))
_NONWORD_RE = re.compile(r"[^\w֐-׿]+", re.UNICODE)


def derive_series_key(row: dict) -> str | None:
    title = row.get("title") or ""
    if not title or (row.get("item_type") or "").startswith("intent"):
        return None
    t = unicodedata.normalize("NFKC", title)
    t = _YEAR_RE.sub(" ", t)
    t = _EDITION_RE.sub(" ", t)
    t = _MONTH_RE.sub(" ", t)
    t = _NONWORD_RE.sub(" ", t).strip().lower()
    # A key that lost all its substance ("2021" → "") identifies nothing.
    if len(t) < 4:
        return None
    return " ".join(t.split())


def derive_edition_year(row: dict) -> int | None:
    return derive_data_vintage(row) or row.get("year_end") or row.get("year_start")


# ── metrics (statistical measure types present) ────────────────────────────
# English codes stored, Hebrew rendered by the UI: the benchmark shows users
# distinguish "שכר חציוני" from "ממוצע" explicitly. Title+summary only —
# full_text on hub pages mentions everything and would destroy precision.

_METRIC_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"ממוצע", "avg"),
    (r"חציוני|חציון", "median"),
    (r"אחוז|שיעור|אחוזים", "pct"),
    (r"מדד(?:י)?\b", "index"),
    (r"התפלגות|פילוח", "distribution"),
    (r"סה\"כ|סך הכל|מספר\s", "count"),
)
_METRIC_RES = [(re.compile(p), code) for p, code in _METRIC_PATTERNS]


def derive_metrics(row: dict) -> list[str]:
    text = " ".join(filter(None, (row.get("title"), row.get("summary"))))
    found = []
    for rx, code in _METRIC_RES:
        if rx.search(text) and code not in found:
            found.append(code)
    return found


# ── cuts (population breakdowns available) ─────────────────────────────────

_CUT_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"\bגיל\b|קבוצות גיל|לפי גיל|ילדים|קשישים|אזרחים ותיקים|בני נוער", "age"),
    (r"\bמין\b|מגדר|נשים וגברים|גברים ונשים", "gender"),
    (r"חרדי|ערבי|יהודי|דתיות|מגזר|בדואי|דרוזי", "sector_religion"),
    (r"עולים|עלייה|הגירה|מהגרים", "immigration"),
    (r"השכלה|תואר|אקדמ|בגרות", "education"),
    (r"ענף(?:י)?\s+כלכל|משלח\s+יד|ענפי\s+", "industry"),
    (r"אשכול|עשירונ|חמישונ|סוציו[\s-]?אקונומי", "ses"),
)
_CUT_RES = [(re.compile(p), code) for p, code in _CUT_PATTERNS]


def derive_cuts(row: dict) -> list[str]:
    text = " ".join(filter(None, (
        row.get("title"), row.get("summary"),
        " ".join(row.get("subject_tags") or []),
    )))
    found = []
    for rx, code in _CUT_RES:
        if rx.search(text) and code not in found:
            found.append(code)
    return found


# ── geo_levels completion ──────────────────────────────────────────────────
# The crawler's managed-metadata geo terms are sparse (most publication rows
# have none) while the title says it outright ("…לפי יישוב"). Adds only
# unambiguous title/summary/file-label evidence; existing values are kept.
# Vocabulary matches the live facets (Hebrew).

_GEO_PATTERNS: tuple[tuple[str, str], ...] = (
    (r"אזור(?:ים)?\s+סטטיסטי|\bא\"ס\b|אזורים סטטיסטיים", "אזור סטטיסטי"),
    (r"רשו(?:ת|יות)\s+מקומי", "רשות מקומית"),
    (r"מועצ(?:ה|ות)\s+אזורי", "מועצה אזורית"),
    (r"לפי\s+יישוב|ביישובים|יישובים\b", "יישוב"),
    (r"לפי\s+נפ(?:ה|ות)|\bנפות\b|\bבנפ(?:ה|ות)\b", "נפה"),
    (r"לפי\s+מחוז|מחוזות", "מחוז"),
    (r"שכונ(?:ה|ות)", "שכונה"),
    (r"רחוב(?:ות)?\b", "רחוב"),
    (r"מטרופולין", "מטרופולין"),
)
_GEO_RES = [(re.compile(p), level) for p, level in _GEO_PATTERNS]


def derive_geo_levels(row: dict) -> list[str] | None:
    existing = list(row.get("geo_levels") or [])
    text = " ".join(filter(None, (
        row.get("title"), row.get("summary"),
        " ".join((fl.get("label") or "") for fl in (row.get("file_links") or [])),
    )))
    for rx, level in _GEO_RES:
        if level not in existing and rx.search(text):
            existing.append(level)
    return existing or None


# ── entry point ────────────────────────────────────────────────────────────

def enrich(row: dict) -> dict:
    """All derived columns for one cbs_index row (as a plain dict).

    Returns the column→value mapping the caller writes back. Never raises on
    weird input — every deriver treats missing fields as absent evidence.
    """
    return {
        "product_form": derive_product_form(row),
        "freq": derive_freq(row),
        "source_op": derive_source_op(row),
        "data_vintage": derive_data_vintage(row),
        "geo_vintage": derive_geo_vintage(row),
        "geo_coverage": derive_geo_coverage(row),
        "series_key": derive_series_key(row),
        "edition_year": derive_edition_year(row),
        "metrics": derive_metrics(row),
        "cuts": derive_cuts(row),
        "geo_levels": derive_geo_levels(row),
    }
