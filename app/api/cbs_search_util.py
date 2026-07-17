"""Shared full-text query helper for the CBS index search.

The index tsvector uses the ``simple`` config (no stemming — right for Hebrew).
``plainto_tsquery`` ANDs every word, so a natural-language phrase like
"קובץ יישובים לשנת 2022" matches nothing unless one row contains all four words.
``or_tsquery`` instead builds an OR-of-words query with prefix matching, so any
word can match and ``ts_rank`` floats the rows matching the most words to the top.
"""
from __future__ import annotations

import re

# \w under UNICODE includes Hebrew letters + digits (and underscore); this splits
# on whitespace and punctuation, dropping tsquery operator characters that would
# otherwise make to_tsquery raise.
_WORD_RE = re.compile(r"[^\w]+", re.UNICODE)

# Hebrew/English function words + domain-generic words. A long natural-language
# question ("האם יש לי דרך להגיע לנתונים כמה תושבים…") is otherwise OR'd word by
# word, and each common word matches tens of thousands of rows. Dropping these
# leaves the meaningful terms so ts_rank surfaces the right rows. The 'simple'
# tsvector config does no stemming, so we match surface forms.
_STOPWORDS = {
    # question / function words
    "האם", "יש", "אין", "לי", "לו", "לה", "להם", "אני", "אתה", "את", "אתם",
    "אנחנו", "הוא", "היא", "הם", "הן", "זה", "זו", "זאת", "אלה", "אלו",
    "מה", "מי", "איך", "כיצד", "איפה", "היכן", "למה", "מדוע", "כמה", "מתי",
    "של", "עם", "על", "אל", "אצל", "בין", "כמו", "כדי", "בגלל", "לפי",
    "או", "גם", "אם", "כי", "אבל", "רק", "כל", "כן", "לא", "עוד", "כבר",
    "דרך", "להגיע", "למצוא", "לקבל", "רוצה", "צריך", "ניתן", "אפשר", "יכול",
    "יכולה", "האם", "נתונים", "נתון", "מידע", "מספר", "כמות", "רשימה",
    # English
    "the", "a", "an", "of", "to", "in", "on", "for", "and", "or", "is",
    "are", "how", "what", "where", "when", "can", "do", "i", "data", "list",
}


# Single-letter Hebrew prefixes (bikhlam + article/relative). A word like
# "לנתונים" is ל+נתונים; stripping one prefix lets us catch the stopword under it.
_PREFIXES = set("לבהושמכ")


def _is_stopword(w: str) -> bool:
    lw = w.lower()
    if lw in _STOPWORDS or w in _STOPWORDS:
        return True
    # One leading prefix letter off, e.g. לנתונים → נתונים, במידע → מידע.
    if len(w) > 1 and w[0] in _PREFIXES and w[1:] in _STOPWORDS:
        return True
    return False


def or_tsquery(q: str) -> str:
    """Turn free text into an OR-of-words ``to_tsquery`` string with prefixes.

    Stopwords (prefix-aware) and single-character tokens are dropped so long
    questions rank on their meaningful terms — a lone "ל:*" would prefix-match
    almost the whole table. Returns "" when nothing usable remains — callers
    should then fall back to an ILIKE match (or skip the text condition).
    """
    words = [w for w in _WORD_RE.split(q or "") if w]
    kept = [w for w in words if len(w) > 1 and not _is_stopword(w)]
    # If the query was *all* stopwords/noise, keep multi-char originals rather
    # than match everything; last resort, keep whatever there was.
    terms = kept or [w for w in words if len(w) > 1] or words
    return " | ".join(f"{w}:*" for w in terms)


# ── Shared search SQL builder ───────────────────────────────────────────────
# One place that turns a filter dict into (WHERE, ORDER BY, params) so the three
# read paths — REST /search, /ask, and the MCP search tool — behave identically.
# Before this, the MCP used plainto_tsquery (AND-of-words) and lacked a ``lang``
# filter, so the same Hebrew question ranked differently through MCP than through
# REST. See app/api/cbs.py, app/api/cbs_ask.py, app/mcp/cbs_server.py.

# High-volume navigational index pages: "פעולות ופרסומים סטטיסטיים חדשים בישראל
# <חודש> <שנה>" summarize every new publication, so their full_text matches (and
# out-ranks) almost any query. They are a table of contents, not a data page —
# demoted below real content in relevance ranking. LIKE prefix, so title starting
# with the phrase is caught regardless of the trailing month/year.
_CATCHALL_TITLE_PREFIXES = ("פעולות ופרסומים סטטיסטיים",)

# Columns every read path returns — keep in one place so projections can't drift.
RESULT_COLS = (
    "url, lang, section, series, item_type, title, title_en, summary, "
    "subject_tags, year_start, year_end, geo_levels, file_links, file_types, "
    "extra, last_crawled"
)

# Which filter keys build_search understands. ``q`` is free text; the rest are
# exact-match facets. ``item_type``/``lang`` are honored by every caller now
# (the MCP previously ignored ``lang``).
_FACET_JSONB = {"subject": ("subject_tags", None), "geo": ("geo_levels", None),
                "file_type": ("file_types", None)}


def build_search(filters: dict, sort: str = "relevance") -> tuple[str, str, dict]:
    """Return (where_sql, order_sql, params) for a cbs_index search.

    ``filters`` keys (all optional): q, subject, geo, file_type, section,
    item_type, lang, year_from, year_to. ``sort`` is "relevance" or "chrono".

    Relevance ordering, in priority order:
      1. intent guidance rows first (the curated question→source layer),
      2. navigational catch-all index pages last,
      3. ts_rank of the OR-of-words query,
      4. newest data year, then most-recently crawled.
    The recency tie-breakers (3→4) fix the "old publication out-ranks the current
    one" failure — e.g. 'הבינוי בישראל 2004' floating above the 2024 release.
    """
    conds: list[str] = []
    params: dict = {}

    q = (filters.get("q") or "").strip()
    tsq = or_tsquery(q) if q else ""
    if q:
        if tsq:
            conds.append("(search_vector @@ to_tsquery('simple', :tsq) OR title ILIKE :qlike)")
            params["tsq"] = tsq
        else:
            conds.append("title ILIKE :qlike")
        params["qlike"] = f"%{q}%"

    for key, (col, _) in _FACET_JSONB.items():
        val = filters.get(key)
        if val:
            conds.append(f"{col} @> :{key}")
            params[key] = f'["{val}"]'
    if filters.get("section"):
        conds.append("section = :section"); params["section"] = filters["section"]
    if filters.get("item_type"):
        conds.append("item_type = :item_type"); params["item_type"] = filters["item_type"]
    if filters.get("lang"):
        conds.append("lang = :lang"); params["lang"] = filters["lang"]
    if filters.get("year_from"):
        conds.append("(year_end IS NULL OR year_end >= :yfrom)"); params["yfrom"] = int(filters["year_from"])
    if filters.get("year_to"):
        conds.append("(year_start IS NULL OR year_start <= :yto)"); params["yto"] = int(filters["year_to"])

    where = (" WHERE " + " AND ".join(conds)) if conds else ""

    if sort == "chrono":
        order = ("coalesce(year_end, year_start) DESC NULLS LAST, "
                 "last_crawled DESC NULLS LAST, id DESC")
        return where, order, params

    # relevance
    catch_terms = []
    for i, pfx in enumerate(_CATCHALL_TITLE_PREFIXES):
        pk = f"catch{i}"
        catch_terms.append(f"title LIKE :{pk}")
        params[pk] = f"{pfx}%"
    catchall = " OR ".join(catch_terms) if catch_terms else "false"

    rank = (f"ts_rank(search_vector, to_tsquery('simple', :tsq))"
            if tsq else "0")
    order = (
        "(item_type = 'intent') DESC, "        # curated guidance rows first
        f"({catchall}) ASC, "                  # navigational index pages last
        f"{rank} DESC, "
        "coalesce(year_end, year_start) DESC NULLS LAST, "
        "last_crawled DESC NULLS LAST, id DESC"
    )
    return where, order, params
