"""The free fast-path: Hebrew question patterns that resolve WITHOUT an LLM.

A large share of what people type at a data console is one of a handful of
shapes — "כמה X", "X לפי Y", "X ב<יישוב>", "ממוצע Y לפי Z". Those do not need a
language model: the semantic model already declares every entity, dimension and
sample value, so the same token matching that powers retrieval can fill the
query directly. Every question answered here costs nothing, returns instantly,
and cannot hallucinate.

DEVIATION FROM THE PLAN, ON PURPOSE. The plan described this layer as
"templates + an LLM router" — a cheap model picking a template and filling
slots. Building it made clear the LLM adds nothing at this position: the
semantic layer behind it (semantic_model.py) already accepts free phrasing and
validates its own output, so a router in front would spend a call to reach the
same place with strictly less coverage. So this layer is fully deterministic and
free, and the semantic layer is the only path that spends a token. Same two
tiers, one fewer paid call.

THE GATE THAT MAKES THIS SAFE: a template only fires when every content token in
the question was consumed by something we recognized — an entity, a dimension, a
value, or a known filler word. A partial match is treated as no match and falls
through to the LLM. Without that rule, "כמה עסקים נסגרו בתל אביב ב-2023" would
quietly answer "כמה עסקים בתל אביב" and drop the part it did not understand —
which is the wrong-number failure this whole design exists to avoid.
"""
from __future__ import annotations

import re

from app.services.semantic_model import _HEB_PREFIXES, norm_token, retrieve, score_entity, tokens

# Words that carry no content: question scaffolding, prepositions, connectives.
# Consumed for free by the coverage gate below.
_STOPWORDS = {
    norm_token(w) for w in (
        "כמה", "מה", "מהו", "מהי", "מי", "איפה", "איזה", "אילו", "האם", "יש", "ישנם",
        "כל", "של", "את", "עם", "לפי", "בפילוח", "פילוח", "לכל", "הכי", "ביותר",
        "רשימה", "רשימת", "הצג", "הצגי", "תן", "תני", "תראה", "מספר", "כמות", "סך",
        "הכול", "הכל", "סהכ", "בשנת", "בשנה", "שנת", "שנה", "בין", "או", "וגם", "גם",
        "היו", "היה", "הם", "הן", "זה", "זו", "אלה", "נא", "בבקשה", "טבלה", "טבלת",
        "נתונים", "מידע", "מאגר", "רשומות", "שורות", "עשרת", "חמשת", "top", "list",
        "how", "many", "what", "which", "show", "the", "of", "by", "in", "for", "and",
        "המובילים", "מובילים", "הראשונים", "ראשונים", "הגדולים", "גדולים",
    )
}


def _covered(tok: str, consumed: set[str]) -> bool:
    """Is this question token accounted for?

    ``tokens`` emits both readings of a possible Hebrew clitic ("בחיפה" → both
    "בחיפה" and "חיפה"), and what gets consumed is whichever reading matched
    something in the model — usually the stripped one. Checking membership on
    the raw form alone would leave the other reading dangling and fail the
    coverage gate on every question that uses a preposition, i.e. most of them.
    """
    if tok in consumed:
        return True
    return len(tok) >= 3 and tok[0] in _HEB_PREFIXES and tok[1:] in consumed

# Aggregation intents. Order matters — "ממוצע" must win before the generic count.
_AGG_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"ממוצע|ממוצעת|בממוצע|average|avg"), "avg"),
    (re.compile(r"סכום|סך הכל|סה\"?כ|total|sum"), "sum"),
    (re.compile(r"מקסימ|הגבוה ביותר|הגדול ביותר|max"), "max"),
    (re.compile(r"מינימ|הנמוך ביותר|הקטן ביותר|min"), "min"),
]

# "לפי X" / "בפילוח לפי X" — the grouping marker. Everything after it, up to the
# next clause boundary, names the dimension.
_GROUP_RE = re.compile(r"(?:בפילוח\s+)?לפי\s+(.+?)(?:\s+(?:ב|החל|עד|בין|כאשר|מ)\b|$)")
_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_TOPN_RE = re.compile(r"\b(\d{1,3})\s*(?:המובילים|הראשונים|top)\b|\btop\s*(\d{1,3})\b")

# An entity has to be a clearly better match than the runner-up before we answer
# without a model. Ties are exactly the case where a human would ask "which
# dataset did you mean?" — so we let the LLM, which can weigh the whole
# question, decide instead of guessing.
_MIN_SCORE = 6.0
_MIN_MARGIN = 1.35


def _best_entity(model: list[dict], question: str) -> dict | None:
    cands = retrieve(model, question, k=3)
    if not cands:
        return None
    q = tokens(question)
    top = score_entity(cands[0], q)
    if top < _MIN_SCORE:
        return None
    if len(cands) > 1:
        second = score_entity(cands[1], q)
        if second > 0 and top < second * _MIN_MARGIN:
            return None
    return cands[0]


def _match_dimension(entity: dict, phrase: str, *, groupable: bool) -> dict | None:
    """The declared dimension a phrase names, by normalized token overlap."""
    want = set(tokens(phrase))
    if not want:
        return None
    best, best_score = None, 0.0
    for d in entity["dimensions"]:
        if groupable and not d.get("groupable"):
            continue
        have = set(tokens(d["key"])) | set(tokens(d.get("title") or ""))
        if not have:
            continue
        overlap = len(want & have)
        if not overlap:
            continue
        # Prefer the dimension whose own name is most fully covered — "מחוז"
        # should beat "מחוז_מגורים_קודם" for the phrase "מחוז".
        score = overlap + overlap / len(have)
        if score > best_score:
            best, best_score = d, score
    return best


def _match_value(entity: dict, q_tokens: set[str]) -> tuple[dict, str] | None:
    """A (dimension, value) whose stored sample value the question mentions.

    Matching against real ``top_values`` rather than a guessed string is what
    keeps a Hebrew filter from returning zero rows for a spelling the dataset
    does not use — the value-linking half of the non-English accuracy gap."""
    for d in entity["dimensions"]:
        for v in d.get("samples", []):
            vt = set(tokens(str(v)))
            if vt and vt <= q_tokens:
                return d, str(v)
    return None


def match(model: list[dict], question: str) -> dict | None:
    """Question → a query dict for semantic_model.validate_query, or None.

    None means "no confident deterministic reading" and the caller falls through
    to the LLM. Returning None is cheap; returning a wrong query is not."""
    entity = _best_entity(model, question)
    if entity is None:
        return None

    q_tokens = set(tokens(question))
    consumed: set[str] = set(_STOPWORDS)
    consumed |= set(tokens(entity["title"]))
    consumed |= {t for s in entity.get("synonyms", []) for t in tokens(s)}

    query: dict = {"entity": entity["key"], "measures": [], "dimensions": [],
                   "filters": [], "enrich": [], "limit": 50}

    # ── grouping: "לפי X" ────────────────────────────────────────────────
    gm = _GROUP_RE.search(question)
    if gm:
        dim = _match_dimension(entity, gm.group(1), groupable=True)
        if dim is None:
            return None  # an explicit "לפי" we could not resolve is a hard miss
        query["dimensions"] = [dim["key"]]
        consumed |= set(tokens(gm.group(1)))

    # ── measure ──────────────────────────────────────────────────────────
    agg = next((a for rx, a in _AGG_PATTERNS if rx.search(question)), None)
    if agg:
        numeric = [d for d in entity["dimensions"] if d["kind"] == "number"]
        target = None
        for d in numeric:
            if set(tokens(d["key"])) & q_tokens or set(tokens(d.get("title") or "")) & q_tokens:
                target = d
                break
        if target is None:
            if len(numeric) != 1:
                return None  # "ממוצע" of what? ambiguous ⇒ let the model read it
            target = numeric[0]
        query["measures"] = [f'{agg}:{target["key"]}']
        consumed |= set(tokens(target["key"])) | set(tokens(target.get("title") or ""))
        # Consume the words the regex actually matched, not a reconstruction of
        # its pattern — the pattern is an alternation and picking it apart to
        # find which branch fired is how this drifts out of sync.
        for rx, a in _AGG_PATTERNS:
            m = rx.search(question)
            if a == agg and m:
                consumed |= set(tokens(m.group(0)))
    else:
        query["measures"] = ["count"]

    # ── year filter ──────────────────────────────────────────────────────
    ym = _YEAR_RE.search(question)
    if ym:
        year = ym.group(1)
        ydim = next(
            (d for d in entity["dimensions"]
             if d["kind"] in ("number", "date")
             and re.search(r"year|שנה|שנת|תאריך|date", d["key"], re.IGNORECASE)),
            None)
        if ydim is None:
            return None  # a year was asked for and the table has nowhere to put it
        if ydim["kind"] == "date":
            query["filters"].append({"field": ydim["key"], "op": "between",
                                     "value": [f"{year}-01-01", f"{year}-12-31"]})
        else:
            query["filters"].append({"field": ydim["key"], "op": "=", "value": int(year)})
        consumed.add(year)
        consumed |= set(tokens(ydim["key"]))

    # ── value filter from real sample values ─────────────────────────────
    hit = _match_value(entity, q_tokens)
    if hit:
        dim, val = hit
        query["filters"].append({"field": dim["key"], "op": "=", "value": val})
        consumed |= set(tokens(val)) | set(tokens(dim["key"]))

    # ── top-N ────────────────────────────────────────────────────────────
    tm = _TOPN_RE.search(question)
    if tm:
        n = int(tm.group(1) or tm.group(2))
        query["limit"] = max(1, min(n, 500))
        consumed |= set(tokens(tm.group(0)))

    query["order"] = {"by": "measure" if query["dimensions"] else "", "dir": "desc"}

    # ── the coverage gate ────────────────────────────────────────────────
    # Everything the user typed must have been accounted for. A leftover content
    # token means there is a constraint in the question that this query does not
    # express, and answering anyway would silently widen the result.
    leftover = {t for t in q_tokens
                if len(t) > 2 and not t.isdigit() and not _covered(t, consumed)}
    if leftover:
        return None

    # A bare "כמה X" with no grouping and no filter is a single number. That is
    # a legitimate answer, but only when the question really was that short —
    # the gate above already guaranteed it.
    return query
