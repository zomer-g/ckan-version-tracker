"""Query operators and match highlighting for "שאלות לעם".

One query string reaches two very different worlds, and that is the whole
problem this module exists to solve.

**TAG-IT understands operators natively.** Measured against scope 15 on
2026-08-11: `תקציב` → 7,384 hits, `תקציב הביטחון` → 4,072 (so the default
between words is AND), `"תקציב הביטחון"` → 234 (quotes really are a phrase),
`תקציב -הביטחון` → 3,312 — which is exactly 7,384 − 4,072, i.e. exclusion is
real. `OR` works too. Parentheses do **not** group (a grouped query returned
more hits than its own first clause), so we strip them rather than pass along
a syntax the backend silently mis-reads.

**The local sources understand nothing.** They are ILIKE / tsquery / substring
matchers. Handing them `"תקציב הביטחון"` searches for text containing the quote
characters, and `-הביטחון` searches for a literal minus sign — both return zero,
which the page would render as "nothing was published about this". So for those
we send a plain ANCHOR string the backend can actually match, and enforce the
operators ourselves on the rows that come back.

The trade is honest and worth naming: post-filtering can only narrow what the
source already returned, so a phrase that is rare inside a large result set may
be missed. We compensate by asking those sources for more rows when operators
are present. Recall degrades; correctness does not.

Highlighting is normalized the same way for both worlds: the gateway emits
``«…»`` around matched text — the convention TAG-IT already uses — so the
frontend has exactly one rendering rule regardless of which corpus answered.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field

# TAG-IT's own highlight markers. Reused for locally-generated highlights so the
# client never has to know which source a card came from.
MARK_OPEN = "«"
MARK_CLOSE = "»"

_TOKEN_RE = re.compile(r'-?"[^"]*"|\S+')


@dataclass(frozen=True)
class ParsedQuery:
    """A search box entry, taken apart."""
    raw: str
    phrases: tuple[str, ...] = ()     # quoted — must appear verbatim
    terms: tuple[str, ...] = ()       # bare words — all must appear
    excludes: tuple[str, ...] = ()    # -word / -"phrase" — must NOT appear
    dropped_grouping: bool = False    # parentheses were present and removed
    has_or: bool = False              # an explicit OR was used

    @property
    def has_operators(self) -> bool:
        return bool(self.phrases or self.excludes)

    @property
    def enforce_positives(self) -> bool:
        """Whether "every positive must appear" is a faithful reading.

        It is not, once OR is involved: `a OR b` means either will do, and
        requiring both would turn a widening operator into a narrowing one.
        Only TAG-IT can express OR, so for everyone else we stop enforcing the
        positives and keep only the exclusions, which stay valid either way.
        """
        return not self.has_or

    @property
    def positives(self) -> tuple[str, ...]:
        """Everything that must be present, phrases first (most selective)."""
        return self.phrases + self.terms

    def anchor(self, ) -> str:
        """The single string to hand a backend that cannot express operators.

        The most selective positive: a phrase if there is one (an exact run of
        text is far narrower than a word), else the longest bare term. Never an
        excluded term — asking a backend to find what we intend to reject would
        invert the query.
        """
        if self.phrases:
            return self.phrases[0]
        if self.terms:
            return max(self.terms, key=len)
        return self.raw.strip()


def parse(q: str) -> ParsedQuery:
    """Parse `"phrase"`, `-exclusion` and bare terms out of a query string."""
    raw = (q or "").strip()
    # Parentheses are not honoured by the backend that has operators, so they
    # are removed rather than forwarded — a grouping that silently does not
    # group is worse than one that is openly unsupported.
    stripped = raw.replace("(", " ").replace(")", " ")
    dropped_grouping = stripped != raw

    phrases: list[str] = []
    terms: list[str] = []
    excludes: list[str] = []
    has_or = False
    for tok in _TOKEN_RE.findall(stripped):
        neg = tok.startswith("-")
        body = tok[1:] if neg else tok
        quoted = len(body) >= 2 and body.startswith('"') and body.endswith('"')
        if quoted:
            body = body[1:-1]
        body = body.strip()
        if not body:
            continue
        if neg:
            excludes.append(body)
        elif quoted:
            phrases.append(body)
        else:
            # A bare "OR" is TAG-IT syntax, not a search term.
            if body.upper() == "OR":
                has_or = True
                continue
            terms.append(body)
    return ParsedQuery(raw=raw, phrases=tuple(phrases), terms=tuple(terms),
                       excludes=tuple(excludes), dropped_grouping=dropped_grouping,
                       has_or=has_or)


def native_query(pq: ParsedQuery) -> str:
    """The string to send a backend that parses operators itself.

    The user's own text, minus the grouping the backend cannot honour.
    """
    if not pq.dropped_grouping:
        return pq.raw
    return " ".join(pq.raw.replace("(", " ").replace(")", " ").split())


def matches(pq: ParsedQuery, *texts: str | None) -> bool:
    """Does this record satisfy the operators, judged on the text we can see?

    Case-insensitive substring test across every field supplied. Used only for
    sources that cannot express operators themselves.
    """
    hay = " \n".join(t for t in texts if t).lower()
    if not hay:
        return not pq.positives
    if pq.enforce_positives:
        for needle in pq.positives:
            if needle.lower() not in hay:
                return False
    elif pq.positives and not any(n.lower() in hay for n in pq.positives):
        # An OR query still has to match SOMETHING — just not everything.
        return False
    for needle in pq.excludes:
        if needle.lower() in hay:
            return False
    return True


# A single character matches inside almost every word, so highlighting it is
# noise rather than explanation — even though the match itself is real and the
# backend did find it. Matching is unaffected; only the marking is.
MIN_MARK_CHARS = 2


def _mark(text: str, needles: tuple[str, ...]) -> str:
    """Wrap every occurrence of each needle in the highlight markers."""
    needles = tuple(n for n in needles if n and len(n) >= MIN_MARK_CHARS)
    if not text or not needles:
        return text
    # Longest first, so "תקציב הביטחון" wins over "תקציב" and we do not mark
    # inside a marker we just inserted.
    pattern = "|".join(re.escape(n) for n in sorted(needles, key=len, reverse=True) if n)
    if not pattern:
        return text
    return re.sub(f"({pattern})", MARK_OPEN + r"\1" + MARK_CLOSE, text,
                  flags=re.IGNORECASE)


def snippet_around(pq: ParsedQuery, *texts: str | None, width: int = 220) -> str | None:
    """A window of text centred on the first match, with the match highlighted.

    This is what makes a result legible: the point of a hit in a 300-page
    protocol is the sentence it appears in, not the document's title. Returns
    None when nothing matches, so the caller can fall back to its own summary.
    """
    needles = pq.positives
    if not needles:
        return None
    for text in texts:
        if not text:
            continue
        flat = re.sub(r"\s+", " ", str(text)).strip()
        low = flat.lower()
        hit = -1
        found = ""
        for n in sorted(needles, key=len, reverse=True):
            i = low.find(n.lower())
            if i >= 0 and (hit < 0 or i < hit):
                hit, found = i, n
        if hit < 0:
            continue
        if len(flat) <= width:
            return _mark(flat, needles)
        half = max(0, (width - len(found)) // 2)
        start = max(0, hit - half)
        end = min(len(flat), start + width)
        start = max(0, end - width)
        out = flat[start:end]
        if start > 0:
            out = "…" + out
        if end < len(flat):
            out = out + "…"
        return _mark(out, needles)
    return None


def mark_all(pq: ParsedQuery, text: str | None) -> str:
    """Highlight every match in a short field (a title), leaving it whole.

    Unlike snippet_around this does not window the text — a title is already
    short, and cutting it would cost more than the highlight gains.
    """
    if not text or already_highlighted(text):
        return text or ""
    return _mark(text, pq.positives)


def already_highlighted(text: str | None) -> bool:
    """Did the source hand us its own highlighting? (TAG-IT does.)"""
    return bool(text) and MARK_OPEN in text
