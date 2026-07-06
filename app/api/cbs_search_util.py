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


def or_tsquery(q: str) -> str:
    """Turn free text into an OR-of-words ``to_tsquery`` string with prefixes.

    Returns "" when the input has no usable word characters — callers should then
    fall back to an ILIKE match (or skip the text condition).
    """
    words = [w for w in _WORD_RE.split(q or "") if w]
    return " | ".join(f"{w}:*" for w in words)
