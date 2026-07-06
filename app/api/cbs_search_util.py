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
