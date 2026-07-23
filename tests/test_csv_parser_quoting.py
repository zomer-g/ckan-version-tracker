"""Regression tests for CSV quoting in app.services.csv_parser.

``parse_csv`` used to hand the whole dialect to ``csv.Sniffer``, which also
*guesses* the quoting rules — from an 8 KB sample. When it inferred
``doublequote=False``, an RFC-4180 field like ``"א""ב"`` parsed as ``א"ב"``:
the escaped inner quote survived but a stray quote was appended.

This was found in production. Two consecutive versions of the same dataset,
written by the same code, parsed differently — v1 gained 17 corrupted values,
v2 none — purely because their first 8 KB differed. Hebrew makes it expensive:
``"`` is ordinary orthography (ע"י, חוו"ד, מנכ"ל, בע"מ), so the corruption
lands on real content and reads like a source change rather than a bug.

Only the delimiter is sniffed now; quoting follows csv.excel (RFC 4180).
"""
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.csv_parser import parse_csv, records_to_csv_bytes  # noqa: E402

# Real values from the מערכת אמו"ן barrier tables — the ones that got corrupted.
HEBREW_WITH_QUOTES = [
    'עיכוב באישור תזכיר חוק ע"י גורמים מאשרים',
    'חוו"ד משפטית לא מאפשרת פעילות',
    'אישור מכרז / פטור ע"י גורם מחוץ למשרד',
    'מנכ"ל המשרד',
    'חברה בע"מ',
]


def _roundtrip(records, fields=None):
    fields = fields or [{"id": k, "type": "text"} for k in records[0]]
    _parsed_fields, parsed = parse_csv(records_to_csv_bytes(fields, records))
    return parsed


def test_embedded_quotes_survive_the_roundtrip():
    records = [{"label": v, "n": str(i)} for i, v in enumerate(HEBREW_WITH_QUOTES)]
    parsed = _roundtrip(records)
    assert [r["label"] for r in parsed] == HEBREW_WITH_QUOTES


def test_no_stray_trailing_quote_is_appended():
    """The exact corruption signature: inner quote kept, extra one appended."""
    parsed = _roundtrip([{"label": 'עיכוב באישור תזכיר חוק ע"י גורמים מאשרים'}])
    assert not parsed[0]["label"].endswith('"')


def test_doublequote_is_not_inferred_from_the_sample():
    """A file whose first rows contain no quotes at all used to make the
    Sniffer choose doublequote=False, corrupting the quoted rows further
    down. The prefix must not change how later rows parse."""
    quoted = 'ע"י גורמים מאשרים'
    plain_prefix = [{"label": f"שורה ללא מרכאות {i}", "n": str(i)} for i in range(400)]
    records = plain_prefix + [{"label": quoted, "n": "x"}]
    parsed = _roundtrip(records)
    assert parsed[-1]["label"] == quoted


def test_alternative_delimiters_still_detected():
    for delimiter in (",", ";", "\t", "|"):
        raw = f'a{delimiter}b\r\nx{delimiter}y\r\n'.encode("utf-8")
        _fields, parsed = parse_csv(raw)
        assert parsed == [{"a": "x", "b": "y"}], delimiter


def test_quoting_is_rfc4180_under_every_delimiter():
    for delimiter in (",", ";", "|"):
        raw = f'a{delimiter}b\r\n"x""y"{delimiter}1\r\n'.encode("utf-8")
        _fields, parsed = parse_csv(raw)
        assert parsed[0]["a"] == 'x"y', delimiter


def test_delimiter_inside_a_quoted_field_is_not_a_separator():
    parsed = _roundtrip([{"label": "כן, אבל", "n": "1"}])
    assert parsed[0]["label"] == "כן, אבל"


def test_newline_inside_a_quoted_field_survives():
    parsed = _roundtrip([{"label": "שורה\nשנייה", "n": "1"}])
    assert parsed[0]["label"] == "שורה\nשנייה"
