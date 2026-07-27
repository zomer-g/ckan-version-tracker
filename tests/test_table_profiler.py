"""Offline unit tests for the table profiler's pure logic.

These never touch a database — they exercise the deterministic building blocks
the SQL profiler relies on: numeric/date classification, date-format detection
(the crux of "detect the format to enable date parsing"), Hebrew keyword
extraction, entity heuristics, the change-signature, and the pilot selector.
The DB-bound paths (aggregate/top-values/upsert) are integration-tested against
a live append DB when APPEND_DATABASE_URL is set — out of scope here.
"""
from app.services import table_profiler as p


# ── numeric detection ─────────────────────────────────────────────────────────
def test_looks_numeric_plain_and_thousands():
    assert p.looks_numeric(["1", "2", "3"]) == 1.0
    assert p.looks_numeric(["1,234", "5,678", "90"]) == 1.0  # thousands sep stripped
    assert p.looks_numeric(["-3.14", "0", "42"]) == 1.0
    assert p.looks_numeric(["abc", "1", "2", "3"]) == 0.75
    assert p.looks_numeric([]) == 0.0


# ── date-format detection ─────────────────────────────────────────────────────
def test_detect_iso_date():
    d = p.detect_date_format(["2020-01-05", "2021-12-31", "2019-06-15"])
    assert d and d["python"] == "%Y-%m-%d" and d["postgres"] == "YYYY-MM-DD"
    assert d["match_rate"] == 1.0


def test_detect_israeli_day_first():
    d = p.detect_date_format(["05/01/2020", "31/12/2021", "15/06/2019"])
    # 31 and 15 can't be months → unambiguously day-first
    assert d and d["python"] == "%d/%m/%Y"
    assert d["ambiguous"] is False


def test_detect_ambiguous_day_month():
    # Every value is valid as both dd/mm and mm/dd → flagged ambiguous.
    d = p.detect_date_format(["01/02/2020", "03/04/2021", "05/06/2019"])
    assert d and d["python"] == "%d/%m/%Y"
    assert d["ambiguous"] is True


def test_detect_datetime_iso_t():
    d = p.detect_date_format(["2020-01-05T10:00:00", "2021-12-31T23:59:59"])
    assert d and d["python"] == "%Y-%m-%dT%H:%M:%S"


def test_detect_non_date_returns_none():
    assert p.detect_date_format(["hello", "world", "foo"]) is None
    assert p.detect_date_format(["123", "456"]) is None  # bare ints aren't dates


def test_detect_below_threshold_none():
    # Only 1/4 parse → below DATE_MIN_MATCH (0.8).
    assert p.detect_date_format(["2020-01-01", "x", "y", "z"]) is None


# ── keyword extraction ────────────────────────────────────────────────────────
def test_extract_keywords_hebrew_and_stopwords():
    samples = ["משרד הבריאות בתל אביב", "משרד הבריאות בירושלים", "עבודת המשרד"]
    kws = {k["token"]: k["count"] for k in p.extract_keywords(samples)}
    assert kws.get("משרד") == 2          # bare form, 2x
    assert kws.get("הבריאות") == 2
    # NOTE: "המשרד" (prefix ה) counts as a DISTINCT token from "משרד" — the exact
    # morphological-inflection problem Phase 2's standardization index will fold.
    assert kws.get("המשרד") == 1
    assert "עם" not in kws               # stopword removed (had it appeared)


def test_tokenize_keeps_geresh_words():
    toks = p.tokenize_he('מנכ"ל ההסתדרות')
    assert 'מנכ"ל' in toks


# ── entity heuristics ─────────────────────────────────────────────────────────
def test_entity_locality_from_gazetteer():
    gz = {p._norm_place(x) for x in ["תל אביב-יפו", "ירושלים", "חיפה"]}
    r = p.classify_entity_heuristic(
        "שם_ישוב", is_numeric=False, date_fmt=None,
        top_values=["תל אביב-יפו", "ירושלים", "חיפה", "באר שבע"],
        distinct_ratio=0.2, locality_names=gz)
    assert r["guess"] == "locality"


def test_entity_corporation_suffix():
    r = p.classify_entity_heuristic(
        "שם_חברה", is_numeric=False, date_fmt=None,
        top_values=['אלפא טכנולוגיות בע"מ', 'בטא ניהול בע"מ'],
        distinct_ratio=0.9, locality_names=set())
    assert r["guess"] == "corporation"


def test_entity_id_high_cardinality_numeric():
    r = p.classify_entity_heuristic(
        "מזהה", is_numeric=True, date_fmt=None, top_values=[],
        distinct_ratio=1.0, locality_names=set())
    assert r["guess"] == "id"


def test_entity_amount_named_numeric():
    r = p.classify_entity_heuristic(
        "סכום_תקציב", is_numeric=True, date_fmt=None, top_values=[],
        distinct_ratio=0.4, locality_names=set())
    assert r["guess"] == "amount"


def test_entity_date_takes_precedence():
    r = p.classify_entity_heuristic(
        "col", is_numeric=False, date_fmt={"python": "%Y-%m-%d", "match_rate": 1.0},
        top_values=[], distinct_ratio=0.5, locality_names=set())
    assert r["guess"] == "date"


# ── column classification (declared type vs sniffed) ─────────────────────────
def test_classify_trusts_declared_timestamp():
    # A real timestamptz column reads as "...+00" when cast to text and would
    # NOT match any text date format — the declared type must win.
    cls = p._classify_columns(
        {"first_seen": ["2026-07-21 18:58:55.359262+00"]},
        {"first_seen": "timestamp"})
    assert cls["first_seen"] == {"kind": "date", "native": True}


def test_classify_trusts_declared_numeric():
    cls = p._classify_columns({"n": ["1", "2"]}, {"n": "int"})
    assert cls["n"]["kind"] == "numeric" and cls["n"]["native"] is True


def test_classify_sniffs_text_columns():
    # append data columns are declared text — fall back to value sniffing.
    cls = p._classify_columns(
        {"d": ["03.01.1993", "19.07.2026"], "amt": ["10", "20"]},
        {"d": "text", "amt": "text"})
    assert cls["d"]["kind"] == "date" and not cls["d"].get("native")
    assert cls["amt"]["kind"] == "numeric" and not cls["amt"].get("native")


def test_native_exprs_have_no_cast():
    assert "::numeric" not in p._numeric_expr("n", native=True)
    assert p._date_expr("t", None, native=True).startswith('MIN("t")')


# ── corrupted-UTF8 column names ───────────────────────────────────────────────
def test_is_encodable():
    assert p.is_encodable("תאריך פרסום") is True
    assert p.is_encodable("objectId") is True
    # lone surrogate (what asyncpg leaves from a bad CP862/CSV decode) → not encodable
    assert p.is_encodable("תא\udc90ריך") is False
    assert p.is_encodable(None) is False
    assert p.is_encodable(123) is False


def test_sanitize_json_replaces_surrogates():
    import json
    bad = json.dumps({"col": "תא\udc90ריך"}, ensure_ascii=False)
    out = p._sanitize_json(bad)
    out.encode("utf-8")  # must not raise
    assert "\udc90" not in out


def test_signature_survives_bad_column_name():
    # A table whose column name has a lone surrogate must still hash (no raise).
    s = p.table_signature(10, [{"name": "תא\udc90ריך", "type": "text"}])
    assert isinstance(s, str) and len(s) == 16


# ── signature ─────────────────────────────────────────────────────────────────
def test_signature_stable_and_sensitive():
    cols = [{"name": "a", "type": "text"}, {"name": "b", "type": "int"}]
    s1 = p.table_signature(100, cols)
    assert s1 == p.table_signature(100, cols)          # stable
    assert s1 != p.table_signature(101, cols)          # row count matters
    assert s1 != p.table_signature(100, cols[::-1])    # column order matters


# ── pilot selection ───────────────────────────────────────────────────────────
def _rec(table, schema="public", source_type="ckan", est_rows=10, kind="dataset"):
    return {"table": table, "schema": schema, "source_type": source_type,
            "est_rows": est_rows, "kind": kind, "title": table, "columns": []}


def test_pilot_spans_all_buckets():
    catalog = (
        [_rec(f"append_ckan_{i}") for i in range(10)]
        + [_rec(f"append_scr_{i}", source_type="scraper") for i in range(10)]
        + [_rec(f"kn_{i}", schema="knesset") for i in range(10)]
        + [_rec(f"idx_{i}", schema="idx") for i in range(10)]
        + [_rec(f"od_{i}", schema="odata") for i in range(10)]
    )
    pilot = p.select_pilot(catalog, n=20)
    assert len(pilot) == 20
    buckets = {p._type_key(r) for r in pilot}
    # All five distinct types are represented.
    assert buckets == {"public:ckan", "public:scraper", "knesset", "idx", "odata"}


def test_pilot_excludes_over_artifacts():
    catalog = [_rec("over_table_profiles"), _rec("append_real")]
    pilot = p.select_pilot(catalog, n=20)
    assert [r["table"] for r in pilot] == ["append_real"]


def test_pilot_prefers_nonempty():
    catalog = [_rec("append_empty", est_rows=0), _rec("append_full", est_rows=500)]
    pilot = p.select_pilot(catalog, n=1)
    assert pilot[0]["table"] == "append_full"
