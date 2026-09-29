"""The notes layer on the deal register: OVER's reading beside the rows.

Pinned here, on the cases a reader actually reported (2026-09-29):

  * a settlement published under a pre-merger name AND its current one (צור יגאל
    / כוכב יאיר) is flagged from BOTH sides, from the measured resolution rather
    than a hard-coded pair,
  * a regional council ("מ. א. משגב") is called a council, not a settlement,
  * a name with no code in the source says a code join will miss it,
  * "מגורים" carries its measured gaps and what they mean, while a shop's
    missing room count (normal for a shop) is not called out,
  * the curated historical names all point at a real CBS settlement,
  * a failure to compute a note never fails the query it annotates.
"""
import asyncio
import json
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import deals_notes, deals_query

_LISTING = [
    {"settlement": "כוכב יאיר", "settlement_code": None, "deals": 1311,
     "resolved_code": 1224, "resolved_name": "כוכב יאיר", "authority": "כוכב יאיר"},
    {"settlement": "צור יגאל", "settlement_code": None, "deals": 1103,
     "resolved_code": 1224, "resolved_name": "כוכב יאיר", "authority": None},
    {"settlement": "מ. א. משגב", "settlement_code": None, "deals": 641,
     "resolved_code": None, "resolved_name": None, "authority": "משגב"},
    {"settlement": "פתח תקווה", "settlement_code": "7900", "deals": 90000,
     "resolved_code": 7900, "resolved_name": "פתח תקווה", "authority": "פתח תקווה"},
    {"settlement": "דבירה", "settlement_code": None, "deals": 338,
     "resolved_code": None, "resolved_name": None, "authority": None},
]


def test_a_settlement_published_under_two_names_is_flagged_from_both_sides():
    kochav = " ".join(deals_notes.settlement_notes("כוכב יאיר", _LISTING))
    tzur = " ".join(deals_notes.settlement_notes("צור יגאל", _LISTING))
    assert "\"צור יגאל\" (1,103)" in kochav
    assert "\"כוכב יאיר\" (1,311)" in tzur
    assert "חלק מ\"כוכב יאיר\"" in tzur          # the curated history, in words


def test_a_regional_council_is_not_called_a_settlement():
    notes = deals_notes.settlement_notes("מ. א. משגב", _LISTING)
    assert any("מועצה אזורית (משגב)" in n for n in notes)


def test_a_name_without_a_source_code_warns_about_code_joins():
    notes = " ".join(deals_notes.settlement_notes("כוכב יאיר", _LISTING))
    assert "אין קוד יישוב במקור" in notes and "סמל 1224" in notes


def test_an_unremarkable_settlement_gets_no_notes():
    assert deals_notes.settlement_notes("פתח תקווה", _LISTING) == []
    assert deals_notes.settlement_notes("לא קיים", _LISTING) == []


def test_an_unresolvable_name_says_so():
    notes = deals_notes.settlement_notes("דבירה", _LISTING)
    assert any("לא מזוהה באינדקס" in n for n in notes)


def test_residential_is_flagged_with_its_measured_gaps_and_their_meaning():
    note = deals_notes.nature_note({"nature": "מגורים", "pct_no_area": 68, "pct_no_rooms": 99})
    assert "ב-68% אין שטח" in note and "ב-99% אין מספר חדרים" in note
    assert "דירה בבית קומות" in note


def test_a_shop_without_rooms_is_not_news():
    assert deals_notes.nature_note({"nature": "חנות", "pct_no_area": 8,
                                    "pct_no_rooms": 100}) is None
    assert deals_notes.nature_note({"nature": "דירה בבית קומות", "pct_no_area": 5,
                                    "pct_no_rooms": 6}) is None


def test_every_historical_alias_points_at_a_real_settlement():
    root = os.path.dirname(os.path.dirname(__file__))
    with open(os.path.join(root, "data", "settlement_aliases_manual.json"), encoding="utf-8") as fh:
        manual = json.load(fh)
    with open(os.path.join(root, "data", "settlements_2024.json"), encoding="utf-8") as fh:
        names = {r["name"] for r in json.load(fh)}
    hist = [m for m in manual if m.get("historical")]
    assert {"צור יגאל", "גבעת עדה", "מכבים-רעות", "קרית חיים"} <= {m["variant"] for m in hist}
    assert all(m["name"] in names for m in hist)


def test_a_note_that_fails_does_not_fail_the_query(monkeypatch):
    async def boom():
        raise RuntimeError("no index")

    monkeypatch.setattr(deals_query, "settlements", boom)
    assert asyncio.run(deals_notes.for_filters({"settlement": "כוכב יאיר"})) == []


def test_notes_are_only_computed_for_what_was_filtered(monkeypatch):
    calls = []

    async def settlements():
        calls.append("settlements")
        return _LISTING

    async def natures():
        calls.append("natures")
        return [{"nature": "מגורים", "pct_no_area": 68, "pct_no_rooms": 99}]

    monkeypatch.setattr(deals_query, "settlements", settlements)
    monkeypatch.setattr(deals_query, "natures", natures)
    assert asyncio.run(deals_notes.for_filters({"gush": 6319})) == []
    assert calls == []
    notes = asyncio.run(deals_notes.for_filters({"nature": "מגורים"}))
    assert calls == ["natures"] and len(notes) == 1


class _Conn:
    def __init__(self, exists=True):
        self.exists = exists
        self.written: list = []

    async def fetchval(self, sql, *args):
        return self.exists

    async def fetch(self, sql, *args):
        return [{"code": 1224, "name": "כוכב יאיר"}, {"code": 4000, "name": "חיפה"}]

    async def executemany(self, sql, rows):
        assert "WHERE a.weight < EXCLUDED.weight" in sql   # never demote a better hit
        self.written += rows


class _Pool:
    def __init__(self, conn):
        self.conn = conn

    def acquire(self):
        pool = self

        class _Ctx:
            async def __aenter__(self):
                return pool.conn

            async def __aexit__(self, *a):
                return False

        return _Ctx()


def _ensure(monkeypatch, conn):
    from app.services import append_store, settlement_index

    async def get_pool():
        return _Pool(conn)

    monkeypatch.setattr(append_store, "get_pool", get_pool)
    return asyncio.run(settlement_index.ensure_manual_aliases())


def test_the_historical_names_are_upserted_after_boot(monkeypatch):
    from app.services.settlement_index import norm
    conn = _Conn()
    assert _ensure(monkeypatch, conn) > 0
    got = {(v, c) for v, c, *_ in conn.written}
    assert (norm("צור יגאל"), 1224) in got and (norm("קרית חיים"), 4000) in got


def test_nothing_is_written_before_the_index_exists(monkeypatch):
    conn = _Conn(exists=False)
    assert _ensure(monkeypatch, conn) == 0 and conn.written == []
