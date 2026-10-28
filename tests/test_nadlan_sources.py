"""A source's own rows for one parcel — the detail behind each source card.

The gazetteer is the case this exists for: the card said "12 assets" and the
reader wants the twelve. The integration half runs only when
``NADLAN_PG_TEST_URL`` names a throwaway PostGIS database (it creates tables).
"""
import asyncio
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services import append_store, column_aliases, nadlan_sources as S

PARCEL = {"gush": 6319, "parcel": 225, "gush_suffix": 0, "parcel_key": "6319-0-225"}


def test_float_text_integers_read_as_integers_and_blanks_as_nothing():
    assert S._clean("4.0") == "4"
    assert S._clean("-3.0") == "-3"
    assert S._clean("4.5") == "4.5"
    assert S._clean("  ") is None and S._clean("NULL") is None and S._clean(None) is None
    assert S._clean("דירת מגורים") == "דירת מגורים"
    assert S._clean("00008") == "8" and S._clean("0") == "0"   # Israel Post's house numbers
    assert S._clean("094151") == "094151"                      # a six-digit code keeps its zero


def test_a_source_is_matched_in_both_of_its_integer_forms():
    assert S._forms(6319) == ["6319", "6319.0"]


def test_an_unknown_source_is_refused():
    with pytest.raises(ValueError):
        asyncio.run(S.source_rows("everything", PARCEL))


PG = os.environ.get("NADLAN_PG_TEST_URL")

GAZ_COLS = ["ID", "GushNum", "ParcelNum", "SubParcelNum", "Type", "BuildingFloors",
            "BuildingYear", "EstimatedAppartments", "SettlmentID", "SettlementNameHeb",
            "StreetNameHeb", "StreetCode", "Remarks"]
GAZ_ROWS = [
    ["3", "6319.0", "225.0", "10", "חנות", "4.0", "1972.0", "", "7900", "פתח תקווה",
     "רוטשילד", "1234", ""],
    ["1", "6319", "225", "2", "דירת מגורים", "4", "1972", "1", "7900", "פתח תקווה",
     "רוטשילד", "1234", ""],
    ["2", "6319", "225", "1", "דירת מגורים", "4", "1972", "1", "7900", "פתח תקווה",
     "רוטשילד", "1234", ""],
    ["9", "6319", "226", "1", "דירת מגורים", "3", "1980", "1", "7900", "פתח תקווה",
     "רוטשילד", "1234", ""],                                   # the next parcel
]


@pytest.fixture
def pg(monkeypatch):
    if not PG:
        pytest.skip("NADLAN_PG_TEST_URL not set")
    monkeypatch.setattr(append_store.settings, "append_database_url", PG)
    monkeypatch.setattr(append_store.settings, "append_readonly_database_url", PG)
    monkeypatch.setattr(append_store, "_pool", None)
    monkeypatch.setattr(append_store, "_ro_pool", None)
    monkeypatch.setattr(S, "_cols_cache", {})

    async def _no_aliases(**kw):
        return {}

    monkeypatch.setattr(column_aliases, "load_map", _no_aliases)
    return monkeypatch


def run(main):
    async def wrapper():
        try:
            pool = await append_store.get_pool()
            async with pool.acquire() as conn:
                await conn.execute("CREATE SCHEMA IF NOT EXISTS odata")
                await conn.execute('DROP TABLE IF EXISTS odata."gaztir_41720377"')
                await conn.execute(
                    'CREATE TABLE odata."gaztir_41720377" ('
                    + ", ".join(f'"{c}" text' for c in GAZ_COLS) + ")")
                await conn.executemany(
                    'INSERT INTO odata."gaztir_41720377" VALUES ('
                    + ", ".join(f"${i + 1}" for i in range(len(GAZ_COLS))) + ")", GAZ_ROWS)
            return await main()
        finally:
            for name in ("_pool", "_ro_pool"):
                p = getattr(append_store, name)
                if p is not None:
                    await p.close()
                    setattr(append_store, name, None)
    return asyncio.run(wrapper())


def test_the_gazetteer_lists_the_parcels_assets_one_by_one(pg):
    out = run(lambda: S.source_rows("gazetteer", PARCEL))
    assert out["total"] == 3                                 # both text forms, not 226
    keys = [c["key"] for c in out["columns"]]
    assert keys[:4] == ["Type", "SubParcelNum", "BuildingFloors", "BuildingYear"]
    assert "GushNum" not in keys and "SettlementNameHeb" not in keys   # said once, on the card
    assert "Remarks" not in keys                             # empty in every row
    labels = {c["key"]: c["label"] for c in out["columns"]}
    assert labels["Type"] == "סוג נכס" and labels["BuildingYear"] == "שנת בנייה"
    rows = [dict(zip(keys, r)) for r in out["rows"]]
    assert [r["SubParcelNum"] for r in rows] == ["1", "2", "10"]     # numeric order
    assert rows[2] == {**rows[2], "Type": "חנות", "BuildingFloors": "4", "BuildingYear": "1972"}
    assert out["row_url"].startswith("https://www.over.org.il/data?q=")


def test_the_gazetteer_is_paged(pg):
    first = run(lambda: S.source_rows("gazetteer", PARCEL, limit=2))
    rest = run(lambda: S.source_rows("gazetteer", PARCEL, limit=2, offset=2))
    assert first["total"] == rest["total"] == 3
    assert len(first["rows"]) == 2 and len(rest["rows"]) == 1


def test_a_parcel_the_gazetteer_does_not_list_has_no_rows(pg):
    out = run(lambda: S.source_rows("gazetteer", {**PARCEL, "parcel": 999,
                                                  "parcel_key": "6319-0-999"}))
    assert out["total"] == 0 and out["rows"] == [] and out["columns"] == []
