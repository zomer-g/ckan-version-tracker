"""Unit tests for the Knesset ODATA mirror's pure helpers (no DB needed)."""
import asyncio
import os
import sys
from datetime import datetime, timezone

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

import pytest  # noqa: E402

from app.services import knesset_db as K  # noqa: E402
from app.services.knesset_tables_meta import TABLES, description_of, group_of  # noqa: E402


# ── EntitySet construction / feed quirks ─────────────────────────────────────

def test_entity_set_url_override_for_document_query():
    es = K.EntitySet("KNS_DocumentQuerie", [("Id", "Edm.Int32")])
    assert es.url_name == "KNS_DocumentQuery"   # declared set 404s; type name works
    assert es.base_url == K.PARLIAMENT_BASE
    assert es.table == "kns_documentquerie"


def test_lobbyist_sets_route_to_lobbyist_service():
    for name in ("V_Lobbyists", "V_LobbyistsClients"):
        es = K.EntitySet(name, [("Id", "Edm.Int32"), ("FullName", "Edm.String")])
        assert es.base_url == K.LOBBYIST_BASE
        assert es.url_name == name
        assert not es.has_last_updated   # views have no LastUpdatedDate → full re-walk


def test_has_last_updated_detection():
    es = K.EntitySet("KNS_Bill", [("Id", "Edm.Int32"), ("LastUpdatedDate", "Edm.DateTimeOffset")])
    assert es.has_last_updated


# ── Value conversion ─────────────────────────────────────────────────────────

def test_datetime_with_offset_parses():
    d = K._to_datetime("2021-07-22T09:47:05.063+03:00")
    assert d is not None and d.utcoffset() is not None
    assert d.year == 2021


def test_naive_datetime_gets_israel_tz():
    # KNS_DocumentQuery serializes offset-less local times.
    d = K._to_datetime("2013-02-12T14:09:35.643")
    assert d is not None and d.tzinfo is K._ISRAEL_TZ


def test_zulu_datetime_parses():
    d = K._to_datetime("2020-01-01T00:00:00Z")
    assert d == datetime(2020, 1, 1, tzinfo=timezone.utc)


def test_convert_types():
    assert K._convert(None, "Edm.String") is None
    assert K._convert(5, "Edm.Int32") == 5
    assert K._convert("7", "Edm.Int64") == 7
    assert K._convert(True, "Edm.Boolean") is True
    assert K._convert("abc", "Edm.String") == "abc"
    assert K._convert("a\x00b", "Edm.String") == "ab"   # Postgres text rejects NUL
    assert K._convert("not-an-int", "Edm.Int32") is None


def test_row_values_case_insensitive_lookup():
    es = K.EntitySet("KNS_DocumentQuerie", [
        ("Id", "Edm.Int32"),
        ("QueryID", "Edm.Int32"),
        ("LastUpdatedDate", "Edm.DateTimeOffset"),
    ])
    # camelCase row as the quirky endpoint returns it (+ a nav prop to ignore)
    row = {"id": 221299, "queryID": 478099,
           "lastUpdatedDate": "2013-02-12T14:09:35.643", "knS_Query": None}
    vals = K._row_values(row, es)
    assert vals[0] == 221299
    assert vals[1] == 478099
    assert vals[2].year == 2013


# ── SQL guardrails (validation happens before any DB touch) ──────────────────

@pytest.mark.parametrize("bad", [
    "", "  ", "DELETE FROM kns_bill", "SELECT 1; SELECT 2",
    "UPDATE kns_bill SET name='x'", "DROP TABLE kns_bill",
    "SELECT * FROM kns_bill; --", "CREATE TABLE t (x int)",
    "WITH d AS (DELETE FROM kns_bill RETURNING *) SELECT * FROM d",
])
def test_run_sql_rejects_non_select(bad):
    with pytest.raises(ValueError):
        asyncio.run(K.run_sql(bad))


@pytest.mark.parametrize("bad", ["DELETE FROM kns_bill", "SELECT 1; SELECT 2", ""])
def test_export_csv_rejects_non_select(bad):
    async def _drain():
        async for _ in K.iter_sql_csv(bad):
            pass
    with pytest.raises(ValueError):
        asyncio.run(_drain())


# ── OData literal formatting ─────────────────────────────────────────────────

def test_fmt_odata_dt_is_utc_zulu():
    d = datetime(2026, 7, 8, 12, 30, tzinfo=timezone.utc)
    assert K._fmt_odata_dt(d) == "2026-07-08T12:30:00Z"


# ── Metadata parse (live-captured fixture) ───────────────────────────────────

_METADATA_SNIPPET = """<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
 <edmx:DataServices>
  <Schema Namespace="OdataService.DAL.ParliamentInfo" xmlns="http://docs.oasis-open.org/odata/ns/edm">
   <EntityType Name="KNS_Bill">
    <Key><PropertyRef Name="Id"/></Key>
    <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
    <Property Name="Name" Type="Edm.String"/>
    <Property Name="IsContinuationBill" Type="Edm.Boolean"/>
    <Property Name="LastUpdatedDate" Type="Edm.DateTimeOffset"/>
   </EntityType>
   <EntityType Name="V_Lobbyist">
    <Key><PropertyRef Name="Id"/></Key>
    <Property Name="Id" Type="Edm.Int32" Nullable="false"/>
    <Property Name="FullName" Type="Edm.String"/>
   </EntityType>
   <EntityContainer Name="Container">
    <EntitySet Name="KNS_Bill" EntityType="OdataService.DAL.ParliamentInfo.KNS_Bill"/>
    <EntitySet Name="V_Lobbyists" EntityType="OdataService.DAL.Lobbyist.V_Lobbyist"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>
"""


def test_fetch_metadata_parses_sets_and_types(monkeypatch):
    class FakeResp:
        text = _METADATA_SNIPPET
        def raise_for_status(self):
            pass

    class FakeClient:
        async def get(self, url):
            assert url.endswith("$metadata")
            return FakeResp()

    sets = asyncio.run(K.fetch_metadata(FakeClient()))
    by_name = {s.name: s for s in sets}
    assert set(by_name) == {"KNS_Bill", "V_Lobbyists"}
    bill = by_name["KNS_Bill"]
    assert bill.columns[0] == ("Id", "Edm.Int32")
    assert bill.has_last_updated
    lob = by_name["V_Lobbyists"]
    # Set name maps to a type in a DIFFERENT namespace (Lobbyist) — resolved by
    # bare type name.
    assert lob.columns == [("Id", "Edm.Int32"), ("FullName", "Edm.String")]
    assert lob.base_url == K.LOBBYIST_BASE
    assert not lob.has_last_updated


# ── Hebrew meta coverage ─────────────────────────────────────────────────────

def test_meta_covers_known_sets_and_falls_back():
    assert group_of("KNS_Bill") == "הצעות חוק"
    assert description_of("KNS_PlenumVoteResult")
    assert group_of("KNS_SomethingNew") == "אחר"     # unknown set still renders
    for name, (group, desc) in TABLES.items():
        assert group and desc, name
