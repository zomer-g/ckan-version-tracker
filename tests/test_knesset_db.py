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


# ── Scale-to-zero gate (_next_due_at) ────────────────────────────────────────
#
# The 3-minute scheduler tick used to hit the append DB unconditionally, which
# on Neon prevents the compute from ever scaling to zero (5-minute idle
# window). These cover the arithmetic that decides when the tick may skip the
# round-trip entirely, plus the fail-open cases that must NOT gate.

from datetime import timedelta  # noqa: E402


def _state(table, *, full_loaded=True, last_synced_at=None):
    return {"table_name": table, "full_loaded": full_loaded,
            "last_synced_at": last_synced_at}


def test_next_due_time_is_the_soonest_table_deadline():
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object(), "b": object()}
    states = [
        _state("a", last_synced_at=now - timedelta(hours=1)),    # due in 11h
        _state("b", last_synced_at=now - timedelta(hours=8)),    # due in 4h ← min
    ]
    assert K._next_due_time(states, sets, now, 12.0) == now + timedelta(hours=4)


def test_next_due_time_does_not_gate_while_a_table_is_mid_full_load():
    """The initial ~3M-row load NEEDS the 3-minute cadence to make progress."""
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object(), "b": object()}
    states = [
        _state("a", last_synced_at=now - timedelta(hours=1)),
        _state("b", full_loaded=False, last_synced_at=now),
    ]
    assert K._next_due_time(states, sets, now, 12.0) is None


def test_next_due_time_does_not_gate_when_a_table_never_synced():
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object()}
    assert K._next_due_time([_state("a", last_synced_at=None)], sets, now, 12.0) is None


def test_next_due_time_does_not_gate_when_a_set_has_no_state_row():
    """Fewer state rows than entity sets ⇒ unknown table ⇒ ask the DB."""
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object(), "b": object()}
    states = [_state("a", last_synced_at=now - timedelta(hours=1))]
    assert K._next_due_time(states, sets, now, 12.0) is None


def test_next_due_time_ignores_tables_dropped_upstream():
    """A state row whose set vanished from $metadata must not pin the gate."""
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object()}
    states = [
        _state("a", last_synced_at=now - timedelta(hours=2)),        # due in 10h
        _state("gone", last_synced_at=now - timedelta(days=400)),    # long overdue
        _state("gone2", last_synced_at=now - timedelta(days=400)),
    ]
    assert K._next_due_time(states, sets, now, 12.0) == now + timedelta(hours=10)


def test_next_due_time_never_returns_a_zero_length_gate():
    now = datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc)
    sets = {"a": object()}
    states = [_state("a", last_synced_at=now - timedelta(hours=12))]   # exactly due
    assert K._next_due_time(states, sets, now, 12.0) > now


def test_sync_tick_skips_without_touching_the_db_while_gated(monkeypatch):
    """The whole point: a gated tick must not open an append-DB connection."""
    monkeypatch.setattr(K.settings, "append_database_url", "postgresql://x/y")
    monkeypatch.setattr(K.settings, "knesset_db_enabled", True)

    async def _boom():
        raise AssertionError("gated tick reached the append DB")
    monkeypatch.setattr(K.append_store, "get_pool", _boom)
    monkeypatch.setattr(K, "_next_due_at",
                        datetime.now(timezone.utc) + timedelta(hours=4))

    res = asyncio.run(K.sync_tick())
    assert res["skipped"] == "not due"


def test_sync_tick_gate_is_bypassed_for_admin_single_table_runs(monkeypatch):
    """An admin pressing "סנכרן" must never be told "not due"."""
    monkeypatch.setattr(K.settings, "append_database_url", "postgresql://x/y")
    monkeypatch.setattr(K.settings, "knesset_db_enabled", True)
    monkeypatch.setattr(K, "_next_due_at",
                        datetime.now(timezone.utc) + timedelta(hours=4))

    reached = []

    async def _marker():
        reached.append(True)
        raise RuntimeError("stop here — we only needed to get past the gate")
    monkeypatch.setattr(K, "ensure_infra", _marker)

    with pytest.raises(RuntimeError):
        asyncio.run(K.sync_tick(only_table="kns_bill"))
    assert reached == [True]


def test_mmm_loaded_version_cache_avoids_the_append_db(monkeypatch):
    """Same gate, MMM half: the second append-DB query per tick, removed."""
    from app.services import knesset_mmm_db as M

    async def _boom():
        raise AssertionError("cached _loaded_version reached the append DB")
    monkeypatch.setattr(M.append_store, "get_pool", _boom)
    monkeypatch.setattr(M, "_loaded_version_cache", 42)

    assert asyncio.run(M._loaded_version()) == 42


def test_mmm_loaded_version_reads_once_then_caches(monkeypatch):
    from app.services import knesset_mmm_db as M

    calls = []

    class _Conn:
        async def fetchval(self, *a):
            calls.append(a)
            return 7

    class _Acq:
        async def __aenter__(self): return _Conn()
        async def __aexit__(self, *a): return False

    class _Pool:
        def acquire(self): return _Acq()

    async def _pool():
        return _Pool()

    monkeypatch.setattr(M.append_store, "get_pool", _pool)
    monkeypatch.setattr(M, "_loaded_version_cache", None)

    assert asyncio.run(M._loaded_version()) == 7
    assert asyncio.run(M._loaded_version()) == 7
    assert len(calls) == 1          # second call served from process memory
