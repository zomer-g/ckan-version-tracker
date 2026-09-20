"""Unit tests for the two property MCP servers: /nadlan/mcp and /deals/mcp.

No DB: the query layer is stubbed. What is under test is the MCP surface —
the tool registries, JSON-RPC dispatch, the identifier precedence in
``lookup_property``, and the three places where these servers exist to stop a
caller from getting a confident wrong answer:

  * a property answer carries its own confidence and caveats,
  * a settlement comparison run without a deal type says so in the RESULT, not
    only in the instructions nobody re-reads,
  * both servers hand back the same deal rows, from the same function.
"""
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.mcp import deals_server as D  # noqa: E402
from app.mcp import nadlan_server as N  # noqa: E402
from app.services import deals_query as DQ  # noqa: E402
from app.services import nadlan_query as NQ  # noqa: E402

_PARCEL = {
    "parcel_key": "6319-0-225", "gush": 6319, "gush_suffix": 0, "parcel": 225,
    "gp_key": "6319-225", "gp_ambiguous": False, "settlement_code": 7900,
    "locality_name": "פתח תקווה", "reg_mun_name": None, "county_name": None,
    "region_name": None, "legal_area": 1234.0, "status_text": "מוסדר",
    "lat": 32.0789, "lon": 34.9171,
}


class _Req:
    """Minimal stand-in for a Starlette Request."""
    headers = {"host": "over.org.il", "x-forwarded-proto": "https"}

    class url:  # noqa: D106
        scheme = "https"
        netloc = "over.org.il"


def _call(server, tool, args=None):
    async def _go():
        return await server._IMPL[tool](_Req(), None, None, args or {})
    return asyncio.run(_go())


@pytest.fixture(autouse=True)
def _stub(monkeypatch):
    """Record which reader each tool reached, and with what."""
    calls: dict[str, list] = {}

    def record(name):
        def _f(*a, **kw):
            calls.setdefault(name, []).append((a, kw))
        return _f

    async def _ready():
        return True

    async def _fetch(sql, *args):
        return []

    async def _gush(g, h, s=None):
        record("by_gush_helka")(g, h, s)
        return [_PARCEL]

    async def _point(lat, lon, r=0.0, limit=50):
        record("by_point")(lat, lon, r, limit)
        return [_PARCEL]

    async def _zip(z):
        record("by_zip")(z)
        return [], [_PARCEL]

    async def _addr(city, street, number=None):
        record("by_address")(city, street, number)
        return [], [_PARCEL]

    async def _deals(gush, helka, limit=50, offset=0, sub_parcel=None):
        record("parcel_deals")(gush, helka, limit, offset, sub_parcel)
        return [{"date": "2025-04-23", "amount": 2_450_000}], 143

    monkeypatch.setattr(NQ, "is_ready", _ready)
    monkeypatch.setattr(NQ, "_fetch", _fetch)
    monkeypatch.setattr(NQ, "by_gush_helka", _gush)
    monkeypatch.setattr(NQ, "by_point", _point)
    monkeypatch.setattr(NQ, "by_zip", _zip)
    monkeypatch.setattr(NQ, "by_address", _addr)
    monkeypatch.setattr(NQ, "parcel_deals", _deals)
    monkeypatch.setattr(DQ, "is_ready", _ready)
    return calls


# ── both servers, the shape every sibling shares ─────────────────────────────
@pytest.mark.parametrize("server", [N, D])
def test_every_advertised_tool_is_implemented(server):
    advertised = {t["name"] for t in server.TOOLS}
    assert advertised == set(server._IMPL), server.SERVER_NAME


@pytest.mark.parametrize("server", [N, D])
def test_initialize_returns_the_instructions(server):
    async def _go():
        return await server.handle_message(_Req(), None, None, None,
                                           {"jsonrpc": "2.0", "id": 1,
                                            "method": "initialize", "params": {}})
    res = asyncio.run(_go())["result"]
    assert res["serverInfo"]["name"] == server.SERVER_NAME
    assert res["instructions"] == server.SERVER_INSTRUCTIONS


class _User:
    """What the dispatcher reads off an McpUser: an id and a client id."""
    id = "00000000-0000-0000-0000-000000000001"
    client_id = None


@pytest.mark.parametrize("server", [N, D])
def test_an_unknown_tool_is_an_error_not_a_crash(server, monkeypatch):
    async def _log(**kw):
        return None

    monkeypatch.setattr(server, "log_usage", _log)

    async def _go():
        return await server.handle_message(_Req(), None, _User(), None, {
            "jsonrpc": "2.0", "id": 2, "method": "tools/call",
            "params": {"name": "definitely_not_a_tool", "arguments": {}}})
    assert asyncio.run(_go())["result"]["isError"] is True


# ── נדל"ן לעם: which identifier wins ─────────────────────────────────────────
def test_lookup_uses_the_address_when_one_was_given(_stub):
    _call(N, "lookup_property", {"city": "פתח תקווה", "street": "אבימלך", "number": "8"})
    assert _stub["by_address"][0][0] == ("פתח תקווה", "אבימלך", "8")


def test_lookup_prefers_an_explicit_identifier_over_free_text(_stub):
    """A caller that named גוש and חלקה has said what it means; sniffing over
    that could only get it wrong."""
    out, _ = _call(N, "lookup_property", {"gush": 6319, "helka": 225, "q": "4935048"})
    assert out["query"]["mode"] == "gush_helka"
    assert "by_zip" not in _stub


def test_lookup_falls_back_to_sniffing_free_text(_stub):
    out, _ = _call(N, "lookup_property", {"q": "גוש 6319 חלקה 225"})
    assert out["query"]["mode"] == "gush_helka"
    assert _stub["by_gush_helka"][0][0][:2] == (6319, 225)


def test_free_text_that_looks_like_an_address_asks_for_the_parts(_stub):
    """Rather than guessing at a split, which is how a wrong parcel gets shown."""
    out, count = _call(N, "lookup_property", {"q": "רחוב אבימלך 8 פתח תקווה"})
    assert count == 0 and "city" in out["hint"] and "suggest_streets" in out["hint"]


def test_lookup_without_any_identifier_is_refused(_stub):
    with pytest.raises(ValueError, match="gush"):
        _call(N, "lookup_property", {})


def test_a_bad_zip_is_refused_before_the_database(_stub):
    with pytest.raises(ValueError, match="מיקוד"):
        _call(N, "lookup_property", {"zip": "123456"})
    assert "by_zip" not in _stub


# ── נדל"ן לעם: what a property answer must carry ─────────────────────────────
def test_a_property_answer_carries_its_confidence_and_its_caveats(_stub):
    out, count = _call(N, "lookup_property", {"gush": 6319, "helka": 225})
    assert count == 1
    prop = out["properties"][0]
    assert prop["match"]["confidence"] == "exact"
    assert "מעובד" in out["source"]
    assert any("תת-גוש" in c for c in out["caveats"])


def test_an_ambiguous_parcel_is_downgraded_not_hidden(monkeypatch, _stub):
    async def _gush(g, h, s=None):
        return [{**_PARCEL, "gp_ambiguous": True}]

    monkeypatch.setattr(NQ, "by_gush_helka", _gush)
    out, _ = _call(N, "lookup_property", {"gush": 6319, "helka": 225})
    assert out["properties"][0]["match"]["confidence"] == "approximate"


def test_the_two_descriptive_layers_can_be_switched_off(monkeypatch, _stub):
    seen = []
    monkeypatch.setattr(NQ, "stat_areas", lambda keys: _noop(seen, "stat"))
    monkeypatch.setattr(NQ, "deal_summaries", lambda parcels: _noop(seen, "deals"))
    _call(N, "lookup_property", {"gush": 6319, "helka": 225,
                                 "include_stat_area": False, "include_deals": False})
    assert seen == []


async def _noop(seen, tag):
    seen.append(tag)
    return {}


def test_the_sub_parcel_is_zero_padded_to_the_published_form(_stub):
    _call(N, "parcel_deals", {"gush": 6319, "helka": 225, "sub_parcel": "7"})
    assert _stub["parcel_deals"][0][0][4] == "007"


# ── עסקאות נדל"ן: the honesty guards ─────────────────────────────────────────
def test_comparing_settlements_without_a_deal_type_warns_in_the_result(monkeypatch):
    """The instructions say to pass `nature`; a model that did not must be told
    again where it will actually be read — next to the numbers."""
    async def _cmp(y1, y2, **kw):
        return [{"settlement": "חיפה", "deals_from": 100, "deals_to": 120,
                 "median_from": 1_000_000, "median_to": 1_500_000, "change_pct": 50.0}]

    monkeypatch.setattr(DQ, "compare_settlements", _cmp)
    out, _ = _call(D, "compare_settlements", {"year_from": 2019, "year_to": 2025})
    assert "תמהיל" in out["warning"]

    out, _ = _call(D, "compare_settlements",
                   {"year_from": 2019, "year_to": 2025, "nature": "דירה בבית קומות"})
    assert "warning" not in out


def test_the_register_is_never_presented_as_processed(monkeypatch):
    async def _search(f, limit=50, offset=0, sort="date_desc"):
        return {"data": [], "total": 0, "total_capped": False, "limit": limit,
                "offset": offset, "sort": sort, "console_sql": "", "row_url": ""}

    monkeypatch.setattr(DQ, "search", _search)
    out, _ = _call(D, "search_deals", {})
    assert out["processed"] is False
    assert any("חציון" in c for c in out["caveats"])


def test_only_known_filters_reach_the_query(monkeypatch):
    """A model improvising a parameter name must not have it silently accepted
    into a filter dict that means nothing."""
    seen = {}

    async def _search(f, limit=50, offset=0, sort="date_desc"):
        seen.update(f)
        return {"data": [], "total": 0, "total_capped": False, "limit": limit,
                "offset": offset, "sort": sort, "console_sql": "", "row_url": ""}

    monkeypatch.setattr(DQ, "search", _search)
    _call(D, "search_deals", {"settlement": "חיפה", "neighbourhood": "הדר",
                              "min_amount": 1_000_000})
    assert seen == {"settlement": "חיפה", "min_amount": 1_000_000}


def test_both_servers_read_one_parcel_through_the_same_function(_stub):
    """Two surfaces, one reader — they cannot disagree about a parcel."""
    a, _ = _call(N, "parcel_deals", {"gush": 6319, "helka": 225})
    b, _ = _call(D, "parcel_deals", {"gush": 6319, "helka": 225})
    assert a["deals"] == b["deals"] and a["total"] == b["total"] == 143


def test_the_settlement_list_can_be_narrowed_by_name(monkeypatch):
    async def _settlements():
        return [{"settlement": "חיפה", "deals": 10}, {"settlement": "תל אביב -יפו", "deals": 20}]

    monkeypatch.setattr(DQ, "settlements", _settlements)
    out, count = _call(D, "list_settlements", {"query": "חיפ"})
    assert count == 1 and out["settlements"][0]["settlement"] == "חיפה"
