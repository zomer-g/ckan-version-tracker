"""Tests for "שאלות לעם" — the cross-source deep search (חיפוש רוחבי).

The invariants that matter here are not "does it return rows" but:

  * a dead / slow / malformed source loses ITS OWN column and nothing else;
  * the per-source ``results_path`` is honoured (the five local MCP servers
    disagree on the key, and hardcoding "items" reads as "no results");
  * a tool name can never come from the request — in-process dispatch bypasses
    app/mcp/auth.py entirely and /data/mcp exposes run_sql;
  * no token value is ever serialized to the public /sources payload.

Follows the house pattern: TestClient over a FastAPI app assembled from the
router alone, no DB, limiter reset per client, outbound HTTP monkeypatched.
"""
import asyncio
import importlib
import os

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi.errors import RateLimitExceeded
from slowapi import _rate_limit_exceeded_handler

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import deep_search as deep_search_api  # noqa: E402
from app.rate_limit import limiter  # noqa: E402
from app.services import deep_search, deep_search_sources  # noqa: E402
from app.services.deep_search_sources import Card  # noqa: E402


def _client() -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(deep_search_api.router)
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


@pytest.fixture
def stub_local(monkeypatch):
    """Replace the in-process transport with a scripted one.

    ``payloads`` maps tool name -> payload dict (or a callable, or an Exception
    instance to raise). Records every (tool, args) pair so tests can assert what
    actually reached the tool.
    """
    calls: list[tuple[str, dict]] = []
    payloads: dict = {}

    def _factory(request, source):
        async def call(tool: str, args: dict):
            calls.append((tool, args))
            if tool not in deep_search.ALLOWED_TOOLS:
                raise deep_search.SourceError(f"tool {tool} not allowed")
            p = payloads.get(tool, {})
            if isinstance(p, BaseException):
                raise p
            if callable(p):
                return await p(args) if asyncio.iscoroutinefunction(p) else p(args)
            return p

        return call

    monkeypatch.setattr(deep_search, "_local_caller", _factory)
    return {"calls": calls, "payloads": payloads}


# ── registry integrity ──────────────────────────────────────────────────────

def test_registry_ids_and_attribution_are_complete():
    ids = [s.id for s in deep_search_sources.SOURCES]
    assert len(ids) == len(set(ids)), "duplicate source id"
    for s in deep_search_sources.SOURCES:
        assert s.color.startswith("#") and len(s.color) == 7, s.id
        # Attribution is a contract with the upstream MCP servers, not styling:
        # processed data must be labelled and linked back for verification.
        assert s.attribution.get("text"), s.id
        assert s.attribution.get("href", "").startswith("http"), s.id
        assert s.local or s.mcp_url, f"{s.id} has no transport"
        assert s.tool or s.run, f"{s.id} has neither tool nor run()"


def test_run_sql_can_never_be_reached():
    """/data/mcp and /knesset/mcp both expose run_sql. If a tool name could ever
    be influenced by the request this page would become an unauthenticated SQL
    console, so the allowlist is the hard stop."""
    assert "run_sql" not in deep_search.ALLOWED_TOOLS
    assert "describe_schema" not in deep_search.ALLOWED_TOOLS
    used = {s.tool for s in deep_search_sources.SOURCES if s.tool}
    assert used <= deep_search.ALLOWED_TOOLS


def test_build_args_match_the_real_tool_schemas():
    """Drift guard: if someone renames an argument in app/mcp/*_server.py, this
    fails here instead of the column silently going empty in production."""
    for s in deep_search_sources.SOURCES:
        if not s.tool:
            continue
        mod = importlib.import_module(deep_search.LOCAL_MODULES[s.local])
        schema = next(t for t in mod.TOOLS if t["name"] == s.tool)["inputSchema"]
        props = set(schema.get("properties") or {})
        cases = [{}] + [
            {f.id: ("2020" if f.type == "number" else "2020-01-01" if f.type == "date" else "x")}
            for f in s.filters
        ]
        for c in cases:
            unknown = set(s.build_args("בדיקה", 10, c)) - props
            assert not unknown, f"{s.id}: build_args produced unknown args {unknown}"


def test_every_declared_filter_is_actually_used():
    for s in deep_search_sources.SOURCES:
        if not s.tool:
            continue
        base = s.build_args("בדיקה", 10, {})
        for f in s.filters:
            val = "2020" if f.type == "number" else "2020-01-01" if f.type == "date" else "x"
            assert s.build_args("בדיקה", 10, {f.id: val}) != base, (
                f"{s.id}: filter {f.id} is declared to the user but never sent")


def test_card_caps_unbounded_free_text():
    """Real rows carry unbounded free text — one Ocal event's participants list
    is ~1,200 chars — and an uncapped snippet blows out its column."""
    c = Card(title="t" * 500, snippet="s" * 5000, badges=("b" * 200, ""))
    assert len(c.title) <= Card.MAX_TITLE
    assert len(c.snippet) <= Card.MAX_SNIPPET
    assert all(len(b) <= Card.MAX_BADGE for b in c.badges)
    assert "" not in c.badges
    # Whitespace is collapsed too — these corpora are full of \r\n runs.
    assert "\n" not in Card(title="a\r\n\r\n   b").title


def test_local_value_error_message_is_surfaced(stub_local):
    """The MCP tools raise ValueError with user-facing Hebrew; show it."""
    stub_local["payloads"]["search_datasets"] = ValueError("מסד הנתונים אינו מוגדר בשרת")
    r = _client().get("/api/deep-search/search", params={"q": "x", "sources": "datasets"})
    assert r.json()["sources"][0]["error"] == "מסד הנתונים אינו מוגדר בשרת"


def test_normalizers_are_total():
    """An empty/garbage row must yield None, never raise — one bad row may not
    empty an otherwise good column."""
    for s in deep_search_sources.SOURCES:
        if s.normalize is None:
            continue
        assert s.normalize({}) is None, s.id


# ── the /sources endpoint ───────────────────────────────────────────────────

def test_sources_lists_every_active_source_and_leaks_no_secret(monkeypatch):
    monkeypatch.setenv("TAGIT_MCP_TOKEN", "super-secret-value")
    r = _client().get("/api/deep-search/sources")
    assert r.status_code == 200
    body = r.json()
    assert {s["id"] for s in body["sources"]} == {
        s.id for s in deep_search_sources.active_sources()}
    assert "super-secret-value" not in r.text
    for s in body["sources"]:
        assert set(s) >= {"id", "name", "color", "attribution", "server", "configured"}


def test_local_sources_are_always_configured():
    for s in deep_search_sources.active_sources():
        if s.local:
            assert deep_search.is_configured(s) is True


def test_remote_source_without_token_is_unconfigured():
    remote = deep_search_sources.Source(
        id="x", name="x", color="#000000", attribution={"text": "t", "href": "https://x"},
        mcp_url="https://example.org/mcp", token_env="NO_SUCH_TOKEN_ENV",
        tool="search", build_args=lambda q, n, f: {}, normalize=lambda r: None)
    assert deep_search.is_configured(remote) is False


# ── the /search endpoint ────────────────────────────────────────────────────

def test_one_dead_source_does_not_sink_the_others(stub_local):
    stub_local["payloads"]["search_datasets"] = {
        "items": [{"title": "מאגר בדיקה", "organization": "org", "page_url": "https://o/1"}]}
    stub_local["payloads"]["search"] = RuntimeError("cbs exploded")

    r = _client().get("/api/deep-search/search", params={"q": "תקציב", "sources": "datasets,cbs"})
    assert r.status_code == 200
    cols = {c["id"]: c for c in r.json()["sources"]}
    assert cols["datasets"]["results"][0]["title"] == "מאגר בדיקה"
    assert cols["datasets"]["error"] is None
    assert cols["cbs"]["error"]
    assert cols["cbs"]["results"] == []


def test_slow_source_times_out_into_its_own_column(stub_local, monkeypatch):
    monkeypatch.setattr(deep_search.settings, "deep_search_source_timeout", 0.05)

    async def _slow(_args):
        await asyncio.sleep(5)
        return {"items": []}

    stub_local["payloads"]["search_datasets"] = _slow
    r = _client().get("/api/deep-search/search", params={"q": "תקציב", "sources": "datasets"})
    assert r.status_code == 200
    col = r.json()["sources"][0]
    assert "בזמן" in col["error"]


def test_results_path_is_per_source(stub_local):
    """ocal answers under "events" and sql under "tables". Reading "items"
    everywhere would show empty columns that look like "no results"."""
    stub_local["payloads"]["search_events"] = {
        "events": [{"title": "פגישה", "location": "ירושלים",
                    "links": {"ocal_view": "https://o/ocal"}}],
        # An "items" key that must be IGNORED for this source.
        "items": [{"title": "לא אמור להופיע"}],
    }
    stub_local["payloads"]["list_tables"] = {
        "tables": [{"table": "append_x", "schema": "public", "title": "טבלה"}]}

    r = _client().get("/api/deep-search/search", params={"q": "x", "sources": "ocal,tables"})
    cols = {c["id"]: c for c in r.json()["sources"]}
    assert [c["title"] for c in cols["ocal"]["results"]] == ["פגישה"]
    assert cols["tables"]["results"][0]["title"] == "append_x"


def test_one_malformed_row_does_not_empty_the_column(stub_local):
    def _picky(row):
        if row.get("title") == "רע":
            raise ValueError("boom")
        return Card(title=row["title"])

    src = deep_search_sources.Source(
        id="picky", name="picky", color="#000000",
        attribution={"text": "t", "href": "https://x"},
        local="over", tool="search_datasets", results_path="items",
        build_args=lambda q, n, f: {"query": q}, normalize=_picky)
    stub_local["payloads"]["search_datasets"] = {
        "items": [{"title": "טוב"}, {"title": "רע"}, {"title": "גם טוב"}]}

    col = asyncio.run(deep_search.run_source(None, src, "x", 10))
    assert [c["title"] for c in col["results"]] == ["טוב", "גם טוב"]
    assert col["error"] is None


def test_declared_filter_reaches_build_args(stub_local):
    stub_local["payloads"]["search_datasets"] = {"items": []}
    _client().get("/api/deep-search/search",
                  params={"q": "x", "sources": "datasets", "f_source_type": "ckan"})
    tool, args = stub_local["calls"][-1]
    assert tool == "search_datasets"
    assert args["source_type"] == "ckan"


def test_undeclared_filter_is_ignored(stub_local):
    stub_local["payloads"]["search_datasets"] = {"items": []}
    _client().get("/api/deep-search/search",
                  params={"q": "x", "sources": "datasets", "f_run_sql": "DROP TABLE x"})
    _, args = stub_local["calls"][-1]
    assert "run_sql" not in args
    assert "DROP TABLE x" not in str(args)


def test_limit_is_clamped(stub_local):
    stub_local["payloads"]["search_datasets"] = {"items": []}
    r = _client().get("/api/deep-search/search",
                      params={"q": "x", "sources": "datasets", "limit": 999})
    assert r.status_code == 422  # Query(le=50) rejects it outright

    _client().get("/api/deep-search/search",
                  params={"q": "x", "sources": "datasets", "limit": 50})
    _, args = stub_local["calls"][-1]
    assert args["limit"] <= deep_search.settings.deep_search_max_limit


def test_blank_and_oversized_queries_are_rejected():
    c = _client()
    assert c.get("/api/deep-search/search", params={"q": "   "}).status_code == 400
    assert c.get("/api/deep-search/search",
                 params={"q": "א" * 500, "sources": "datasets"}).status_code == 400


def test_unknown_source_id_is_a_400_not_a_silent_all_search():
    r = _client().get("/api/deep-search/search", params={"q": "x", "sources": "nope"})
    assert r.status_code == 400


def test_disabled_by_settings(monkeypatch):
    monkeypatch.setattr(deep_search_api.settings, "deep_search_enabled", False)
    c = _client()
    assert c.get("/api/deep-search/sources").status_code == 503
    assert c.get("/api/deep-search/search", params={"q": "x"}).status_code == 503


def test_both_routes_have_a_rate_limit():
    app = FastAPI()
    app.include_router(deep_search_api.router)
    paths = {r.path for r in app.routes if hasattr(r, "endpoint")}
    limited = {
        r.path for r in app.routes
        if hasattr(r, "endpoint") and hasattr(r.endpoint, "_rate_limit_exceeded_handler")
        or (hasattr(r, "endpoint") and getattr(r.endpoint, "__wrapped__", None) is not None)
    }
    assert {"/api/deep-search/sources", "/api/deep-search/search"} <= paths
    assert {"/api/deep-search/sources", "/api/deep-search/search"} <= limited


# ── remote transport ────────────────────────────────────────────────────────

class _FakeResponse:
    def __init__(self, status_code=200, content_type="application/json", payload=None, text=""):
        self.status_code = status_code
        self.headers = {"content-type": content_type}
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


class _FakeClient:
    """Stands in for httpx.AsyncClient; serves a scripted list of responses."""
    script: list = []
    posts: list = []

    def __init__(self, *a, **kw):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def post(self, url, headers=None, json=None):
        _FakeClient.posts.append((url, headers, json))
        item = _FakeClient.script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def _remote_source(**kw):
    return deep_search_sources.Source(
        id="r", name="r", color="#000000", attribution={"text": "t", "href": "https://x"},
        mcp_url="https://example.org/mcp", tool="search", results_path="items",
        build_args=lambda q, n, f: {"q": q}, normalize=lambda r: None, **kw)


def test_remote_parses_plain_json(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(payload={
        "result": {"content": [{"type": "text", "text": '{"items": [{"title": "a"}]}'}]}})]
    call = deep_search._remote_caller(_remote_source(), "tok", 5.0)
    assert asyncio.run(call("search", {}))["items"][0]["title"] == "a"


def test_remote_parses_sse_frame(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(
        content_type="text/event-stream",
        text='event: message\ndata: {"result": {"structuredContent": {"items": [{"title": "b"}]}}}\n')]
    call = deep_search._remote_caller(_remote_source(), "tok", 5.0)
    assert asyncio.run(call("search", {}))["items"][0]["title"] == "b"


def test_remote_401_is_not_retried(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(status_code=401)]
    _FakeClient.posts = []
    call = deep_search._remote_caller(_remote_source(), "bad", 5.0)
    with pytest.raises(deep_search.SourceError):
        asyncio.run(call("search", {}))
    assert len(_FakeClient.posts) == 1


def test_remote_gives_up_inside_its_budget(monkeypatch):
    """Unlike tagit_mcp's 100s wake budget, a source here must fail fast — the
    user is watching seven columns."""
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(status_code=503) for _ in range(20)]
    _FakeClient.posts = []
    call = deep_search._remote_caller(_remote_source(), "tok", 2.0)
    loop_start = asyncio.run(_timed(call))
    assert loop_start < 6.0


async def _timed(call):
    import time
    t0 = time.monotonic()
    try:
        await call("search", {})
    except deep_search.SourceError:
        pass
    return time.monotonic() - t0
