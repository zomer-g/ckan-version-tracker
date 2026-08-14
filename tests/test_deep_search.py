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


def test_iso_date_reads_each_corpus_the_way_that_corpus_writes_it():
    """A silently transposed date is the worst kind of wrong — it still sorts
    and still renders. The מבקר library writes DD.MM.YYYY; the cooperatives
    feed writes US M/D/YYYY."""
    d = deep_search_sources.iso_date
    assert d("08.05.2018") == "2018-05-08"      # 8 May, not 5 August
    assert d("26.07.2026") == "2026-07-26"
    assert d("3/1/2020 12:00:00 AM") == "2020-03-01"   # US: 1 March
    assert d("25/12/2021") == "2021-12-25"      # first number can't be a month
    assert d("2019-06-24T10:00:00") == "2019-06-24"
    assert d("") is None and d(None) is None
    assert d("לא ידוע") is None


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


def test_external_sources_are_declared_and_attributed():
    """Someone else's corpus must SAY so and link back to the owner — a reader
    has to be able to tell what OVER produced from what it merely surfaces."""
    external = [s for s in deep_search_sources.SOURCES if s.external]
    assert {s.id for s in external} == {
        "mevaker", "protocols_text", "mmm_text", "gov_decisions", "obudget"}
    for s in external:
        assert s.local is None, f"{s.id} is marked external but dispatches locally"
        assert "חיצוני" in s.attribution["text"], f"{s.id} attribution hides its origin"
    ob = deep_search_sources.source_by_id("obudget")
    # The brief was explicit: say it is external AND processed, and link out.
    assert "מעובד" in ob.attribution["text"]
    assert "obudget.org" in ob.attribution["href"]


def test_idx_search_rejects_identifiers_it_did_not_whitelist():
    """idx_text_search builds its own SQL, so the identifiers must be proven
    safe: everything but [a-z0-9_] is refused before a statement is formed.
    The search term itself is always a bound parameter, never interpolated."""
    for bad in ('mevaker"; DROP TABLE x --', "public.users", "a-b", "Tbl", "x;y", ""):
        with pytest.raises(deep_search.SourceError):
            asyncio.run(deep_search.idx_text_search(bad, ("title",), ("title",), "1", "q", 5))
    with pytest.raises(deep_search.SourceError):
        asyncio.run(deep_search.idx_text_search(
            "ok_table", ('title" , (select 1) as x --',), ("title",), "1", "q", 5))


def test_mevaker_catalog_targets_idx_tables_not_dataset_ids():
    """query_dataset_rows cannot reach these corpora — they have no append_
    table and it answers zero rows without erroring. The registry must address
    them by mirror table."""
    for spec in deep_search_sources.MEVAKER_DATASETS:
        assert spec["table"].startswith("mevaker_"), spec
        assert deep_search._IDX_TABLE_RE.match(spec["table"]), spec
    opts = {o["value"] for o in deep_search_sources.MEVAKER_FILTER.options if o["value"]}
    assert opts == {s["table"] for s in deep_search_sources.MEVAKER_DATASETS}


def test_operators_parse_into_the_right_buckets():
    from app.services import deep_search_query as dsq
    p = dsq.parse('"תקציב הביטחון" חינוך -ירושלים')
    assert p.phrases == ("תקציב הביטחון",)
    assert p.terms == ("חינוך",)
    assert p.excludes == ("ירושלים",)
    # The anchor is what a backend with no operator support gets: the phrase,
    # because an exact run of text is far narrower than any single word.
    assert p.anchor() == "תקציב הביטחון"
    assert p.has_operators


def test_or_is_not_silently_turned_into_and():
    """`a OR b` widens. Enforcing every positive on a source that cannot
    express OR would invert the operator into a narrowing one."""
    from app.services import deep_search_query as dsq
    p = dsq.parse("תקציב OR גירעון")
    assert p.has_or and not p.enforce_positives
    assert dsq.matches(p, "רק גירעון מוזכר כאן") is True
    assert dsq.matches(p, "שום דבר רלוונטי") is False
    # ...while a plain AND query still requires everything.
    q = dsq.parse("תקציב גירעון")
    assert dsq.matches(q, "רק גירעון מוזכר כאן") is False


def test_exclusion_and_phrase_semantics():
    from app.services import deep_search_query as dsq
    ex = dsq.parse("תקציב -ביטחון")
    assert dsq.matches(ex, "תקציב החינוך") is True
    assert dsq.matches(ex, "תקציב הביטחון") is False
    ph = dsq.parse('"תקציב הביטחון"')
    assert dsq.matches(ph, "דיון על תקציב הביטחון היום") is True
    assert dsq.matches(ph, "תקציב וגם הביטחון, בנפרד") is False


def test_grouping_is_dropped_not_forwarded():
    """Parentheses do NOT group on the full-text backend (measured). Passing
    them through would mean the user thinks they grouped and they did not."""
    from app.services import deep_search_query as dsq
    p = dsq.parse("(תקציב OR גירעון) חינוך")
    assert p.dropped_grouping
    assert "(" not in dsq.native_query(p) and ")" not in dsq.native_query(p)


def test_snippet_is_a_window_around_the_match():
    from app.services import deep_search_query as dsq
    p = dsq.parse("הביטחון")
    long = "מבוא ארוך מאוד. " * 40 + "כאן דנים על הביטחון בהרחבה. " + "סיום ארוך. " * 40
    sn = dsq.snippet_around(p, long, width=120)
    assert sn and "«הביטחון»" in sn
    assert len(sn) <= 160, "the window must stay a window"
    assert sn.startswith("…") and sn.endswith("…"), "both sides were trimmed"
    assert dsq.snippet_around(p, "טקסט בלי שום התאמה") is None


def test_tagit_highlighting_is_never_rebuilt():
    """TAG-IT already marks the match; re-marking would double-wrap it."""
    from app.services import deep_search_query as dsq
    assert dsq.already_highlighted("במשרד «הביטחון» מקבל") is True
    assert dsq.already_highlighted("plain text") is False


def test_truncation_never_leaves_a_dangling_highlight():
    """A cut mid-marker would render as a highlight swallowing the line."""
    c = Card(title="t", snippet="x" * 315 + " «הביטחון» ועוד טקסט")
    assert c.snippet.count("«") == c.snippet.count("»")


def test_full_text_sources_are_operator_native_and_metadata_ones_are_not():
    by_id = {s.id: s for s in deep_search_sources.SOURCES}
    for sid in ("mevaker", "protocols_text", "mmm_text", "gov_decisions"):
        assert by_id[sid].native_operators is True, sid
    for sid in ("datasets", "tables", "cbs", "protocols", "mmm", "ocal",
                "mevaker_reports", "entities", "obudget"):
        assert by_id[sid].native_operators is False, sid


def test_no_source_hardcodes_a_coverage_range():
    """Every hand-written range on these columns has gone stale, twice.

    The מבקר note first said "local government, 2018–2019 only" (the first page
    of one query), was corrected to "1989–2019 · 981 documents", and was stale
    again within two days because TAG-IT re-imported the corpus to 6,168
    documents spanning 1949–2026. Coverage is now read from list_scopes at
    request time and appended to the hint, so there is nothing left to rot.
    """
    import re
    for s in deep_search_sources.SOURCES:
        if not s.scope_setting:
            continue
        text = f"{s.hint} {s.attribution['text']}"
        years = re.findall(r"(?<!\d)(19|20)\d{2}(?!\d)", text)
        assert not years, (
            f"{s.id} states a coverage year by hand: {text!r} — let "
            f"tagit_meta.coverage_label supply it")


def test_the_date_filter_is_declared_but_can_be_withdrawn_by_the_service():
    """The registry states intent; the corpus gets a veto.

    Protocols lost its filter while only 4 of 22,036 documents were dated, and
    got it back once the backfill landed — two manual flips in two days. Now the
    filter is declared once and tagit_meta.dates_usable withdraws it whenever
    the corpus cannot honour it.
    """
    from app.services import tagit_meta
    by_id = {s.id: s for s in deep_search_sources.SOURCES}
    for sid in ("mevaker", "protocols_text", "mmm_text", "gov_decisions"):
        assert {f.id for f in by_id[sid].filters} >= {"date_from", "date_to"}, sid
    assert tagit_meta.dates_usable({"doc_count": 22036, "dated_doc_count": 4,
                                    "has_dates": True}) is False
    assert tagit_meta.dates_usable({"doc_count": 22036, "dated_doc_count": 22036,
                                    "has_dates": True}) is True
    # Unknown must NOT read as False, or an unreachable TAG-IT would strip every
    # date filter on the page.
    assert tagit_meta.dates_usable(None) is None


def test_session_servers_are_flagged():
    """מפתח התקציב refuses a bare tools/call with "Missing session ID"; ours and
    TAG-IT's are stateless. Getting this flag wrong is a dead column."""
    by_id = {s.id: s for s in deep_search_sources.SOURCES}
    assert by_id["obudget"].handshake is True
    assert by_id["mevaker"].handshake is False
    assert all(not s.handshake for s in deep_search_sources.SOURCES if s.local)


def test_obudget_needs_no_token_but_tagit_does(monkeypatch):
    by_id = {s.id: s for s in deep_search_sources.SOURCES}
    assert deep_search.is_configured(by_id["obudget"]) is True
    monkeypatch.delenv("TAGIT_MCP_TOKEN", raising=False)
    monkeypatch.setattr(deep_search.settings, "tagit_mcp_token", "")
    assert deep_search.is_configured(by_id["mevaker"]) is False
    monkeypatch.setattr(deep_search.settings, "tagit_mcp_token", "tok")
    assert deep_search.is_configured(by_id["gov_decisions"]) is True


def test_tagit_sources_use_distinct_scopes():
    from app.config import settings
    scopes = {settings.tagit_mevaker_scope, settings.tagit_mmm_scope,
              settings.tagit_protocols_scope, settings.tagit_gov_decisions_scope}
    assert len(scopes) == 4, f"TAG-IT scopes collide: {scopes}"


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
    call, _ = deep_search._remote_caller(_remote_source(), "tok", 5.0)
    assert asyncio.run(call("search", {}))["items"][0]["title"] == "a"


def test_remote_parses_sse_frame(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(
        content_type="text/event-stream",
        text='event: message\ndata: {"result": {"structuredContent": {"items": [{"title": "b"}]}}}\n')]
    call, _ = deep_search._remote_caller(_remote_source(), "tok", 5.0)
    assert asyncio.run(call("search", {}))["items"][0]["title"] == "b"


def test_remote_401_is_not_retried(monkeypatch):
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(status_code=401)]
    _FakeClient.posts = []
    call, _ = deep_search._remote_caller(_remote_source(), "bad", 5.0)
    with pytest.raises(deep_search.SourceError):
        asyncio.run(call("search", {}))
    assert len(_FakeClient.posts) == 1


def test_remote_gives_up_inside_its_budget(monkeypatch):
    """Unlike tagit_mcp's 100s wake budget, a source here must fail fast — the
    user is watching seven columns."""
    monkeypatch.setattr(httpx, "AsyncClient", _FakeClient)
    _FakeClient.script = [_FakeResponse(status_code=503) for _ in range(20)]
    _FakeClient.posts = []
    call, _ = deep_search._remote_caller(_remote_source(), "tok", 2.0)
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
