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
import time
from dataclasses import replace

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
    has to be able to tell what OVER produced from what it merely surfaces.

    ``external`` and ``local`` are INDEPENDENT axes and this test used to
    conflate them: external described whose data it is, local describes how the
    tool is reached, and until odata every external source happened to be remote
    too. מידע לעם is a sibling project's catalog reached through OVER's own
    in-process pass-through server — external data, local transport. What must
    hold is the attribution, not the transport.
    """
    external = [s for s in deep_search_sources.SOURCES if s.external]
    assert {s.id for s in external} == {
        "mevaker", "protocols_text", "mmm_text", "gov_decisions", "obudget", "odata"}
    for s in external:
        assert "חיצוני" in s.attribution["text"], f"{s.id} attribution hides its origin"
        assert s.attribution["href"], f"{s.id} does not link back to its owner"
        # Whoever owns the data, the link must leave OVER — pointing an external
        # corpus at our own domain would claim it as ours.
        assert "over.org.il" not in s.attribution["href"], s.id
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


def test_sources_never_waits_on_a_cold_tagit(monkeypatch):
    """/sources gates the search button, so it must not await a third party.

    The coverage labels put a TAG-IT call on this request, and tagit_mcp will
    retry a spun-down Render service for up to 100s — which left the שאלות לעם
    button disabled with a spinner cursor until TAG-IT woke up. The fetch is now
    a shared background task the request merely watches for a moment.
    """
    import asyncio as aio

    from app.services import tagit_meta

    tagit_meta.reset_for_tests()

    async def _slow(tool, args):
        await aio.sleep(0.30)                     # a cold upstream, in miniature
        return {"scopes": [{"id": 13, "doc_count": 9074}]}

    monkeypatch.setattr(tagit_meta, "_call", _slow)

    async def go():
        loop = aio.get_running_loop()
        t0 = loop.time()
        out = await tagit_meta.scopes(max_wait=0.05)
        waited = loop.time() - t0
        # The timed-out wait must NOT have cancelled the fetch — shield keeps it
        # alive, so letting the loop run a little longer finds the cache warm.
        # (Checked here, inside the loop: asyncio.run cancels pending tasks on
        # teardown, which would look like a shield failure and is not one.)
        alive = not tagit_meta._cache._task.cancelled()
        await aio.sleep(0.45)
        return out, waited, alive, dict(tagit_meta._cache.value)

    out, waited, alive, later = aio.run(go())
    assert out == {}, "a cold upstream must yield no labels, not a hang"
    assert waited < 0.25, f"the request waited {waited:.2f}s on a third party"
    assert alive, "the bounded wait cancelled the fetch instead of shielding it"
    assert 13 in later, "the shielded fetch never landed, so it is pure waste"

    # A warm cache is returned immediately and never re-fetches.
    tagit_meta._cache._value, tagit_meta._cache._at = {13: {"doc_count": 1}}, time.time()
    tagit_meta._cache._task = None
    assert aio.run(tagit_meta.scopes(max_wait=0.01)) == {13: {"doc_count": 1}}
    assert tagit_meta._cache._task is None, "a fresh cache must not spawn a fetch"


def test_the_sources_endpoint_answers_even_when_tagit_is_down(monkeypatch):
    """End to end: TAG-IT unreachable ⇒ the registry still renders."""
    from app.services import tagit_meta

    tagit_meta.reset_for_tests()

    async def _boom(tool, args):
        raise RuntimeError("TAG-IT is asleep")

    monkeypatch.setattr(tagit_meta, "_call", _boom)
    monkeypatch.setattr(tagit_meta, "warm_in_background", lambda ids: None)

    r = _client().get("/api/deep-search/sources")
    assert r.status_code == 200
    ids = {s["id"] for s in r.json()["sources"]}
    assert {"mevaker", "protocols_text", "datasets", "odata"} <= ids


async def _noop_describe(sources):
    """Skip the TAG-IT enrichment in tests that are about something else."""
    return None


def test_no_card_leads_with_a_machine_identifier():
    """A heading is the one line a reader scans, so it has to mean something.

    The טבלאות column led with the physical table name — "govmap_22_bd519a1c
    _f6a7046f" — while "שכונות — המרכז למיפוי ישראל" sat in the preview below
    it. The identifier is what you type into /data, so it is still shown; it is
    just not what decides whether the row is worth reading.
    """
    import re

    card = deep_search_sources._n_table({
        "table": "govmap_22_bd519a1c_f6a7046f",
        "title": "שכונות — המרכז למיפוי ישראל",
        "organization": "israel_mapping_center",
        "schema": "idx", "est_rows": 5863})
    assert card.title == "שכונות — המרכז למיפוי ישראל"
    assert "govmap_22_bd519a1c_f6a7046f" in card.badges, (
        "the actionable identifier must survive somewhere")

    # No title at all ⇒ fall back to the identifier rather than render blank.
    assert deep_search_sources._n_table({"table": "append_x"}).title == "append_x"

    # And the rule holds for every normalizer given a realistic row: an
    # ascii-only slug/hash heading is a bug wherever it appears.
    machine = re.compile(r"^[A-Za-z0-9_\-\.]+$")
    rows = {
        "tables": {"table": "append_x", "title": "רישום קבלנים", "schema": "public"},
        "datasets": {"title": "רישום קבלנים", "organization": "משרד הבינוי",
                     "page_url": "https://o/1"},
        "odata": {"title": "תגובת העירייה", "organization": "עמותת הצלחה",
                  "url": "https://www.odata.org.il/dataset/x"},
    }
    for sid, row in rows.items():
        src = deep_search_sources.source_by_id(sid)
        card = src.normalize(row)
        assert card and not machine.match(card.title), f"{sid} leads with {card.title!r}"


def test_the_publisher_filter_is_read_from_the_catalog_not_hardcoded(monkeypatch):
    """44 publishers exist; the filter shipped with 8, hiding the other 36.

    A select that cannot express most of its own domain is a broken control,
    not a shortcut — and the shortlist was wrong the day it was written, like
    every other hardcoded list on this site.
    """
    from app.services import odata_meta

    odata_meta.reset_for_tests()

    async def _catalog():
        return [
            {"id": "hatzlacha", "title": "עמותת הצלחה", "datasets": 6063},
            {"id": "zomer", "title": "גיא זומר", "datasets": 3449},
            {"id": "nadavglaw", "title": "נדב גדליהו", "datasets": 41},
        ]

    monkeypatch.setattr(odata_meta._cache, "_fetch", _catalog)
    monkeypatch.setattr(deep_search_api, "_describe_full_text_sources",
                        _noop_describe)

    r = _client().get("/api/deep-search/sources")
    assert r.status_code == 200
    odata = next(s for s in r.json()["sources"] if s["id"] == "odata")
    opt = next(f for f in odata["filters"] if f["id"] == "organization")["options"]
    assert opt[0] == {"value": "", "label": "כל הגופים"}
    values = [o["value"] for o in opt]
    # A publisher absent from the seeded shortlist must still be selectable.
    assert "nadavglaw" in values, "the live catalog did not reach the filter"
    # The count rides along, because it tells the reader whether narrowing to
    # this body leaves them anything.
    assert any("6,063" in o["label"] for o in opt)


def test_the_publisher_filter_degrades_to_a_shortlist_not_to_nothing(monkeypatch):
    """An unreachable catalog costs options, never the control itself."""
    from app.services import odata_meta

    odata_meta.reset_for_tests()

    async def _down():
        raise RuntimeError("odata unreachable")

    monkeypatch.setattr(odata_meta._cache, "_fetch", _down)
    monkeypatch.setattr(deep_search_api, "_describe_full_text_sources",
                        _noop_describe)

    r = _client().get("/api/deep-search/sources")
    assert r.status_code == 200
    odata = next(s for s in r.json()["sources"] if s["id"] == "odata")
    opt = next(f for f in odata["filters"] if f["id"] == "organization")["options"]
    values = {o["value"] for o in opt}
    assert "" in values and "hatzlacha" in values, "the filter lost its options"
    assert len(opt) == len(deep_search_sources.ODATA_ORGS) + 1


def test_a_bounded_cache_answers_fast_and_keeps_the_fetch_alive():
    """The shared primitive behind both metadata reads.

    Two numbers that must stay separate: how long the fetch takes, and how long
    a request waits for it. Merging them is what put a third party's cold start
    on the search page.
    """
    import asyncio as aio

    from app.services.refresh_cache import BoundedRefreshCache

    async def go():
        calls = []

        async def slow():
            calls.append(1)
            await aio.sleep(0.30)
            return {"a": 1}

        c = BoundedRefreshCache("t", slow, ttl_seconds=60, empty={},
                                default_max_wait=0.05)
        loop = aio.get_running_loop()
        t0 = loop.time()
        first = await c.get()
        waited = loop.time() - t0
        alive = not c._task.cancelled()
        await aio.sleep(0.45)
        return first, waited, alive, c.value, len(calls)

    first, waited, alive, later, n = aio.run(go())
    assert first == {} and waited < 0.25, "the reader was not bounded"
    assert alive, "the bounded wait cancelled the fetch instead of shielding it"
    assert later == {"a": 1}, "the shielded fetch never landed"
    assert n == 1, "each waiter started its own fetch"


def test_a_bounded_cache_never_overwrites_good_data_with_an_empty_answer():
    """An upstream answering with nothing is having a bad day, not reporting
    that it lost all of its content."""
    import asyncio as aio

    from app.services.refresh_cache import BoundedRefreshCache

    async def go():
        state = {"give": {"a": 1}}

        async def fetch():
            return state["give"]

        c = BoundedRefreshCache("t", fetch, ttl_seconds=0, empty={})
        await c.get(max_wait=None)
        state["give"] = {}                       # upstream goes quiet
        await c.get(max_wait=None)
        return c.value

    assert aio.run(go()) == {"a": 1}


def test_odata_is_dispatched_in_process_and_declared_external():
    """"local" means the TOOL is in-process, not that the data is ours.

    odata_server is a pass-through to odata.org.il's CKAN. Dispatching it
    locally saves a token and a cold start; the `external` flag is what tells
    the reader the corpus is a sibling project's, not OVER's.
    """
    from app.mcp import odata_server

    src = deep_search_sources.source_by_id("odata")
    assert src.local == "odata"
    assert deep_search.LOCAL_MODULES["odata"] == "app.mcp.odata_server"
    assert src.tool in deep_search.ALLOWED_TOOLS
    assert src.tool in odata_server._IMPL, "registry names a tool the server lacks"
    assert src.external is True, "a sibling project's catalog must be badged external"
    # The attribution has to say the files are not ours — this corpus is
    # processed FOI responses, and implying OVER holds them would be false.
    assert "odata.org.il" in src.attribution["href"]
    assert "מעובד" in src.attribution["text"]


def test_odata_card_credits_the_publisher_without_claiming_it_is_the_source():
    """The badge is the requester, not the authority that produced the data.

    On this catalog the publishing organization is whoever filed the freedom-of
    -information request. Labelling that as the document's source would credit
    a ministry's data to the NGO that obtained it.
    """
    card = deep_search_sources._n_odata({
        "title": "מכרז לשירותי מחשוב", "notes": "מכרז מספר 13-2014",
        "organization": "עמותת הצלחה", "num_resources": 2,
        "url": "https://www.odata.org.il/dataset/contract-13-2014",
        "metadata_modified": "2019-03-04T10:00:00",
        "resources": [{"format": "PDF"}, {"format": "XLSX"}]})
    assert card.url.startswith("https://www.odata.org.il/dataset/")
    assert "עמותת הצלחה" in card.badges
    assert card.date == "2019-03-04"
    # A dataset with no title is dropped rather than rendered as an empty card.
    assert deep_search_sources._n_odata({"notes": "x"}) is None
    # Singular/plural on the file count.
    one = deep_search_sources._n_odata(
        {"title": "א", "num_resources": 1, "resources": [{"format": "CSV"}]})
    assert "קובץ אחד" in one.badges


def test_odata_search_requires_a_query_and_clamps_the_page_size():
    """A catalog of 11.5k datasets must not be exportable through this tool."""
    import asyncio

    from app.mcp import odata_server

    assert odata_server.MAX_ROWS == 50
    assert odata_server._clamp(999, 1, 50, 20) == 50
    assert odata_server._clamp(0, 1, 50, 20) == 1
    assert odata_server._clamp("junk", 1, 50, 20) == 20
    with pytest.raises(ValueError, match="חסר טקסט"):
        asyncio.run(odata_server._tool_search(None, None, None, {"query": "  "}))
    # A slug with a dash must reach CKAN quoted, or fq reads it as two terms.
    assert odata_server.dataset_url({"name": "contract-13-2014"}).endswith(
        "/dataset/contract-13-2014")
    # Prefer the slug over the uuid — same dataset, readable address.
    assert odata_server.dataset_url(
        {"name": "amidar-shivook", "id": "4031842c"}).endswith("/dataset/amidar-shivook")


def test_an_isError_result_raises_instead_of_normalizing_to_no_results():
    """An isError result carries a MESSAGE where the payload would be.

    Digging for `items` in it finds nothing and yields an empty column, so a
    filter rejection or a query timeout renders as "this corpus has nothing".
    TAG-IT flagged this as the one remaining data-losing bug on our side.
    """
    from app.services import tagit_mcp

    err = {"isError": True,
           "content": [{"type": "text", "text": "statement timeout after 25s"}]}

    with pytest.raises(deep_search.SourceError, match="statement timeout"):
        deep_search._tool_payload(err)
    with pytest.raises(tagit_mcp.DeepSearchError, match="statement timeout"):
        tagit_mcp._tool_payload(err)

    # An isError with no text part must still raise rather than return {}.
    with pytest.raises(deep_search.SourceError):
        deep_search._tool_payload({"isError": True, "content": []})
    # And a normal result is untouched.
    assert deep_search._tool_payload(
        {"content": [{"type": "text", "text": '{"items": [1]}'}]}) == {"items": [1]}


def test_an_unknown_total_stays_null_and_never_becomes_the_page_size():
    """null total means "not counted", which is not the same as zero.

    We ask TAG-IT for total_mode=skip (it halves the latency), so the count is
    absent by design. Substituting len(cards) would state the page size as the
    whole answer — the "2 of 234" lie, inverted.
    """
    src = next(s for s in deep_search_sources.SOURCES if s.id == "mevaker")
    cards = [Card(title="דוח א"), Card(title="דוח ב")]

    unknown = deep_search._column(src, results=cards, total_unknown=True)
    assert unknown["total"] is None
    assert len(unknown["results"]) == 2

    # Without the flag the old fallback still applies, for sources that do count.
    assert deep_search._column(src, results=cards)["total"] == 2
    assert deep_search._column(src, results=cards, total=234)["total"] == 234
    # Zero must stay a real, reportable zero.
    assert deep_search._column(src, results=[], total=0)["total"] == 0


def test_tagit_asks_for_the_count_to_be_skipped():
    import inspect
    src = inspect.getsource(deep_search_sources._tagit_runner)
    assert '"total_mode": "skip"' in src


def test_highlight_offsets_survive_a_document_containing_the_marker():
    """The reason to take offsets over a pre-marked string.

    Once « is in the text, the marker and the content are the same character
    and no parser can separate them. Offsets are immune, provided the clean
    text is neutralized before insertion — and that the insertion runs
    backwards, or each mark shifts every span still to be applied.
    """
    from app.services import deep_search_query as dsq

    # "אמר «כך» ותקציב" — the span covers "ותקציב" at offset 9, and the offsets
    # are stated against the CLEAN text, so the document's own « must not move
    # them. Neutralizing is length-preserving for exactly that reason.
    out = dsq.mark_from_spans("אמר «כך» ותקציב", [{"start": 9, "length": 6}])
    assert out == "אמר ‹כך› «ותקציב»"

    two = dsq.mark_from_spans("תקציב וגם ביטחון",
                              [{"start": 0, "length": 5}, {"start": 10, "length": 6}])
    assert two == "«תקציב» וגם «ביטחון»"      # second span not shifted by the first

    # Overlapping spans merge instead of nesting into unbalanced markers.
    assert dsq.mark_from_spans("תקציב", [{"start": 0, "length": 3},
                                         {"start": 1, "length": 4}]) == "«תקציב»"
    # Nothing usable ⇒ None, so the caller falls back to the pre-marked snippet.
    assert dsq.mark_from_spans("תקציב", []) is None
    assert dsq.mark_from_spans("", [{"start": 0, "length": 2}]) is None
    assert dsq.mark_from_spans("תקציב", [{"start": 99, "length": 2}]) is None
    assert dsq.mark_from_spans("תקציב", ["junk", {"start": "x"}]) is None


def test_a_card_links_to_the_publisher_not_to_the_sso_viewer():
    """TAG-IT's `link` is anchored to the passage but sits behind SSO, and this
    page is anonymous — a link that demands a login is a dead end."""
    card = deep_search_sources._n_tagit({
        "title": "דוח שנתי", "link": "https://tag-it.biz/doc/1?anchor=5",
        "source_url": "https://library.mevaker.gov.il/x.pdf"})
    assert card.url == "https://library.mevaker.gov.il/x.pdf"
    # Fall back to link when the publisher URL is absent.
    assert deep_search_sources._n_tagit(
        {"title": "דוח", "link": "https://tag-it.biz/doc/1"}).url == "https://tag-it.biz/doc/1"


def test_every_full_text_source_declares_the_backends_own_cut_off():
    """Without upstream_timeout_s a silent upstream timeout reads as "no hits"."""
    by_id = {s.id: s for s in deep_search_sources.SOURCES}
    for sid in ("mevaker", "protocols_text", "mmm_text", "gov_decisions"):
        assert by_id[sid].upstream_timeout_s == 25.0, sid
        # Ours must stay ABOVE theirs, or we would cut the query off before the
        # backend has had its own budget and never see the empty-plus-slow case.
        assert by_id[sid].timeout_s > by_id[sid].upstream_timeout_s, sid


def test_a_slow_empty_answer_is_reported_as_unfinished_not_as_zero_hits():
    """The measured contradiction: 0 hits in 26.1s next to 2,103 hits in 1.3s.

    A genuine zero loads no candidate documents, so it is fast. An empty answer
    that consumed most of the backend's own 25s budget ran out of time, and
    saying "אין תוצאות" there tells the reader the documents do not exist.
    """
    src = next(s for s in deep_search_sources.SOURCES if s.id == "mevaker")
    empty, full = {"results": []}, {"results": [{"title": "דוח"}]}

    assert deep_search._empty_because_it_gave_up(src, empty, 26.1) is True
    # Fast and empty is an honest zero — the common case, and it must stay quiet.
    assert deep_search._empty_because_it_gave_up(src, empty, 1.3) is False
    # Slow but not empty is just a heavy query that finished.
    assert deep_search._empty_because_it_gave_up(src, full, 26.1) is False
    # A source that never told us its cut-off gets no inference drawn about it.
    metadata_col = next(s for s in deep_search_sources.SOURCES if s.id == "datasets")
    assert deep_search._empty_because_it_gave_up(metadata_col, empty, 99.0) is False


def test_the_unfinished_search_surfaces_as_an_error_on_that_column(
        stub_local, monkeypatch):
    """End to end: an empty-and-slow column must not render as "no results"."""
    src = next(s for s in deep_search_sources.SOURCES if s.id == "datasets")
    monkeypatch.setattr(deep_search_sources, "SOURCES", tuple(
        replace(s, upstream_timeout_s=0.05) if s.id == "datasets" else s
        for s in deep_search_sources.SOURCES))

    async def _slow_and_empty(_args):
        await asyncio.sleep(0.2)
        return {"items": []}

    stub_local["payloads"]["search_datasets"] = _slow_and_empty
    r = _client().get("/api/deep-search/search",
                      params={"q": "ביטחון", "sources": "datasets"})
    col = r.json()["sources"][0]
    assert col["results"] == []
    assert "לא הושלם" in (col["error"] or ""), col
    assert src.upstream_timeout_s is None      # the real registry is untouched


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
    # Read from "tables", not "items" — the point of this test. The heading is
    # the human title; the physical name rides along as a badge.
    assert cols["tables"]["results"][0]["title"] == "טבלה"
    assert "append_x" in cols["tables"]["results"][0]["badges"]


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
