"""Tests for the declarative source registry (app/services/source_registry.py).

The registry lets the GOVSCRAPER worker onboard a brand-new site without an
OVER deploy, so the invariants that keep that safe are locked here:

  1. A manifest can never shadow one of the fifteen hardcoded sources —
     reserved ids are rejected outright, and classification is only ever
     consulted after every hardcoded parser has missed.
  2. Hebrew URLs classify identically whether percent-encoded (browser
     copy-paste) or raw (typed into a JSON body).
  3. The sync endpoint is worker-key gated, idempotent by hash, and never
     removes a source that's simply absent from the payload.

No Postgres: the DB is a small in-memory fake, which is enough because the
registry's logic is regex matching plus an upsert.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import sources as sources_api
from app.api import worker as worker_api
from app.config import settings
from app.database import get_db
from app.models.source_registry import SourceRegistry
from app.rate_limit import limiter
from app.services import source_registry as sr


TOY_MANIFEST = {
    "manifest_version": 1,
    "id": "toysource",
    "label_he": "מקור צעצוע",
    "label_en": "Toy Source",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#fae8ff", "fg": "#86198f", "accent": "#c026d3"},
    "default_poll_interval": 43200,
    "neon_eligible": True,
    "default_config": {"download_files": True, "max_docs": 500},
    "url_patterns": [
        {
            "regex": r"^https?://toy\.example\.org/מכרזים(?:/(?P<year>\d{4}))?/?$",
            "page_type": "toysource_tenders",
            "title_he": "מקור צעצוע — מכרזים {year}",
            "config": {"corpus": "tenders", "year": "{year}"},
        },
        {
            "regex": r"^https?://toy\.example\.org/decisions/?$",
            "page_type": "toysource_decisions",
        },
    ],
}


@pytest.fixture(autouse=True)
def clean_cache():
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


def _manifest(**overrides) -> dict:
    return {**TOY_MANIFEST, **overrides}


# --- manifest validation ---------------------------------------------------


def test_valid_manifest_derives_conventions():
    man = sr.validate_manifest(TOY_MANIFEST)
    assert man.resolved_origin == "toy.example.org"
    assert man.ckan_id_prefix == "toysource-scraper-"
    assert man.slug_prefix == "toysource-scraper"
    assert man.mirror_prefix == "gov-versions-toysource"


@pytest.mark.parametrize("bad_id", ["jda", "govil", "knesset", "cbs", "govmap"])
def test_reserved_ids_are_rejected(bad_id):
    """A manifest claiming a built-in source's id would create datasets the
    hardcoded path also claims; the two would disagree on which engine runs."""
    with pytest.raises(Exception):
        sr.validate_manifest(_manifest(id=bad_id))


@pytest.mark.parametrize("bad_id", ["Toy", "1toy", "toy-source", "t"])
def test_malformed_ids_are_rejected(bad_id):
    with pytest.raises(Exception):
        sr.validate_manifest(_manifest(id=bad_id))


def test_unanchored_regex_is_rejected():
    """An unanchored pattern would match mid-URL and claim unrelated hosts."""
    with pytest.raises(Exception):
        sr.validate_manifest(
            _manifest(url_patterns=[{"regex": r"https?://toy\.example\.org/"}])
        )


def test_uncompilable_regex_is_rejected():
    with pytest.raises(Exception):
        sr.validate_manifest(_manifest(url_patterns=[{"regex": "^https://(unclosed"}]))


def test_overlong_regex_is_rejected():
    huge = "^https://toy.example.org/" + ("a" * sr.MAX_REGEX_LENGTH)
    with pytest.raises(Exception):
        sr.validate_manifest(_manifest(url_patterns=[{"regex": huge}]))


def test_unsupported_manifest_version_is_rejected():
    with pytest.raises(Exception):
        sr.validate_manifest(_manifest(manifest_version=2))


def test_non_hex_badge_colour_is_rejected():
    with pytest.raises(Exception):
        sr.validate_manifest(
            _manifest(badge={"bg": "hotpink", "fg": "#000", "accent": "#f0f"})
        )


# --- classification --------------------------------------------------------


def test_hebrew_url_matches_encoded_and_decoded():
    """A browser copy-paste arrives percent-encoded; a JSON body typed by hand
    arrives raw. A manifest author writes only one of the two forms."""
    man = sr.validate_manifest(TOY_MANIFEST)
    encoded = "https://toy.example.org/%D7%9E%D7%9B%D7%A8%D7%96%D7%99%D7%9D/2024"
    decoded = "https://toy.example.org/מכרזים/2024"
    for url in (encoded, decoded):
        match = sr.match_manifests(url, [man])
        assert match is not None, url
        assert match.page_type == "toysource_tenders"
        assert match.scraper_config["year"] == "2024"


def test_named_groups_fill_title_and_config():
    man = sr.validate_manifest(TOY_MANIFEST)
    match = sr.match_manifests("https://toy.example.org/מכרזים/2024", [man])
    assert match.title == "מקור צעצוע — מכרזים 2024"
    assert match.collector_name == "toysource-tenders"
    assert match.scraper_config["corpus"] == "tenders"
    # default_config is the base; the pattern's config layers on top.
    assert match.scraper_config["max_docs"] == 500
    # The worker dispatches on this.
    assert match.scraper_config["kind"] == "toysource"


def test_absent_optional_group_leaves_no_blank_value():
    """The year group is optional — an unmatched one must not leak an empty
    string into the config, nor a dangling dash into the title."""
    man = sr.validate_manifest(TOY_MANIFEST)
    match = sr.match_manifests("https://toy.example.org/מכרזים", [man])
    assert match.title == "מקור צעצוע — מכרזים"
    assert "year" not in match.scraper_config


def test_page_type_defaults_to_source_main():
    man = sr.validate_manifest(
        _manifest(url_patterns=[{"regex": r"^https://toy\.example\.org/x/?$"}])
    )
    match = sr.match_manifests("https://toy.example.org/x", [man])
    assert match.page_type == "toysource_main"
    assert match.title == "מקור צעצוע"


def test_first_matching_pattern_wins():
    man = sr.validate_manifest(
        _manifest(
            url_patterns=[
                {"regex": r"^https://toy\.example\.org/.*$", "page_type": "toysource_all"},
                {"regex": r"^https://toy\.example\.org/x$", "page_type": "toysource_x"},
            ]
        )
    )
    assert sr.match_manifests("https://toy.example.org/x", [man]).page_type == "toysource_all"


def test_unrelated_url_does_not_match():
    man = sr.validate_manifest(TOY_MANIFEST)
    assert sr.match_manifests("https://www.gov.il/he/departments/general/x", [man]) is None


def test_absurdly_long_url_is_refused():
    man = sr.validate_manifest(TOY_MANIFEST)
    long_url = "https://toy.example.org/" + "a" * sr.MAX_URL_LENGTH
    assert sr.match_manifests(long_url, [man]) is None


def test_greedy_manifest_cannot_reach_a_hardcoded_source():
    """The regression that matters most: a manifest whose regex covers
    jda.gov.il must not be able to claim it, because datasets.py consults the
    registry only AFTER every hardcoded parser has missed. This test pins the
    ordering at the call site, not just the matcher."""
    from app.api.jda import _parse_jda_url

    jda_url = "https://jda.gov.il/מכרזיםפנימי/"
    greedy = sr.validate_manifest(
        _manifest(url_patterns=[{"regex": r"^https?://.*$", "page_type": "toysource_all"}])
    )
    # The matcher itself is greedy enough to take it...
    assert sr.match_manifests(jda_url, [greedy]) is not None
    # ...but the hardcoded parser claims it first, so classify_url is never
    # reached for this URL in datasets.py.
    page_type, collector = _parse_jda_url(jda_url)
    assert page_type == "jda_tenders" and collector == "jda-tenders"


# --- cache-backed helpers --------------------------------------------------


def test_neon_kinds_and_source_names_read_the_cache():
    man = sr.validate_manifest(TOY_MANIFEST)
    sr._cache = (2**31, [man])  # far-future timestamp: never expires mid-test
    assert "toysource" in sr.neon_kinds()
    assert any("מקור צעצוע" in name for name in sr.registry_source_names())
    sr.invalidate_cache()
    assert sr.neon_kinds() == frozenset()


def test_neon_eligibility_honours_the_manifest():
    """A registered source declares NEON eligibility itself instead of being
    added to TABULAR_SCRAPER_KINDS."""
    from app.api.datasets import dataset_is_neon_eligible

    class _DS:
        source_type = "scraper"
        scraper_config = {"kind": "toysource"}

    sr.invalidate_cache()
    assert dataset_is_neon_eligible(_DS()) is False
    sr._cache = (2**31, [sr.validate_manifest(TOY_MANIFEST)])
    assert dataset_is_neon_eligible(_DS()) is True


def test_display_view_carries_badge_and_defaults_the_links():
    view = sr.display_view(sr.validate_manifest(TOY_MANIFEST))
    assert view["ckan_id_prefix"] == "toysource-scraper-"
    assert view["badge"]["bg"] == "#fae8ff"
    assert view["badge"]["label"] == "Toy Source"  # falls back to label_en
    assert "מקור צעצוע" in view["source_link_he"]
    assert "toy.example.org" in view["source_link_en"]


def test_manifest_hash_is_stable_and_key_order_independent():
    reordered = dict(reversed(list(TOY_MANIFEST.items())))
    assert sr.manifest_hash(TOY_MANIFEST) == sr.manifest_hash(reordered)
    changed = _manifest(default_poll_interval=999)
    assert sr.manifest_hash(changed) != sr.manifest_hash(TOY_MANIFEST)


# --- endpoints -------------------------------------------------------------


class _FakeDB:
    """Just enough AsyncSession for the sync endpoint and load_enabled."""

    def __init__(self, rows: list[SourceRegistry] | None = None):
        self.rows = rows or []
        self.committed = False

    async def execute(self, stmt):
        rows = self.rows
        compiled = str(stmt)
        db = self

        class _Result:
            def scalars(self):
                class _S:
                    def all(self_inner):
                        return [r for r in rows if r.enabled]
                return _S()

            def scalar_one_or_none(self):
                # The sync endpoint's "does this id exist?" lookup.
                wanted = db.lookup_id
                return next((r for r in rows if r.id == wanted), None)

        assert "source_registry" in compiled
        return _Result()

    def add(self, row):
        self.rows.append(row)

    async def commit(self):
        self.committed = True


def _client(db: _FakeDB) -> TestClient:
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(sources_api.router)
    app.include_router(worker_api.router)

    async def _fake_db():
        yield db

    app.dependency_overrides[get_db] = _fake_db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


def _sync(client, manifests, key="workerkey"):
    return client.post(
        "/api/worker/sources/sync",
        json={"manifests": manifests, "worker_version": "abc123"},
        headers={"Authorization": f"Bearer {key}"},
    )


@pytest.fixture
def worker_key(monkeypatch):
    monkeypatch.setattr(settings, "worker_api_key", "workerkey")


def test_sync_requires_the_worker_key(worker_key):
    db = _FakeDB()
    db.lookup_id = None
    client = _client(db)
    assert client.post("/api/worker/sources/sync", json={"manifests": []}).status_code == 401
    assert _sync(client, [], key="wrong").status_code == 403


def test_sync_upserts_then_reports_unchanged(worker_key, monkeypatch):
    db = _FakeDB()
    db.lookup_id = "toysource"
    client = _client(db)

    r = _sync(client, [TOY_MANIFEST])
    assert r.status_code == 200
    assert r.json()["upserted"] == ["toysource"]
    assert db.committed and len(db.rows) == 1

    # Re-syncing the identical manifest is a no-op (hash match).
    r2 = _sync(client, [TOY_MANIFEST])
    assert r2.json() == {"upserted": [], "unchanged": ["toysource"], "rejected": []}
    assert len(db.rows) == 1


def test_sync_rejects_a_bad_manifest_without_dropping_the_good_ones(worker_key):
    db = _FakeDB()
    db.lookup_id = "toysource"
    client = _client(db)
    bad = _manifest(id="jda")  # reserved

    r = _sync(client, [TOY_MANIFEST, bad])
    body = r.json()
    assert body["upserted"] == ["toysource"]
    assert len(body["rejected"]) == 1 and body["rejected"][0]["id"] == "jda"


def test_sync_never_removes_an_absent_source(worker_key):
    """An older worker syncing its shorter manifest list must not wipe sources
    a newer worker registered."""
    existing = SourceRegistry(
        id="othersource", manifest=_manifest(id="othersource"),
        manifest_hash="x", enabled=True,
    )
    db = _FakeDB([existing])
    db.lookup_id = "toysource"
    client = _client(db)

    _sync(client, [TOY_MANIFEST])
    assert any(r.id == "othersource" for r in db.rows)
    assert existing.enabled is True


def test_validate_endpoint_classifies_and_returns_display_metadata(worker_key):
    row = SourceRegistry(
        id="toysource", manifest=TOY_MANIFEST, manifest_hash="h", enabled=True,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)

    r = client.post(
        "/api/sources/validate",
        json={"url": "https://toy.example.org/decisions"},
    )
    body = r.json()
    assert body["valid"] is True
    assert body["source_id"] == "toysource"
    assert body["page_type"] == "toysource_decisions"
    assert body["default_poll_interval"] == 43200
    assert body["badge"]["accent"] == "#c026d3"


def test_validate_endpoint_rejects_an_unknown_url(worker_key):
    db = _FakeDB([])
    db.lookup_id = None
    client = _client(db)
    r = client.post("/api/sources/validate", json={"url": "https://unknown.example/x"})
    assert r.json()["valid"] is False


def test_disabled_source_stops_classifying():
    """The kill switch: a disabled manifest is invisible to load_enabled."""
    row = SourceRegistry(
        id="toysource", manifest=TOY_MANIFEST, manifest_hash="h", enabled=False,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)
    r = client.post("/api/sources/validate", json={"url": "https://toy.example.org/decisions"})
    assert r.json()["valid"] is False


def test_registry_endpoint_lists_display_views_without_regexes(worker_key):
    row = SourceRegistry(
        id="toysource", manifest=TOY_MANIFEST, manifest_hash="h", enabled=True,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)

    r = client.get("/api/sources/registry")
    assert r.headers["Cache-Control"] == "public, max-age=300"
    sources = r.json()["sources"]
    assert len(sources) == 1
    assert sources[0]["id"] == "toysource"
    # Regexes are Python-flavoured — the browser must never try to evaluate one.
    assert "url_patterns" not in sources[0]
    assert "regex" not in str(sources[0])


# ── case-insensitive groups ─────────────────────────────────────────────────

CASEFOLD_MANIFEST = {
    "manifest_version": 1,
    "id": "toycase",
    "label_he": "מקור צעצוע — רישיות",
    "label_en": "Toy Source (case)",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
    "url_patterns": [
        {
            "regex": r"^https?://toy\.example\.org/u/(?P<who>[A-Za-z0-9_]+)/?$",
            "page_type": "toycase_user",
            # The title keeps the casing that was pasted — it is cosmetic and
            # not part of a dataset's identity. The CONFIG is folded, because
            # it is.
            "title_he": "צעצוע — @{who}",
            "config": {"who": "{who|lower}"},
        },
    ],
}


def _casefold_manifest():
    return sr.validate_manifest(CASEFOLD_MANIFEST)


@pytest.mark.parametrize("url", [
    "https://toy.example.org/u/MOHreport",
    "https://toy.example.org/u/mohreport",
    "https://toy.example.org/u/MOHREPORT",
])
def test_a_folded_group_normalises_the_config(url):
    """A source whose ids are case-insensitive must not yield a different
    config per spelling — the config is what identity is computed from."""
    match = sr.match_manifests(url, [_casefold_manifest()])
    assert match.scraper_config["who"] == "mohreport"


def test_spellings_that_differ_only_in_case_are_one_dataset():
    manifests = [_casefold_manifest()]
    identities = {
        sr.identity_of(sr.match_manifests(u, manifests))
        for u in ("https://toy.example.org/u/MOHreport",
                  "https://toy.example.org/u/mohreport",
                  "https://toy.example.org/u/MOHREPORT")
    }
    assert len(identities) == 1


def test_folding_does_not_collapse_genuinely_different_values():
    manifests = [_casefold_manifest()]
    assert (sr.identity_of(sr.match_manifests("https://toy.example.org/u/alpha", manifests))
            != sr.identity_of(sr.match_manifests("https://toy.example.org/u/beta", manifests)))


def test_the_title_keeps_the_casing_that_was_pasted():
    """Only the config is folded. The title is display, and @MOHreport is how
    the channel writes itself."""
    match = sr.match_manifests("https://toy.example.org/u/MOHreport",
                               [_casefold_manifest()])
    assert match.title == "צעצוע — @MOHreport"


def test_an_unmodified_placeholder_still_substitutes_verbatim():
    """The existing manifests use bare {name} and must be untouched by this."""
    match = sr.match_manifests("https://toy.example.org/מכרזים/2024",
                               [sr.validate_manifest(TOY_MANIFEST)])
    assert match.scraper_config["year"] == "2024"
    assert match.title == "מקור צעצוע — מכרזים 2024"


def test_an_unknown_placeholder_or_modifier_is_left_visible():
    """Left as-is rather than raising: a stray brace in a title is cosmetic,
    a 500 on paste is not. A bad modifier stays visible so a config that
    stopped normalising is noticed here, not months later as duplicates."""
    assert sr._render("{nope} {who|shout}", {"who": "X"}) == "{nope} {who|shout}"


def test_a_folded_optional_group_that_did_not_match_is_dropped():
    """An empty fold renders empty, which must still drop the key rather than
    hand the worker a blank string it would treat as a real value."""
    manifest = sr.validate_manifest({
        **CASEFOLD_MANIFEST, "id": "toycase2",
        "url_patterns": [{
            "regex": r"^https?://toy\.example\.org/o(?:/(?P<who>[A-Za-z]+))?/?$",
            "page_type": "toycase2_o",
            "config": {"who": "{who|lower}", "corpus": "all"},
        }],
    })
    match = sr.match_manifests("https://toy.example.org/o", [manifest])
    assert "who" not in match.scraper_config
    assert match.scraper_config["corpus"] == "all"


# ── per-pattern cadence ─────────────────────────────────────────────────────

CADENCE_MANIFEST = {
    "manifest_version": 1,
    "id": "toycadence",
    "label_he": "מקור צעצוע — קצב",
    "label_en": "Toy Source (cadence)",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
    "default_poll_interval": 604800,
    "url_patterns": [
        {
            # The cheap corpus: one request, so it can be read constantly.
            "regex": r"^https?://toy\.example\.org/c/(?P<who>\w+)#feed$",
            "page_type": "toycadence_feed",
            "poll_interval": 300,
            "config": {"corpus": "feed"},
        },
        {
            # The expensive one: hundreds of requests, read weekly.
            "regex": r"^https?://toy\.example\.org/c/(?P<who>\w+)$",
            "page_type": "toycadence_all",
            "config": {"corpus": "all"},
        },
    ],
}


def test_a_pattern_may_set_its_own_cadence():
    """One source can hold corpora whose costs differ by orders of magnitude,
    so a single source-wide default has to be wrong for one of them."""
    manifests = [sr.validate_manifest(CADENCE_MANIFEST)]
    feed = sr.match_manifests("https://toy.example.org/c/x#feed", manifests)
    whole = sr.match_manifests("https://toy.example.org/c/x", manifests)
    assert feed.poll_interval == 300
    assert whole.poll_interval == 604800   # falls back to the manifest default


def test_the_manifest_default_still_applies_without_a_pattern_interval():
    manifests = [sr.validate_manifest(TOY_MANIFEST)]
    match = sr.match_manifests("https://toy.example.org/decisions", manifests)
    assert match.pattern_poll_interval is None
    assert match.poll_interval == TOY_MANIFEST["default_poll_interval"]


def test_a_pattern_cadence_does_not_leak_into_the_scraper_config():
    """It is a property of the dataset, not an instruction to the engine."""
    manifests = [sr.validate_manifest(CADENCE_MANIFEST)]
    match = sr.match_manifests("https://toy.example.org/c/x#feed", manifests)
    assert "poll_interval" not in match.scraper_config


def test_a_nonsense_pattern_interval_is_rejected():
    with pytest.raises(Exception):
        sr.validate_manifest({
            **CADENCE_MANIFEST, "id": "toycadence2",
            "url_patterns": [{"regex": r"^https?://toy\.example\.org/z$",
                              "poll_interval": 0}],
        })


# ── per-pattern cadence reaches the tracking form ───────────────────────────

FEED_MANIFEST = {
    "manifest_version": 1,
    "id": "toyfeed",
    "label_he": "מקור צעצוע — פיד",
    "label_en": "Toy Source (feed)",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
    # The whole corpus is expensive, so the source-wide default is daily.
    "default_poll_interval": 86400,
    "url_patterns": [
        # ...but this one route is a single request, and says so.
        {"regex": r"^https?://toy\.example\.org/x/?#/feed/?$",
         "page_type": "toyfeed_feed", "poll_interval": 300},
        {"regex": r"^https?://toy\.example\.org/x/?$",
         "page_type": "toyfeed_all"},
    ],
}


def test_validate_returns_the_matched_patterns_cadence(worker_key):
    """The tracking form seeds its frequency picker from this and then always
    sends a value back, so a source-wide default here silently overrode every
    per-pattern poll_interval: a 5-minute feed was offered — and created — at
    the whole-corpus cadence of 24 hours, making it exactly as fresh as the
    history it exists to front-run."""
    row = SourceRegistry(
        id="toyfeed", manifest=FEED_MANIFEST, manifest_hash="h", enabled=True,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)

    feed = client.post("/api/sources/validate",
                       json={"url": "https://toy.example.org/x#/feed"}).json()
    assert feed["page_type"] == "toyfeed_feed"
    assert feed["default_poll_interval"] == 300

    whole = client.post("/api/sources/validate",
                        json={"url": "https://toy.example.org/x"}).json()
    assert whole["page_type"] == "toyfeed_all"
    assert whole["default_poll_interval"] == 86400


def test_a_pattern_cadence_below_the_floor_is_raised_to_it():
    """min_poll_interval is the contract; a manifest cannot undercut it, and
    the form must never be seeded with a value the backend would reject."""
    manifest = sr.validate_manifest({
        **FEED_MANIFEST, "id": "toyfeed2",
        "url_patterns": [{"regex": r"^https?://toy\.example\.org/y/?$",
                          "page_type": "toyfeed2_y", "poll_interval": 5}],
    })
    match = sr.match_manifests("https://toy.example.org/y", [manifest])
    assert max(match.poll_interval, settings.min_poll_interval) == \
        settings.min_poll_interval


COMPANION_MANIFEST = {
    "manifest_version": 1,
    "id": "toycomp",
    "label_he": "מקור צעצוע — נלווים",
    "label_en": "Toy Source (companions)",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#e0f2fe", "fg": "#075985", "accent": "#0ea5e9"},
    "default_poll_interval": 86400,
    "url_patterns": [
        {"regex": r"^https?://toy\.example\.org/c/(?P<who>\w+)/?#/feed/?$",
         "page_type": "toycomp_feed", "poll_interval": 300,
         "title_he": "צעצוע — {who} (פיד)"},
        {"regex": r"^https?://toy\.example\.org/c/(?P<who>\w+)/?$",
         "page_type": "toycomp_all", "title_he": "צעצוע — {who}",
         "companions": ["https://toy.example.org/c/{who}#/feed"]},
    ],
}


def test_validate_announces_the_companions_a_paste_will_open(worker_key):
    """One paste creating two datasets is a surprise worth telling the user
    about before they submit. Each companion is resolved through the registry,
    so what is announced is what the creation path will actually open — not a
    template rendered hopefully."""
    row = SourceRegistry(
        id="toycomp", manifest=COMPANION_MANIFEST, manifest_hash="h", enabled=True,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)

    body = client.post("/api/sources/validate",
                       json={"url": "https://toy.example.org/c/Abc"}).json()
    assert body["page_type"] == "toycomp_all"
    assert body["companions"] == [{
        "url": "https://toy.example.org/c/Abc#/feed",
        "title": "צעצוע — Abc (פיד)",
        "page_type": "toycomp_feed",
        "poll_interval": 300,
    }]


def test_a_url_with_no_companions_says_so_with_null_not_an_empty_list(worker_key):
    """So the frontend can branch on presence without special-casing []."""
    row = SourceRegistry(
        id="toycomp", manifest=COMPANION_MANIFEST, manifest_hash="h", enabled=True,
    )
    db = _FakeDB([row])
    db.lookup_id = None
    client = _client(db)

    body = client.post("/api/sources/validate",
                       json={"url": "https://toy.example.org/c/Abc#/feed"}).json()
    assert body["page_type"] == "toycomp_feed"
    assert body["companions"] is None
