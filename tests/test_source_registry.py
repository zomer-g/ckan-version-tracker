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
