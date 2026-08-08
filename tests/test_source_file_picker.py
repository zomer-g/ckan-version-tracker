"""The file picker: previewing a page's files and storing what was ticked.

A source whose pages hold many files (cbs.gov.il publications: 21, 45, 110)
cannot be tracked with one yes/no. The manifest asks for a picker, OVER reads
the page live so the choice can be made before the dataset exists, and the
ticks land in ``scraper_config["files"]``.

Two halves have to agree for a picker to appear — the manifest declaring
``file_picker`` AND this build knowing how to read that source's pages — and
the invariants below are about what happens when they don't, because the
failure modes there are silent ones: a form that offers files nobody can
import, or a config key no engine reads.
"""
import asyncio
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app.api import sources as sources_api
from app.api.datasets import _apply_file_selection, MAX_SELECTED_FILES
from app.database import get_db
from app.models.source_registry import SourceRegistry
from app.rate_limit import limiter
from app.services import source_registry as sr

PICKER_MANIFEST = {
    "manifest_version": 1,
    "id": "toypicker",
    "label_he": "מקור צעצוע",
    "label_en": "Toy Source",
    "site_url": "https://toy.example.org/",
    "badge": {"bg": "#fae8ff", "fg": "#86198f", "accent": "#c026d3"},
    "neon_eligible": True,
    "file_picker": True,
    "default_config": {"download_files": True},
    "url_patterns": [{"regex": r"^https?://toy\.example\.org/page/\d+$",
                      "page_type": "toypicker_page"}],
}

PAGE = "https://toy.example.org/page/7"


@pytest.fixture(autouse=True)
def clean_cache():
    sr.invalidate_cache()
    yield
    sr.invalidate_cache()


def _manifest(**overrides):
    return {**PICKER_MANIFEST, **overrides}


class _FakeDB:
    def __init__(self, rows):
        self.rows = rows

    async def execute(self, stmt):
        rows = self.rows

        class _Result:
            def scalars(self):
                class _S:
                    def all(self_inner):
                        return [r for r in rows if r.enabled]
                return _S()
        return _Result()


def _client(manifest: dict) -> TestClient:
    row = SourceRegistry(
        id=manifest["id"], manifest=manifest, enabled=True,
        manifest_hash=sr.manifest_hash(manifest),
    )
    app = FastAPI()
    app.state.limiter = limiter
    app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
    app.include_router(sources_api.router)

    async def _fake_db():
        yield _FakeDB([row])

    app.dependency_overrides[get_db] = _fake_db
    limiter.reset()
    return TestClient(app, raise_server_exceptions=False)


# --- the manifest flag ------------------------------------------------------


def test_file_picker_is_off_unless_the_manifest_asks():
    """Every source that shipped before this defaults to no picker, and keeps
    tracking whatever its engine decides."""
    plain = {k: v for k, v in PICKER_MANIFEST.items() if k != "file_picker"}
    assert sr.validate_manifest(plain).file_picker is False
    assert sr.validate_manifest(_manifest(file_picker=True)).file_picker is True


def test_validate_reports_a_picker_only_when_a_previewer_exists(monkeypatch):
    """The manifest asking is not enough — reading a page's file table is
    site-specific work. A form told 'picker: true' with nothing behind it would
    show a spinner that never resolves."""
    client = _client(PICKER_MANIFEST)
    assert client.post("/api/sources/validate", json={"url": PAGE}).json()["file_picker"] is False

    monkeypatch.setitem(sources_api.PREVIEWERS, "toypicker", lambda: None)
    assert client.post("/api/sources/validate", json={"url": PAGE}).json()["file_picker"] is True


def test_a_source_with_no_picker_reports_none():
    plain = {k: v for k, v in PICKER_MANIFEST.items() if k != "file_picker"}
    client = _client(plain)
    body = client.post("/api/sources/validate", json={"url": PAGE}).json()
    assert body["valid"] is True and body["file_picker"] is False


# --- the preview endpoint ---------------------------------------------------


def test_preview_returns_the_pages_files(monkeypatch):
    async def fake(url):
        return {"title": "עמוד פרסום", "url": url, "files": [
            {"path": "/a/x.xlsx", "name": "x.xlsx", "title": "לוח 1", "chapter": "לוחות",
             "subject": "", "order": 1.0, "ext": "xlsx", "size": 100, "modified": "",
             "url": "https://toy.example.org/a/x.xlsx", "on_page": True, "tabular": True},
        ]}

    monkeypatch.setitem(sources_api.PREVIEWERS, "toypicker", lambda: fake)
    body = _client(PICKER_MANIFEST).post("/api/sources/preview", json={"url": PAGE}).json()
    assert body["source_id"] == "toypicker"
    assert body["title"] == "עמוד פרסום"
    assert [f["path"] for f in body["files"]] == ["/a/x.xlsx"]


def test_preview_marks_files_over_already_has(monkeypatch):
    """Coming back to a page of 27 files of which 23 are already in the approval
    queue gave no way to see which four were left: the picker offered all 27 and
    the submit answered "all duplicates"."""
    async def fake(url):
        return {"title": "עמוד", "url": url, "files": [
            {"path": "/a/x.xlsx", "url": "https://toy.example.org/a/x.xlsx",
             "name": "x.xlsx", "title": "לוח 1", "chapter": "", "subject": "",
             "order": None, "ext": "xlsx", "size": 1, "modified": "",
             "on_page": True, "tabular": True},
            {"path": "/a/y.xlsx", "url": "https://toy.example.org/a/y.xlsx",
             "name": "y.xlsx", "title": "לוח 2", "chapter": "", "subject": "",
             "order": None, "ext": "xlsx", "size": 1, "modified": "",
             "on_page": True, "tabular": True},
        ]}

    async def fake_mark(db, files):
        known = {"https://toy.example.org/a/x.xlsx":
                 {"dataset_id": "abc", "status": "pending"}}
        return [{**f, "tracked": f["url"] in known,
                 **({"tracked_dataset": known[f["url"]]} if f["url"] in known else {})}
                for f in files]

    monkeypatch.setitem(sources_api.PREVIEWERS, "toypicker", lambda: fake)
    monkeypatch.setattr(sources_api, "_mark_tracked", fake_mark)
    body = _client(PICKER_MANIFEST).post("/api/sources/preview", json={"url": PAGE}).json()
    by_path = {f["path"]: f for f in body["files"]}
    assert by_path["/a/x.xlsx"]["tracked"] is True
    assert by_path["/a/x.xlsx"]["tracked_dataset"]["status"] == "pending"
    assert by_path["/a/y.xlsx"]["tracked"] is False


def test_marking_tracked_never_costs_the_file_list(monkeypatch):
    """The list is the point of the endpoint; the annotation is a nicety."""
    async def fake(url):
        return {"title": "עמוד", "url": url, "files": [
            {"path": "/a/x.xlsx", "url": "https://toy.example.org/a/x.xlsx",
             "name": "x.xlsx", "title": "לוח", "chapter": "", "subject": "",
             "order": None, "ext": "xlsx", "size": 1, "modified": "",
             "on_page": True, "tabular": True},
        ]}

    class _Boom:
        async def execute(self, stmt):
            raise RuntimeError("db down")

    monkeypatch.setitem(sources_api.PREVIEWERS, "toypicker", lambda: fake)
    files = asyncio.run(sources_api._mark_tracked(_Boom(), _run_files(fake)))
    assert len(files) == 1 and files[0]["path"] == "/a/x.xlsx"


def _run_files(fake):
    return asyncio.run(fake("u"))["files"]


def test_preview_refuses_a_url_no_source_claims():
    resp = _client(PICKER_MANIFEST).post(
        "/api/sources/preview", json={"url": "https://elsewhere.example.org/x"},
    )
    assert resp.status_code == 400


def test_preview_refuses_a_source_with_no_previewer():
    """Rather than 500ing on a KeyError, which is what a missing registration
    would otherwise look like from the form."""
    resp = _client(PICKER_MANIFEST).post("/api/sources/preview", json={"url": PAGE})
    assert resp.status_code == 400
    assert "does not publish a file list" in resp.json()["detail"]


def test_a_failing_preview_reports_the_sites_own_message(monkeypatch):
    """The person pasted a URL and is waiting; 'something went wrong' hides the
    one fact that would let them fix it."""
    async def fake(url):
        raise RuntimeError("לא נמצא עמוד פרסום בכתובת הזו")

    monkeypatch.setitem(sources_api.PREVIEWERS, "toypicker", lambda: fake)
    resp = _client(PICKER_MANIFEST).post("/api/sources/preview", json={"url": PAGE})
    assert resp.status_code == 502
    assert "לא נמצא עמוד פרסום" in resp.json()["detail"]


# --- identity: one dataset per page, not per source --------------------------

PAGED = _manifest(
    id="toypaged",
    url_patterns=[{
        "regex": r"^https?://toy\.example\.org/(?P<section>[^/]+)/(?P<slug>[^/?#]+)\.aspx$",
        "page_type": "toypaged_page",
        "config": {"page": "{section}/{slug|unquote}"},
    }],
)


def _identity(url: str):
    return sr.identity_of(sr.match_manifests(url, [sr.validate_manifest(PAGED)]))


def test_a_page_in_the_config_is_what_makes_it_its_own_dataset():
    """identity_of is (source, page_type, config). A source whose every URL
    yields the same config makes every page of the site ONE dataset — the
    second page pasted resolves to the first one's versions page. cbs_pub
    shipped that way and every CBS publication collapsed into one."""
    a = _identity("https://toy.example.org/a/one.aspx")
    b = _identity("https://toy.example.org/a/two.aspx")
    c = _identity("https://toy.example.org/b/one.aspx")
    assert len({a, b, c}) == 3


def test_the_unquote_modifier_folds_the_two_spellings_of_a_hebrew_page():
    """match_manifests tries the URL raw AND decoded and takes the first that
    matches, so a Hebrew slug is captured encoded from a browser copy-paste and
    decoded from the same link typed out. Without the fold that is two
    identities, and the second paste opens a duplicate dataset."""
    encoded = "https://toy.example.org/a/%D7%94%D7%A8%D7%A9%D7%95%D7%99%D7%95%D7%AA.aspx"
    decoded = "https://toy.example.org/a/הרשויות.aspx"
    assert _identity(encoded) == _identity(decoded)
    assert "%D7" not in _identity(encoded)[2]


def test_an_unknown_modifier_stays_visible_rather_than_silently_not_folding():
    """A config that quietly stopped normalising would surface only as
    duplicate datasets, much later."""
    from app.services.source_registry import _render
    assert _render("{slug|bogus}", {"slug": "x"}) == "{slug|bogus}"


# --- storing the selection --------------------------------------------------


def _match(manifest: dict):
    return sr.match_manifests(PAGE, [sr.validate_manifest(manifest)])


def test_the_selection_is_stored_as_scraper_config_files():
    sc = _apply_file_selection(_match(PICKER_MANIFEST), {"kind": "toypicker"},
                               ["/a/x.xlsx", "/a/y.xlsx"])
    assert sc["files"] == ["/a/x.xlsx", "/a/y.xlsx"]


def test_an_empty_selection_leaves_the_engines_own_default():
    """'Nothing ticked' is not 'import nothing' — it is 'whatever the page
    itself lists', which is what each engine does with no files key."""
    for empty in (None, [], ["", "   "]):
        assert "files" not in _apply_file_selection(
            _match(PICKER_MANIFEST), {"kind": "toypicker"}, empty)


def test_a_source_without_a_picker_does_not_get_a_files_key():
    """A config key no engine reads is a promise the dataset does not keep."""
    plain = {k: v for k, v in PICKER_MANIFEST.items() if k != "file_picker"}
    sc = _apply_file_selection(_match(plain), {"kind": "toypicker"}, ["/a/x.xlsx"])
    assert "files" not in sc


def test_the_selection_is_deduped_and_bounded():
    """This arrives on an anonymous endpoint, so it is bounded like any other
    public input rather than trusted to be a real page's file list."""
    match = _match(PICKER_MANIFEST)
    sc = _apply_file_selection(match, {}, ["/a/x.xlsx", "/a/x.xlsx", " /a/y.xlsx "])
    assert sc["files"] == ["/a/x.xlsx", "/a/y.xlsx"]

    flood = [f"/a/{i}.xlsx" for i in range(MAX_SELECTED_FILES + 50)]
    assert len(_apply_file_selection(match, {}, flood)["files"]) == MAX_SELECTED_FILES

    mixed = _apply_file_selection(match, {}, ["/a/x.xlsx", 17, None, {"path": "/a/z"}])
    assert mixed["files"] == ["/a/x.xlsx"]
