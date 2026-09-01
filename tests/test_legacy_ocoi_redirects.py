"""ocoi.org.il's old paths must keep resolving after the service is shut down.

OCOI's whole credibility model was that every claim links back to the document
it came from — its MCP server explicitly instructs models to cite
``https://www.ocoi.org.il/document?id=…`` and ``/entity?type=&id=``. Those URLs
are already sitting in text nobody can go back and edit, so pointing the domain
at OVER without these routes would turn every citation the project ever emitted
into a 404 at the moment of cutover.

The SPA fallback answers 200 for unknown paths, so "it does not 404" proves
nothing here. Each test asserts the redirect and its target.
"""
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)
OCOI_HOST = {"host": "www.ocoi.org.il"}


def _location(path: str, headers: dict | None = None) -> tuple[int, str]:
    r = client.get(path, follow_redirects=False, headers=headers or {})
    return r.status_code, r.headers.get("location", "")


def test_entity_deep_link_lands_on_the_graph_focused_on_that_entity():
    code, loc = _location("/entity?type=person&id=abc-123")
    assert code == 301
    assert loc == "/projects/ocoi?tab=graph&type=person&id=abc-123"


def test_document_deep_link_lands_on_that_document():
    """Not merely on the documents tab: a citation that resolves to an
    unfiltered list has not resolved."""
    code, loc = _location("/document?id=doc-9")
    assert code == 301
    assert loc == "/projects/ocoi?tab=documents&doc=doc-9"


def test_bare_legacy_paths_still_reach_the_project():
    for path, target in (("/entity", "/projects/ocoi"),
                         ("/document", "/projects/ocoi?tab=documents")):
        code, loc = _location(path)
        assert code == 301 and loc == target, path


def test_ids_are_escaped_into_the_query():
    code, loc = _location("/entity?type=company&id=a%20b%26c=1")
    assert code == 301
    assert "a%20b%26c%3D1" in loc
    # The injected value must not start a new parameter.
    assert loc.count("&") == 2


def test_search_redirects_only_for_the_legacy_host():
    """/search is a name OVER may want for itself; the redirect is scoped to
    the retiring domain rather than claiming it site-wide."""
    code, loc = _location("/search?q=%D7%9E%D7%A9%D7%94", headers=OCOI_HOST)
    assert code == 301
    assert loc == "/projects/ocoi?q=%D7%9E%D7%A9%D7%94"

    # Not "404": OVER is an SPA and its fallback answers 200 for any unknown
    # path. What matters is that it does not REDIRECT.
    code, loc = _location("/search?q=x", headers={"host": "www.over.org.il"})
    assert code != 301 and not loc


def test_apex_legacy_host_is_honoured_too():
    code, _ = _location("/search?q=x", headers={"host": "ocoi.org.il"})
    assert code == 301


def test_a_lookalike_host_does_not_get_the_redirect():
    """endswith() on a bare domain would also match notocoi.org.il."""
    code, loc = _location("/search?q=x", headers={"host": "myocoi.org.il"})
    assert code != 301 and not loc
