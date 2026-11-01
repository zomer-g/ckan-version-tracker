"""אטלס ישראל (https://www.atlas.gov.il/) expands to its GovMap layers.

Atlas is an app on its own host that embeds GovMap with its own key; 290 of its
~293 layers are not in GovMap's public catalog. The bare site expands to every
layer its menu lists (read live from the app), each stored as the app's own
layer link ``https://www.atlas.gov.il/?lay=<id>``. That URL is what tells the
worker to use Atlas's key (GOVSCRAPER govmap/embed_auth.py); its identity is
still ``("govmap", <id>)``, so a layer that is also public is never tracked
twice. See docs/atlas-source.md.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest  # noqa: E402

from app.api import govmap  # noqa: E402
from app.api.govmap import (  # noqa: E402
    ATLAS, MONTHLY, ValidateRequest, atlas_layers_from, parse_govmap_group_id,
    parse_govmap_site, parse_govmap_url, site_layer_defaults, validate_govmap_url,
)
from app.services.url_identity import url_identity as identity  # noqa: E402

SITE = "https://www.atlas.gov.il/"
MALLS = "https://www.atlas.gov.il/?lay=228527"

# The shape of the app's menu (backend-assets/he/menus/main.json, 2026-11-01):
# headings carry a slug or nothing in ``lay``, layers a GovMap id.
MENU = [
    {"title": "חברה וכלכלה", "uuid": "a", "parent": "", "lay": "economy_and_something"},
    {"title": "מרכזי קניות", "uuid": "b", "parent": "a", "lay": ""},
    {"title": "קניונים", "uuid": "c", "parent": "b", "lay": 228527},
    {"title": "מספר  קניונים ביישוב ", "uuid": "d", "parent": "b", "lay": "228608"},
    {"title": "תוכנית מתאר - עפולה", "uuid": "e", "parent": "", "lay": ""},
    {"title": "מגרשים", "uuid": "f", "parent": "e", "lay": "231379"},
    {"title": "תוכנית מתאר - רהט", "uuid": "g", "parent": "", "lay": ""},
    {"title": "מגרשים", "uuid": "h", "parent": "g", "lay": "231486"},
    # The same layer under two headings is one layer.
    {"title": "נחלים", "uuid": "i", "parent": "b", "lay": "233199"},
    {"title": "נחלים", "uuid": "j", "parent": "e", "lay": "233199"},
    {"title": "שכונות", "uuid": "k", "parent": "", "lay": "neighborhoods_area"},
]
# The layer-info file names one layer the menu does not.
INFO = [
    {"title": "קניונים", "field_layer_id": "228527"},
    {"title": "אתרי סילוק פסולת", "field_layer_id": 227259},
    {"title": "אזורים סטטיסטיים", "field_layer_id": None},
]


@pytest.fixture(autouse=True)
def _menu(monkeypatch):
    async def fake_fetch():
        layers = atlas_layers_from(MENU, INFO)
        govmap._atlas_cache = (0.0, layers)
        return layers

    monkeypatch.setattr(govmap, "fetch_atlas_layers", fake_fetch)
    monkeypatch.setattr(govmap, "_atlas_cache", None)


def _validate(url: str):
    fn = getattr(validate_govmap_url, "__wrapped__", validate_govmap_url)
    return asyncio.run(fn(None, ValidateRequest(url=url)))


def test_the_site_is_a_site_link_and_a_layer_link_is_a_layer_link():
    for url in (SITE, "https://atlas.gov.il", "https://www.atlas.gov.il/?c=219143,618345&z=1&b=0"):
        assert parse_govmap_url(url) is None, url
        assert parse_govmap_site(url) == ATLAS, url
    assert parse_govmap_group_id(SITE) is None
    parsed = parse_govmap_url("https://www.atlas.gov.il/?c=219143,618345&z=1&lay=228527")
    assert parsed.layer_id == "228527" and parsed.site == "atlas"
    assert parse_govmap_site(MALLS) is None
    # Not every *.atlas-ish host.
    assert parse_govmap_site("https://www.notatlas.gov.il/") is None


def test_menu_becomes_one_entry_per_layer_with_its_group():
    layers = atlas_layers_from(MENU, INFO)
    assert [layer["layer_id"] for layer in layers] == [
        "228527", "228608", "231379", "231486", "233199", "227259"]
    captions = {layer["layer_id"]: layer["caption"] for layer in layers}
    assert captions["228527"] == "קניונים — מרכזי קניות"
    assert captions["228608"] == "מספר קניונים ביישוב — מרכזי קניות"
    # Two layers called "מגרשים" stay apart by their plan.
    assert captions["231379"] == "מגרשים — תוכנית מתאר - עפולה"
    assert captions["231486"] == "מגרשים — תוכנית מתאר - רהט"
    assert captions["227259"] == "אתרי סילוק פסולת"
    for layer in layers:
        assert layer["url"] == f"https://www.atlas.gov.il/?lay={layer['layer_id']}"
        assert parse_govmap_url(layer["url"]).site == "atlas"


def test_garbage_menu_is_no_layers():
    assert atlas_layers_from(None, None) == []
    assert atlas_layers_from({"x": 1}, "oops") == []


def test_validate_expands_the_site():
    r = _validate(SITE)
    assert r.valid and r.site_id == "atlas" and r.title == "אטלס ישראל"
    assert len(r.layers) == 6 and r.layers[0].url == MALLS
    assert r.poll_interval == MONTHLY


def test_validate_reports_an_unreadable_menu(monkeypatch):
    async def boom():
        raise OSError("down")

    monkeypatch.setattr(govmap, "fetch_atlas_layers", boom)
    r = _validate(SITE)
    assert not r.valid and r.site_id == "atlas" and "אטלס" in r.error


def test_layer_title_and_cadence():
    # Cold cache: still the Atlas cadence, and a title that names the site.
    assert site_layer_defaults(MALLS) == ("אטלס ישראל — שכבה 228527", MONTHLY)
    asyncio.run(govmap.warm_site_titles([MALLS]))
    assert site_layer_defaults(MALLS) == ("קניונים — מרכזי קניות (אטלס ישראל)", MONTHLY)
    r = _validate(MALLS)
    assert r.valid and r.layer_id == "228527" and r.poll_interval == MONTHLY
    # Ordinary GovMap layers are untouched.
    assert site_layer_defaults("https://www.govmap.gov.il/?lay=11") == (None, None)


def test_identity_is_the_layer_on_either_host():
    assert identity(MALLS) == ("govmap", "228527")
    assert identity("https://www.atlas.gov.il/?c=1,2&lay=228527&z=3") == ("govmap", "228527")
    assert identity("https://www.govmap.gov.il/?lay=228527") == ("govmap", "228527")
