"""A GovMap "sites" sub-site link expands to the sub-site's layers.

The energy ministry's solar-potential map,
https://www.govmap.gov.il/sites/energy/index.html?map=solar_panels, draws two
layers that are NOT in GovMap's public catalog (225678 roofs, 225680 public
areas); they are served only with the sub-site's embed key, which the worker
reads from the sub-site page (GOVSCRAPER govmap/embed_auth.py). So OVER stores
each layer as the page plus ``&lay=<id>``: the URL keeps naming the sub-site,
the identity stays ``("govmap", <id>)``, and the layer is polled monthly.
See docs/govmap-solar-source.md.
"""
import asyncio
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api.govmap import (  # noqa: E402
    GOVMAP_SITES, MONTHLY, ValidateRequest, parse_govmap_group_id, parse_govmap_site,
    parse_govmap_url, site_layer_defaults, site_layers, validate_govmap_url,
)
from app.services.url_identity import url_identity as identity  # noqa: E402

SITE_URL = "https://www.govmap.gov.il/sites/energy/index.html?map=solar_panels"
ROOFS_URL = SITE_URL + "&lay=225678"


def _validate(url: str):
    # The rate limiter wraps the endpoint; call the undecorated function.
    fn = getattr(validate_govmap_url, "__wrapped__", validate_govmap_url)
    return asyncio.run(fn(None, ValidateRequest(url=url)))


def test_the_site_page_is_a_site_link_not_a_layer_link():
    assert parse_govmap_url(SITE_URL) is None
    assert parse_govmap_site(SITE_URL) == ("energy", "solar_panels")
    assert parse_govmap_group_id(SITE_URL) is None


def test_a_site_layer_link_is_a_layer_link():
    parsed = parse_govmap_url(ROOFS_URL)
    assert parsed.layer_id == "225678" and parsed.site == "energy"
    assert parse_govmap_site(ROOFS_URL) is None
    # Ordinary layer links are untouched.
    plain = parse_govmap_url("https://www.govmap.gov.il/?c=1,2&lay=11")
    assert plain.layer_id == "11" and plain.site is None


def test_site_expands_to_both_layers_with_storable_urls():
    title, layers = site_layers(("energy", "solar_panels"))
    assert title == "פוטנציאל ייצור אנרגיה סולארית בשטחים בנויים"
    assert [layer["layer_id"] for layer in layers] == ["225678", "225680"]
    assert layers[0]["url"] == ROOFS_URL
    assert layers[1]["caption"] == "פוטנציאל הצללה סולארית"
    # Every stored URL parses back to its layer, on the sub-site.
    for layer in layers:
        parsed = parse_govmap_url(layer["url"])
        assert parsed.layer_id == layer["layer_id"] and parsed.site == "energy"


def test_site_layer_identity_is_the_layer_id():
    # So a later "?lay=225678" link, or the same layer copied with a viewport,
    # is recognised as the dataset already tracked, not requested twice.
    assert identity(ROOFS_URL) == ("govmap", "225678")
    assert identity("https://www.govmap.gov.il/?c=1,2&lay=225678") == ("govmap", "225678")


def test_site_layers_are_monthly_with_their_own_title():
    title, interval = site_layer_defaults(ROOFS_URL)
    assert interval == MONTHLY == 30 * 24 * 3600
    assert title == "מבנים בשטחים בנויים (פוטנציאל ייצור אנרגיה סולארית בשטחים בנויים)"
    # A layer of the site that is not listed still gets the site's cadence.
    assert site_layer_defaults(SITE_URL + "&lay=999")[1] == MONTHLY
    # Nothing changes for an ordinary layer.
    assert site_layer_defaults("https://www.govmap.gov.il/?lay=11") == (None, None)


def test_validate_expands_the_site_page():
    r = _validate(SITE_URL)
    assert r.valid and r.site_id == "energy/solar_panels"
    assert r.group_id is None and r.layer_id is None
    assert [layer.layer_id for layer in r.layers] == ["225678", "225680"]
    assert r.poll_interval == MONTHLY


def test_validate_a_site_layer_link():
    r = _validate(ROOFS_URL)
    assert r.valid and r.layer_id == "225678"
    assert r.poll_interval == MONTHLY
    assert r.title.startswith("מבנים בשטחים בנויים")


def test_unknown_sub_site_is_refused_with_the_supported_ones():
    r = _validate("https://www.govmap.gov.il/sites/water/index.html?map=x")
    assert not r.valid and r.site_id == "water/x"
    assert SITE_URL in r.error
    # The energy site's other map (the dashboard) carries no layers.
    assert not _validate("https://www.govmap.gov.il/sites/energy/index.html").valid


def test_registry_shape():
    for (site, map_id), entry in GOVMAP_SITES.items():
        assert entry["url"].startswith(f"https://www.govmap.gov.il/sites/{site}/")
        assert f"map={map_id}" in entry["url"]
        assert all(lid.isdigit() for lid, _ in entry["layers"])
