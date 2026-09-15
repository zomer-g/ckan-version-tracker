"""A GovMap layer-GROUP link (``?g=397``) expands to its member layers.

The bug: ``https://www.govmap.gov.il/?c=198360.87,677529.5&z=4&g=397`` names
no ``lay=``, so validation rejected it and the resolver's path match listed 25
unrelated tracked layers as "already tracked". Group 397 is "קלפיות בחירות 26",
two layers (235035, 235036), taken from the live catalog on 2026-09-15.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api.govmap import group_layers, parse_govmap_group_id, parse_govmap_url  # noqa: E402

GROUP_URL = "https://www.govmap.gov.il/?c=198360.87,677529.5&z=4&g=397"

CATALOG = {
    "catalog": [
        {"id": "235035", "caption": "ריכוזי קלפיות טופס א' כנסת 26",
         "serviceLayerId": "govmap:layer_235035"},
        {"id": "235036", "caption": "קלפיות טופס א' כנסת 26",
         "serviceLayerId": "govmap:layer_235036"},
    ],
    "userGroups": [
        {"id": 397, "groupName": "קלפיות בחירות 26", "nodes": [
            # Out of display order on purpose: the group's order wins.
            {"nodeId": 235036, "nodeType": "layer", "displayOrder": 1},
            {"nodeId": 235035, "nodeType": "layer", "displayOrder": 0},
        ]},
        {"id": 13, "groupName": "יעודי קרקע באר שבע", "nodes": [
            {"nodeId": 211862, "nodeType": "layer", "displayOrder": 0,
             "layerIcon": {"layerName": "התראות קוויות"}},
        ]},
    ],
}


def test_group_link_is_not_a_layer_link():
    assert parse_govmap_url(GROUP_URL) is None
    assert parse_govmap_group_id(GROUP_URL) == "397"


def test_a_layer_link_is_never_read_as_a_group():
    assert parse_govmap_group_id("https://www.govmap.gov.il/?g=397&lay=11") is None
    assert parse_govmap_group_id("https://example.com/?g=397") is None


def test_group_expands_to_its_layers_in_display_order():
    name, layers = group_layers(CATALOG, "397")
    assert name == "קלפיות בחירות 26"
    assert [layer["layer_id"] for layer in layers] == ["235035", "235036"]
    assert layers[0]["caption"] == "ריכוזי קלפיות טופס א' כנסת 26"
    assert layers[0]["url"] == "https://www.govmap.gov.il/?lay=235035"
    # Each member URL is a plain layer link the request form accepts.
    assert parse_govmap_url(layers[1]["url"]).layer_id == "235036"


def test_member_missing_from_catalog_keeps_its_id():
    _, layers = group_layers(CATALOG, "13")
    assert layers == [{"layer_id": "211862", "caption": "התראות קוויות",
                       "url": "https://www.govmap.gov.il/?lay=211862"}]


def test_unknown_group_is_none():
    assert group_layers(CATALOG, "999") is None
    assert group_layers({}, "397") is None
