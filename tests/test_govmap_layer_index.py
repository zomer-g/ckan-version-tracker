"""over_govmap_layers: rows match GovMap's "about" panel; the change log."""
from datetime import datetime, timezone

from app.services.govmap_layer_index import diff, layer_rows, parse_source_date

CATALOG = {
    "groups": {
        "categories": {"13": {"name": "תיירות, טבע, פנאי וספורט"}},
        "offices": {"29": {"name": "החברה להגנת הטבע"}, "6": {"name": "משטרת ישראל"}},
        "settlements": {"6": {"name": "עיריית באר שבע"}},
    },
    "sources": {"0": {"id": 0, "name": "ממשלה ומוסדות ציבור"},
                "1": {"id": 1, "name": "רשויות מקומיות"}},
    "userGroups": [{"groupName": "יעודי קרקע באר שבע",
                    "nodes": [{"nodeId": 211862, "nodeType": "layer"}]}],
    "catalog": [
        {"id": "211862", "caption": "יעודי קרקע", "publicPublishType": 1,
         "publisherId": 6, "categoryId": 99, "updateDate": "2026-01-05 10:00:00.5",
         "keywords": [], "serviceLayerId": "govmap:layer_211862", "featureVersion": 71},
        {"id": "414", "name": "haganathatevapoly", "description": "אין מידע",
         "publisherId": 29, "publicPublishType": 0,
         "keywords": ["החברה להגנת הטבע", "סקרי טבע ונוף"],
         "caption": "אינדקס סקרי טבע ונוף", "layerKind": 2, "categoryId": 13,
         "serviceLayerId": "govmap:layer_haganathatevapoly",
         "updateDate": "2024-07-30 07:47:33", "featureVersion": 2,
         "needCredentials": False},
    ],
}

NOW = datetime(2026, 10, 14, tzinfo=timezone.utc)


def test_row_is_the_about_panel():
    rows = layer_rows(CATALOG)
    assert [r["layer_id"] for r in rows] == ["414", "211862"]
    r = rows[0]
    assert r["caption"] == "אינדקס סקרי טבע ונוף"
    assert r["description"] == "אין מידע"
    assert r["publisher"] == "החברה להגנת הטבע"
    assert r["publisher_type"] == "ממשלה ומוסדות ציבור"
    assert r["category"] == "תיירות, טבע, פנאי וספורט"
    assert r["keywords"] == "החברה להגנת הטבע, סקרי טבע ונוף"
    assert r["layer_url"] == "https://www.govmap.gov.il/?lay=414"
    # Shown on the site as 30.07.2024 10:47 (IDT) — the catalog is UTC.
    assert r["source_updated_at"] == datetime(2024, 7, 30, 7, 47, 33, tzinfo=timezone.utc)


def test_publisher_id_space_depends_on_publish_type():
    r = layer_rows(CATALOG)[1]
    assert r["publisher"] == "עיריית באר שבע"      # id 6 in settlements, not offices
    assert r["publisher_type"] == "רשויות מקומיות"
    assert r["category"] is None
    assert r["layer_groups"] == "יעודי קרקע באר שבע"
    assert r["data_version"] == 71


def test_bad_date_is_none():
    assert parse_source_date("not a date") is None
    assert parse_source_date(None) is None


def _stored(rows, **over):
    return {r["layer_id"]: {**r, "in_catalog": True, **over} for r in rows}


def test_first_build_is_a_baseline_not_additions():
    assert diff({}, layer_rows(CATALOG), NOW, allow_removals=True) == []


def test_unchanged_read_logs_nothing():
    rows = layer_rows(CATALOG)
    assert diff(_stored(rows), rows, NOW, allow_removals=True) == []


def test_added_removed_renamed_republished():
    rows = layer_rows(CATALOG)
    old = _stored(rows)
    old["999"] = {**rows[0], "layer_id": "999", "caption": "שכבה שנעלמה", "in_catalog": True}
    del old["211862"]
    old["414"] = {**old["414"], "caption": "שם ישן", "data_version": 1}
    kinds = {(c[1], c[3]) for c in diff(old, rows, NOW, allow_removals=True)}
    assert ("211862", "added") in kinds
    assert ("999", "removed") in kinds
    assert ("414", "renamed") in kinds
    assert ("414", "republished") in kinds


def test_removals_held_back_on_a_short_read():
    rows = layer_rows(CATALOG)
    old = _stored(rows)
    old["999"] = {**rows[0], "layer_id": "999", "in_catalog": True}
    assert not [c for c in diff(old, rows, NOW, allow_removals=False) if c[3] == "removed"]


def test_layer_back_in_catalog_is_returned():
    rows = layer_rows(CATALOG)
    old = _stored(rows)
    old["414"]["in_catalog"] = False
    assert ("414", "returned") in {(c[1], c[3]) for c in diff(old, rows, NOW, allow_removals=True)}


def test_metadata_change_names_the_field():
    rows = layer_rows(CATALOG)
    old = _stored(rows)
    old["414"]["description"] = "ישן"
    [c] = diff(old, rows, NOW, allow_removals=True)
    assert c[3:] == ("metadata", "description", "ישן", "אין מידע")
