"""The daily catalog watch (app/services/catalog_watch.py) and the republish
rule it added to the GovMap catalog refresh (govmap_coverage)."""
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.services.catalog_watch import plan_org  # noqa: E402
from app.services.govmap_coverage import (  # noqa: E402
    _extract_layers,
    changed_since_capture,
    parse_update_date,
)


def _row(ckan_name, status="active", resource_ids=None, resource_id=None, ckan_id=None):
    return SimpleNamespace(
        ckan_name=ckan_name, ckan_id=ckan_id or ckan_name, status=status,
        resource_ids=resource_ids, resource_id=resource_id,
    )


def _pkg(name, modified, *rids, pid=None):
    return {"id": pid or f"uuid-{name}", "name": name, "metadata_modified": modified,
            "resources": [{"id": r} for r in rids]}


# --- data.gov.il --------------------------------------------------------------

def test_untracked_recent_package_is_onboarded_old_one_is_not():
    pkgs = [_pkg("e-new-data-gov-il", "2026-08-16T10:00:00", "r1"),
            _pkg("bqt", "2023-03-21T10:00:00", "r2")]
    plan = plan_org(pkgs, [], "2026-01-01")
    assert [p["name"] for p in plan["onboard"]] == ["e-new-data-gov-il"]
    assert plan["skipped"] == [("bqt", "untouched since 2026-01-01")]


def test_tracked_by_uuid_alone_counts_as_tracked():
    pkg = _pkg("tatag", "2026-06-21T00:00:00", "r1", pid="abc")
    row = _row("some-other-slug", resource_ids=["r1"], ckan_id="abc")
    assert plan_org([pkg], [row], "2026-01-01") == {"onboard": [], "extend": [], "skipped": []}


def test_new_resource_is_added_to_the_whole_package_row():
    # data.gov.il "shape" today: one tracked resource, three more at the source.
    pkg = _pkg("shape", "2026-08-16T00:00:00", "parcels", "cancel", "last", "struct")
    row = _row("shape", resource_ids=["parcels"])
    plan = plan_org([pkg], [row], "2026-01-01")
    assert plan["onboard"] == []
    assert plan["extend"] == [(row, ["cancel", "last", "struct"])]


def test_rejected_package_stays_rejected():
    pkg = _pkg("x", "2026-09-01T00:00:00", "r1", "r2")
    plan = plan_org([pkg], [_row("x", status="rejected", resource_ids=["r1"])], "2026-01-01")
    assert plan == {"onboard": [], "extend": [], "skipped": []}


def test_split_mode_package_is_reported_not_guessed():
    pkg = _pkg("x", "2026-09-01T00:00:00", "r1", "r2")
    row = _row("x", resource_id="r1", resource_ids=["r1"])
    plan = plan_org([pkg], [row], "2026-01-01")
    assert plan["extend"] == []
    assert plan["skipped"] == [("x", "1 new resource(s) on a split-mode package")]


def test_legacy_track_all_row_has_nothing_missing():
    pkg = _pkg("modedim", "2026-09-14T00:00:00", "r1", "r2")
    assert plan_org([pkg], [_row("modedim")], "2026-01-01") == {
        "onboard": [], "extend": [], "skipped": []}


def test_resources_spread_over_two_active_rows_are_not_missing():
    pkg = _pkg("x", "2026-09-01T00:00:00", "r1", "r2")
    rows = [_row("x", resource_ids=["r1"]), _row("x", resource_ids=["r2"])]
    assert plan_org([pkg], rows, "2026-01-01")["extend"] == []


# --- GovMap -------------------------------------------------------------------

def test_update_date_is_read_as_israel_time():
    dt = parse_update_date("2026-09-23 00:04:31.207584")
    assert dt == datetime(2026, 9, 22, 21, 4, 31, 207584, tzinfo=timezone.utc)
    assert parse_update_date("") is None
    assert parse_update_date("not a date") is None


def test_republished_layer_is_due_only_past_the_floor():
    now = datetime(2026, 9, 23, 6, 0, tzinfo=timezone.utc)
    republished = datetime(2026, 9, 22, 21, 4, tzinfo=timezone.utc)
    # Layer 15: last scraped 2026-09-02, republished last night → due.
    assert changed_since_capture(republished, now - timedelta(days=21), now, 7)
    # Scraped 3 days ago, republished since → waits for the floor.
    assert not changed_since_capture(republished, now - timedelta(days=3), now, 7)
    # Not republished since the scrape → not due.
    assert not changed_since_capture(now - timedelta(days=30), now - timedelta(days=21), now, 7)
    # Never scraped / no date → the ordinary rules decide, not this one.
    assert not changed_since_capture(republished, None, now, 7)
    assert not changed_since_capture(None, now - timedelta(days=21), now, 7)


def test_catalog_extraction_keeps_the_update_date():
    catalog = {"groups": {"offices": {}, "settlements": {}},
               "catalog": [{"id": "240040", "caption": "גבולות תהליכי קדסטר", "layerKind": 2,
                            "complexity": 1, "updateDate": "2026-09-23 00:05:06.951315",
                            "publisherId": 2, "publicPublishType": 0}]}
    [lay] = _extract_layers(catalog)
    assert lay["update_date"] == "2026-09-23 00:05:06.951315"


# --- data.gov.il's 403 face of the wall ---------------------------------------

def test_403_on_a_file_is_blocked_not_an_error():
    import asyncio

    import httpx

    from app.services import version_detector as vd

    async def fake_download(url, resource_id="", max_bytes=None):
        req = httpx.Request("GET", "https://aws-e.data.gov.il/x.zip")
        raise httpx.HTTPStatusError(
            "Client error '403 Forbidden' for url 'https://aws-e.data.gov.il/x.zip'",
            request=req, response=httpx.Response(403, request=req))

    real = vd.ckan_client.download_resource
    vd.ckan_client.download_resource = fake_download
    try:
        changed, hashes, errors, blocked = asyncio.run(vd.detect_resource_changes(
            {"_hashes": {"zip": "old"}},
            [{"id": "zip", "url": "https://data.gov.il/x.zip", "format": "ZIP"}]))
    finally:
        vd.ckan_client.download_resource = real
    assert blocked == {"zip"}
    assert errors == []
    assert hashes == {"zip": "old"}  # a skip is not a change
