"""A refusal that says where the data actually is.

/api/append serves the datasets OVER archives into `public.append_*`. Every
GovMap mapping layer is archived somewhere else — the `idx` mirror — so all ~812
of them answered this API with a bare 409 "Dataset is not an append archive",
while their rows sat behind a GiST index, queryable by SQL and by bounding box.

That sentence is true and it is read as "download-only". It was read that way:
an outside developer walked the documented API, hit it, and concluded OVER kept
its planning layers as opaque files with no spatial access — then started
writing a client-side ITM reprojection to work around a limitation that does
not exist.

Pinned here: when the rows ARE reachable elsewhere, the refusal carries the
address. And the status stays 409 — a 200 whose body has no `rows` key would
break the clients that check the status before reading, which is the correct
thing for a client to do.
"""
import asyncio
import os
import sys
import types
import uuid

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.api import append as api  # noqa: E402
from app.services import index_mirror  # noqa: E402

_DS = types.SimpleNamespace(
    id=uuid.UUID("c2bd90e1-fb36-437a-878a-d01f1f4aea62"),
    title='גבולות ישובים בשטחי איו"ש',
)
_MIRROR = {"table": "govmap_200541_b19acb42_c2bd90e1", "schema": "idx",
           "rows": 138, "version_number": 2, "has_geom": True}


def _detail(monkeypatch, mirror):
    async def fake(_dataset_id):
        return mirror

    monkeypatch.setattr(index_mirror, "mirrored_table", fake)
    return asyncio.run(api._not_here_detail(_DS))


def test_a_dataset_nowhere_else_gets_the_plain_message(monkeypatch):
    assert _detail(monkeypatch, None) == "Dataset is not an append archive"


def test_a_mirrored_dataset_gets_the_table_and_the_route(monkeypatch):
    d = _detail(monkeypatch, _MIRROR)
    assert d["table"] == "idx.govmap_200541_b19acb42_c2bd90e1"
    assert d["queryable_via"] == "/api/tables/sql"
    assert d["rows"] == 138
    # A copy-pasteable first query beats a description of one.
    assert d["example_sql"].startswith("SELECT * FROM idx.")


def test_a_layer_with_geometry_advertises_the_bbox_route(monkeypatch):
    d = _detail(monkeypatch, _MIRROR)
    assert d["features_url"] == "/api/tables/govmap_200541_b19acb42_c2bd90e1/features"
    # The CRS is the fact that stops a caller reprojecting by hand.
    assert "4326" in d["geometry"] and "bbox=" in d["geometry"]


def test_a_mirrored_table_without_geometry_promises_no_bbox(monkeypatch):
    """Half the mirror is FOI/scraper indexes with no geometry at all. Offering
    them a /features URL would hand back a 404 and teach the caller that the
    pointer lies."""
    d = _detail(monkeypatch, {**_MIRROR, "has_geom": False})
    assert "features_url" not in d and "geometry" not in d


def test_a_lookup_failure_degrades_to_the_plain_message(monkeypatch):
    """The pointer is a courtesy. It must never turn a 409 into a 500."""
    async def boom(_dataset_id):
        raise RuntimeError("append DB unreachable")

    monkeypatch.setattr(index_mirror, "mirrored_table", boom)
    assert asyncio.run(api._not_here_detail(_DS)) == "Dataset is not an append archive"
