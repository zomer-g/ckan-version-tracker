"""A table the console already serves must not silently freeze.

The size cap exists because streaming a huge CSV on a 512MB dyno once OOM-killed
the web process. But it was applied to every sync alike, so a dataset whose CSV
grew past it stayed in the catalog answering queries about whatever version it
happened to hold when it crossed the line — with nothing in the UI to say so.
That is how "רשויות מקומיות" (57MB) sat outside the console entirely while being
tracked all along.

The rule pinned here: the cap gates what we START mirroring; a table that is
already live refreshes past it, up to a hard ceiling that still protects the dyno.
"""
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.config import settings  # noqa: E402
from app.services import index_mirror as IM  # noqa: E402

MB = 2 ** 20
ITEM = {"dataset_id": "d1", "table": "govmap_125_x", "version_number": 3,
        "r2_value": "r2:some/key.csv", "title": "רשויות מקומיות"}


def _run(size_mb, *, live, cap_mb=25, monkey=None):
    """Call sync_one with a stubbed object size and liveness, capturing the
    deferral decision without touching R2 or the database."""
    recorded = {}

    async def fake_size(_value):
        return int(size_mb * MB)

    async def fake_live(_table):
        return live

    async def fake_record(*a, **kw):
        recorded.update(kw)

    async def fake_load(*a, **kw):
        recorded["loaded"] = True
        return {"rows": 411, "new_rows": 411, "mode": "rebuild", "columns": []}

    orig = (IM.storage_client.object_size, IM._table_is_live, IM._record, IM.load_index_csv)
    IM.storage_client.object_size, IM._table_is_live = fake_size, fake_live
    IM._record, IM.load_index_csv = fake_record, fake_load
    try:
        res = asyncio.run(IM.sync_one(dict(ITEM), max_bytes=cap_mb * MB))
    finally:
        (IM.storage_client.object_size, IM._table_is_live,
         IM._record, IM.load_index_csv) = orig
    return res, recorded


def test_a_new_oversized_dataset_is_still_deferred():
    # The original guard, untouched: nothing this big STARTS mirroring on its own.
    res, rec = _run(57.4, live=False)
    assert res.get("deferred")
    assert "57.4 MB" in res["deferred"]
    assert "loaded" not in rec


def test_an_already_served_table_refreshes_past_the_cap():
    res, rec = _run(57.4, live=True)
    assert not res.get("deferred")
    assert rec.get("loaded") is True


def test_the_hard_ceiling_still_protects_the_dyno():
    # Live or not, past the refresh ceiling we would rather serve stale rows than
    # take the process down with a multi-gigabyte stream.
    res, _ = _run(settings.index_mirror_refresh_max_csv_mb + 1, live=True)
    assert res.get("deferred")


def test_a_small_dataset_is_unaffected_either_way():
    for live in (True, False):
        res, rec = _run(3, live=live)
        assert not res.get("deferred")
        assert rec.get("loaded") is True


def test_the_load_cap_never_exceeds_the_refresh_ceiling():
    """The two caps are a pair, and raising the load one past the refresh one
    would break both of them at once: the "already served, refresh anyway"
    branch becomes unreachable (its window is cap → ceiling), and a table would
    be allowed to LOAD at a size it is never allowed to REFRESH at — so it would
    land in the catalog and freeze at version one, which is the exact failure
    the refresh ceiling was added to prevent.

    Live values, not constants: this is a guard on the settings, checked
    whenever the load cap is raised a tier (25 → 100 on 2026-08-19)."""
    assert (settings.index_mirror_max_csv_mb
            <= settings.index_mirror_refresh_max_csv_mb)
