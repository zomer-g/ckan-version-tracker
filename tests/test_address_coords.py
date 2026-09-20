"""נדל"ן לעם — the address spine's coordinates: ITM, and where a point came from.

Three things are pinned here, and each of them was a real defect:

* **A rebuild used to delete every geocoded point.** ``build_addresses``
  TRUNCATEs, and the merge only considers ``merged = false`` — so the 93,988
  points GovMap contributed (of 451,667, measured 2026-09-20) would have been
  wiped and never re-merged, and the build would have reported success. The
  rebuild must clear the flag.
* **ITM was computed and thrown away.** The register publishes רשת ישראל and
  the build converts it to WGS84; without stored ITM columns every consumer
  converts it back by hand on each export.
* **Provenance cannot be reconstructed afterwards.** ``over_re_geocode`` is not
  readable by the console role, and the two kinds of point differ in accuracy
  by more than an order of magnitude — so whoever sets a point must say so.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import inspect  # noqa: E402

from app.services import geocode_queue as gq  # noqa: E402
from app.services import nadlan_index as ni  # noqa: E402


def _addresses_ddl() -> str:
    ddl = [d for d in ni._DDL if ni.ADDRESSES_TABLE in d]
    assert len(ddl) == 1, "expected exactly one CREATE TABLE for the addresses"
    return ddl[0]


# ── the rebuild must not destroy the geocoded points ──────────────────────────
def test_rebuild_lets_the_geocoded_points_back_in():
    """TRUNCATE + `merged = true` = silent permanent loss. Clearing the flag is
    the only thing that makes a rebuild recoverable, so it must sit in the same
    function as the TRUNCATE."""
    src = inspect.getsource(ni.build_addresses)
    assert "TRUNCATE" in src
    assert "over_re_geocode" in src, "the rebuild never touches the geocode table"
    assert "merged = false" in src, "the rebuild does not clear `merged`"
    truncate_at = src.index("TRUNCATE")
    reset_at = src.index("merged = false")
    assert reset_at > truncate_at, "the reset must follow the TRUNCATE"


def test_the_reset_survives_a_missing_geocode_table():
    """geocode_queue imports THIS module, so its table can legitimately not
    exist yet. An unguarded UPDATE would abort the whole build transaction."""
    src = inspect.getsource(ni.build_addresses)
    assert "to_regclass('public.over_re_geocode')" in src


# ── ITM is stored, not recomputed by every consumer ───────────────────────────
def test_the_addresses_table_stores_itm():
    ddl = _addresses_ddl()
    for col in ("itm_x", "itm_y"):
        assert col in ddl, f"{col} missing from the addresses DDL"
    assert "GENERATED ALWAYS AS" in ddl and "STORED" in ddl
    assert f"ST_Transform(point, {ni.ITM_SRID})" in ddl
    assert ni.ITM_SRID == 6991, "the repo's canonical ITM SRID moved"


def test_itm_is_also_added_to_tables_that_already_exist():
    """CREATE TABLE IF NOT EXISTS never evolves a live table — the production
    one is three years old. Every post-launch column needs the ALTER too."""
    added = {col for _t, col, _ty in ni._COLUMN_ADDITIONS}
    assert {"itm_x", "itm_y", "point_source"} <= added


# ── provenance is stamped by whoever sets the point ───────────────────────────
def test_the_register_stamps_its_own_points():
    src = inspect.getsource(ni.build_addresses)
    assert "'address_register'" in src
    assert "point_source" in src


def test_the_geocoder_stamps_the_points_it_merges():
    src = inspect.getsource(gq.merge_into_addresses) if hasattr(
        gq, "merge_into_addresses") else inspect.getsource(gq)
    assert "point_source = 'govmap'" in src


def test_the_backfill_only_ever_fills_blanks():
    """It runs on every startup. Keyed on NULL it retires itself; keyed on
    anything else it would overwrite the stamps the producers just wrote."""
    src = inspect.getsource(ni._backfill_point_source)
    # Count in the CODE only — the docstring names the same condition.
    body = src.split('"""', 2)[2]
    assert body.count("point_source IS NULL") == 2, (
        "both backfill statements must be keyed on a NULL provenance")
    assert "'govmap'" in src and "'address_register'" in src
    # The govmap half must read the authoritative list, not guess by distance.
    assert "over_re_geocode" in src
    assert "ST_DWithin" not in src and "ST_Distance" not in src
