"""The parcel crosswalk for GovMap layers too large for the idx mirror.

Two halves:

* pure tests of the CSV side — what a feature row becomes, which columns are
  kept, how batches are bounded, that a feature without geometry is skipped;
* an integration test of the build itself, against a real PostGIS. It runs
  only when ``NADLAN_PG_TEST_URL`` points at a throwaway database (it creates
  tables there), because the SQL — containment, radius, the generation flip —
  is the part a stub cannot vouch for.
"""
import asyncio
import json
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services import append_store, column_aliases, nadlan_layer_links as L, nadlan_layers

MAVAT = "b105ae22-a6f5-4cd0-b37e-c63dc5da40c3"        # mavat_plans, containment
PRICE = "b85a7bc3-4455-4600-86f7-343b138fa6c7"        # controlled_price, 300 m


def _csv(rows):
    head = "objectid,pl_number,pl_name,mavat_code,geometry_wkt\n"
    out = [head]
    for r in rows:
        out.append(",".join(f'"{v}"' for v in r) + "\n")
    return out


SQUARE_P1_P2 = "POLYGON((34.795 32.075,34.815 32.075,34.815 32.085,34.795 32.085,34.795 32.075))"
SQUARE_P1 = "POLYGON((34.799 32.079,34.801 32.079,34.801 32.081,34.799 32.081,34.799 32.079))"
SQUARE_NOWHERE = "POLYGON((35.5 31.0,35.51 31.0,35.51 31.01,35.5 31.01,35.5 31.0))"

ROWS = [
    ["1", "פת/1000", "תוכנית גדולה", "2000343621", SQUARE_P1_P2],
    ["2", "פת/2000", "", "", SQUARE_P1],
    ["3", "פת/3000", "רחוקה", "", SQUARE_NOWHERE],
    ["4", "פת/4000", "בלי גאומטריה", "", ""],
    ["2", "פת/2000", "", "", SQUARE_P1],                 # an exact duplicate row
]


# ── the CSV side ──
def test_a_feature_row_keeps_attributes_and_drops_plumbing():
    [(header, batch)] = list(L.iter_csv_batches(iter(_csv(ROWS[:1]))))
    key, wkt, attrs = batch[0]
    assert wkt == SQUARE_P1_P2
    assert json.loads(attrs) == {"pl_number": "פת/1000", "pl_name": "תוכנית גדולה",
                                 "mavat_code": "2000343621"}   # objectid, wkt dropped
    assert len(key) == 32 and key == L.feature_key(
        ["1", "פת/1000", "תוכנית גדולה", "2000343621", SQUARE_P1_P2])


def test_a_feature_without_geometry_is_skipped_and_blank_values_dropped():
    batches = list(L.iter_csv_batches(iter(_csv(ROWS))))
    keys = [b[0] for _, batch in batches for b in batch]
    assert len(keys) == 4                                  # row 4 has no geometry
    assert json.loads(batches[0][1][1][2]) == {"pl_number": "פת/2000"}


def test_batches_are_bounded_by_rows_and_by_bytes():
    assert [len(b) for _, b in L.iter_csv_batches(iter(_csv(ROWS)), batch_rows=2)] == [2, 2]
    by_bytes = list(L.iter_csv_batches(iter(_csv(ROWS)), batch_bytes=60))
    assert [len(b) for _, b in by_bytes] == [1, 1, 1, 1]


def test_a_csv_without_geometry_is_refused():
    with pytest.raises(ValueError):
        list(L.iter_csv_batches(iter(["a,b\n", "1,2\n"])))


def test_long_values_are_cut():
    row = ["1", "x" * 1000, "", "", SQUARE_P1]
    [(_, [(_, _, attrs)])] = list(L.iter_csv_batches(iter(_csv([row]))))
    assert len(json.loads(attrs)["pl_number"]) == L.MAX_ATTR_CHARS + 1


def test_the_match_is_containment_or_radius():
    assert "ST_Intersects" in L._match_sql(0)
    assert "try_geom" not in L._match_sql(0)          # the geometry is materialised
    sql = L._match_sql(300)
    assert "ST_DWithin" in sql and ", 300)" in sql and "ST_Expand" in sql


def test_every_cube_dataset_has_a_radius():
    assert nadlan_layers.radius_for_dataset(MAVAT) == 0
    assert nadlan_layers.radius_for_dataset(PRICE) == 300
    assert nadlan_layers.radius_for_dataset("not-a-cube") == 0


# ── the build, against a real PostGIS ──
PG = os.environ.get("NADLAN_PG_TEST_URL")


@pytest.fixture
def pg(monkeypatch):
    if not PG:
        pytest.skip("NADLAN_PG_TEST_URL not set")
    monkeypatch.setattr(append_store.settings, "append_database_url", PG)
    monkeypatch.setattr(append_store.settings, "append_readonly_database_url", PG)
    monkeypatch.setattr(append_store, "_pool", None)
    monkeypatch.setattr(append_store, "_ro_pool", None)
    L.invalidate_generations()
    nadlan_layers.invalidate_caches()

    async def _no_tables():
        return {}

    # Every cube dataset is "too large for idx" here, so the crosswalk is used.
    monkeypatch.setattr(nadlan_layers, "_geo_tables", _no_tables)
    return monkeypatch


def run(main):
    """One event loop per test: the pools and locks are bound to it."""
    async def wrapper():
        L._build_lock = asyncio.Lock()
        nadlan_layers._lock = asyncio.Lock()
        try:
            await _setup()
            return await main()
        finally:
            for name in ("_pool", "_ro_pool"):
                p = getattr(append_store, name)
                if p is not None:
                    await p.close()
                    setattr(append_store, name, None)
    return asyncio.run(wrapper())


async def _setup():
    from app.services import index_mirror
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute("CREATE SCHEMA IF NOT EXISTS extensions")
        await conn.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA extensions")
        await index_mirror.ensure_schema(conn)
        for t in (L.FEATURES_TABLE, L.STATE_TABLE, "over_re_parcels"):
            await conn.execute(f'DROP TABLE IF EXISTS public."{t}"')
        await conn.execute("""
            CREATE TABLE public.over_re_parcels (
                parcel_key text PRIMARY KEY,
                centroid extensions.geometry(Point, 4326))""")
        await conn.execute("""
            INSERT INTO public.over_re_parcels VALUES
              ('1-0-1', extensions.ST_SetSRID(extensions.ST_MakePoint(34.80, 32.08), 4326)),
              ('1-0-2', extensions.ST_SetSRID(extensions.ST_MakePoint(34.81, 32.08), 4326)),
              ('1-0-3', extensions.ST_SetSRID(extensions.ST_MakePoint(34.90, 32.10), 4326))""")
        await conn.execute("CREATE INDEX ON public.over_re_parcels USING gist (centroid)")
    L.invalidate_generations()


def _stub_source(monkeypatch, rows, version=3):
    async def _latest(ds):
        return version, {"נתוני הסורק": "r2:layer.csv", "_symbology": "r2:doc.zip"}

    async def _labels(values):
        return {"pl_number": "מספר תוכנית"}

    monkeypatch.setattr(L, "latest_version", _latest)
    monkeypatch.setattr(L, "_r2_lines", lambda value: iter(_csv(rows)))
    monkeypatch.setattr(column_aliases, "_bundle_dictionary", _labels)


async def _fetch(sql):
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        return [dict(r) for r in await conn.fetch(sql)]


def test_a_layer_is_linked_to_the_parcels_inside_it(pg):
    _stub_source(pg, ROWS)
    pg.setattr(L, "BATCH_ROWS", 2)                       # several batches

    async def main():
        res = await L.build(MAVAT)
        L.invalidate_generations()
        c = nadlan_layers.cube("mavat_plans")
        out = await nadlan_layers.spatial_cube(c, {"parcel_key": "1-0-1", "lat": 32.08,
                                                   "lon": 34.80})
        other = await nadlan_layers.spatial_cube(c, {"parcel_key": "1-0-3", "lat": 32.1,
                                                     "lon": 34.9})
        hits = await nadlan_layers.probe({"parcel_key": "1-0-1"})
        cat = {x["id"]: x for x in await nadlan_layers.catalog()}
        return res, out, other, hits, cat

    res, out, other, hits, cat = run(main)
    assert res["status"] == "ok", res
    assert res["features_read"] == 4                     # the no-geometry row skipped
    assert res["features_linked"] == 2                   # the far one is not kept
    assert res["links"] == 3                             # P1-F1, P2-F1, P1-F2
    assert out["count"] == 2 and out["available"] is True
    assert other["count"] == 0
    plan = next(f for f in out["features"] if any(x["value"] == "פת/1000" for x in f["fields"]))
    by_key = {x["key"]: x for x in plan["fields"]}
    assert by_key["pl_number"]["label"] == "מספר תוכנית"   # caption from the bundle
    assert by_key["mavat_code"]["url"] == "https://mavat.iplan.gov.il/SV4/1/2000343621/310"
    assert out["sources"][0]["row_url"].startswith("https://www.over.org.il/data?q=")
    assert hits["mavat_plans"] == 2
    assert cat["mavat_plans"]["available"] is True
    assert cat["taba_plots"]["available"] is False       # never built


def test_a_rebuild_flips_generations_and_drops_the_old_one(pg):
    async def main():
        _stub_source(pg, ROWS)
        first = await L.build(MAVAT)
        again = await L.build(MAVAT)
        _stub_source(pg, ROWS[:1], version=4)
        newer = await L.build(MAVAT)
        gens = fgens = await _fetch(
            f'SELECT DISTINCT generation FROM public."{L.FEATURES_TABLE}"')
        st = await _fetch(f'SELECT generation, version_number, building, status '
                          f'FROM public."{L.STATE_TABLE}"')
        return first, again, newer, gens, fgens, st

    first, again, newer, gens, fgens, st = run(main)
    assert first["generation"] == 1
    assert again["status"] == "current"                  # same version: nothing to do
    assert newer["generation"] == 2 and newer["links"] == 2
    assert gens == [{"generation": 2}] and fgens == [{"generation": 2}]
    assert st == [{"generation": 2, "version_number": 4, "building": None, "status": "ok"}]


def test_a_radius_layer_links_what_is_near_with_its_distance(pg):
    _stub_source(pg, [["1", "מגרש 7", "", "", "POINT(34.902 32.1)"]])   # ~190 m from P3

    async def main():
        res = await L.build(PRICE, radius_m=300)
        L.invalidate_generations()
        cube = await nadlan_layers.spatial_cube(nadlan_layers.cube("controlled_price"),
                                                {"parcel_key": "1-0-3", "lat": 32.1, "lon": 34.9})
        assert 150 < cube["features"][0]["distance_m"] < 250   # read back per parcel
        rows = await _fetch(f'SELECT parcels[1] AS parcel_key, distances[1] AS distance_m '
                            f'FROM public."{L.FEATURES_TABLE}"')
        return res, rows

    res, rows = run(main)
    assert res["links"] == 1
    [r] = rows
    assert r["parcel_key"] == "1-0-3" and 150 < r["distance_m"] < 250


def test_an_itm_era_layer_is_reprojected(pg):
    async def main():
        itm = (await _fetch(
            "SELECT extensions.ST_AsText(extensions.ST_Transform(extensions.ST_Buffer("
            "extensions.ST_SetSRID(extensions.ST_MakePoint(34.80, 32.08), 4326)::"
            "extensions.geography, 50)::extensions.geometry, 6991)) AS w"))[0]["w"]
        _stub_source(pg, [["1", "ITM", "", "", itm]])
        return await L.build(MAVAT)

    res = run(main)
    assert res["status"] == "ok" and res["links"] == 1


def test_a_failed_build_is_recorded_and_leaves_readers_alone(pg):
    async def main():
        _stub_source(pg, ROWS)
        await L.build(MAVAT)
        _stub_source(pg, [["1", "x", "", "", "NOT WKT AT ALL 99"]], version=5)
        res = await L.build(MAVAT)
        return res, (await L.get_state(MAVAT))[0]

    res, st = run(main)
    assert res["status"] == "failed"
    assert st["status"] == "failed" and st["generation"] == 1   # readers stay on gen 1


def test_the_r2_reader_streams_text_with_a_bom_and_hebrew(monkeypatch):
    import io

    raw = ("\ufeffpl_number,pl_name,geometry_wkt\n"
           '"פת/1","שורה\nשנייה","POINT(34.8 32.08)"\n').encode("utf-8")
    closed = {}

    class Body(io.BytesIO):
        def close(self):
            closed["yes"] = True
            super().close()

    class Client:
        def get_object(self, Bucket, Key):
            assert Key == "datasets/x/layer.csv"           # the r2: marker stripped
            return {"Body": Body(raw)}

    monkeypatch.setattr(L.storage_client, "_get_client", lambda: Client())
    [(header, [(_, wkt, attrs)])] = list(L.iter_csv_batches(L._r2_lines("r2:datasets/x/layer.csv")))
    assert header[0] == "pl_number"                        # BOM gone
    assert wkt == "POINT(34.8 32.08)"
    assert json.loads(attrs) == {"pl_number": "פת/1", "pl_name": "שורה\nשנייה"}
    assert closed == {"yes": True}


def test_the_scheduler_builds_the_listed_layers_in_order_one_per_tick(pg):
    _stub_source(pg, ROWS)
    pg.setattr(L.settings, "nadlan_layer_links_autobuild", f"{MAVAT}, {PRICE}")

    async def main():
        first = await L.build_pending()
        second = await L.build_pending()
        third = await L.build_pending()
        return first, second, third

    first, second, third = run(main)
    assert first["dataset_id"] == MAVAT and first["status"] == "ok"
    assert first["table_bytes"] > 0
    assert second["dataset_id"] == PRICE and second["status"] == "ok"
    assert third is None                                   # nothing left


def test_a_failed_layer_stops_the_rollout(pg):
    _stub_source(pg, [["1", "x", "", "", "NOT WKT AT ALL 99"]])
    pg.setattr(L.settings, "nadlan_layer_links_autobuild", f"{MAVAT},{PRICE}")

    async def main():
        failed = await L.build_pending()
        _stub_source(pg, ROWS)
        return failed, await L.build_pending(), await L.get_state(PRICE)

    failed, after, price = run(main)
    assert failed["status"] == "failed"
    assert after is None and price == []                   # not retried, next not started


def test_a_build_cut_short_by_a_restart_is_built_again_cleanly(pg):
    _stub_source(pg, ROWS)
    pg.setattr(L.settings, "nadlan_layer_links_autobuild", MAVAT)

    async def main():
        await L.ensure_tables()
        await L._set_state(MAVAT, status="building", building=1)
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:            # a half-written generation 1
            await conn.execute(
                f'INSERT INTO public."{L.FEATURES_TABLE}" VALUES '
                f"($1, 1, 'stale', '{{}}', ARRAY['1-0-1'], NULL)", MAVAT)
        res = await L.build_pending()
        keys = await _fetch(f'SELECT feature_key FROM public."{L.FEATURES_TABLE}"')
        return res, keys

    res, keys = run(main)
    assert res["status"] == "ok" and res["generation"] == 1 and res["links"] == 3
    assert "stale" not in {k["feature_key"] for k in keys}
