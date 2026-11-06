"""Israel's outline and the GovMap features outside it — the pure parts."""
from datetime import datetime, timedelta, timezone

from app.services import dataset_facets, field_flags, govmap_abroad as ga


def test_layer_id_of_reads_the_govmap_layer_number():
    assert ga.layer_id_of("govmap_376_41d39d22_fb79e65e") == "376"
    assert ga.layer_id_of("govmap_119_31b46b93_efc47aea") == "119"
    assert ga.layer_id_of("append_cbs_pub_file_42a1e1f0_693e114c") is None
    assert ga.layer_id_of("govmap_x_1") is None


def test_needs_check_on_new_layer_new_version_or_new_outline():
    built = datetime(2026, 11, 6, tzinfo=timezone.utc)
    assert ga.needs_check(None, 3, built)
    prev = {"version_number": 3, "boundary_built_at": built}
    assert not ga.needs_check(prev, 3, built)
    assert ga.needs_check(prev, 4, built)                       # layer changed
    assert ga.needs_check(prev, 3, built + timedelta(hours=1))  # outline rebuilt
    assert ga.needs_check({"version_number": 3, "boundary_built_at": None}, 3, built)
    # a different tolerance re-checks; the same one does not
    prev_tol = {**prev, "tolerance_m": 10.0}
    assert not ga.needs_check(prev_tol, 3, built, 10.0)
    assert ga.needs_check(prev_tol, 3, built, 25.0)
    assert ga.needs_check(prev, 3, built, 10.0)   # checked before tolerances existed


def test_tolerance_keeps_points_near_the_border_out_of_abroad():
    insert0, _ = ga._check_sql("govmap_442_7e4323a2_2c6b3fe1", 0.0)
    assert "ST_DWithin" not in insert0
    insert10, counts = ga._check_sql("govmap_442_7e4323a2_2c6b3fe1", 10.0)
    assert "ST_DWithin" in insert10 and ", 10.0)" in insert10
    assert insert10.count("AND NOT EXISTS") == 2      # touches nothing, and not near
    assert "in_any" in counts


def test_tolerance_setting_is_never_negative(monkeypatch):
    monkeypatch.setattr(ga.settings, "govmap_abroad_tolerance_m", -5, raising=False)
    assert ga.tolerance_m() == 0.0
    monkeypatch.setattr(ga.settings, "govmap_abroad_tolerance_m", 25, raising=False)
    assert ga.tolerance_m() == 25.0


def test_abroad_means_touching_no_region_at_all():
    insert, counts = ga._check_sql("govmap_376_41d39d22_fb79e65e")
    assert "govmap_376_41d39d22_fb79e65e" in insert
    # one NOT EXISTS over the pieces of EVERY region (land, איו"ש and sea)
    assert insert.count("AND NOT EXISTS") == 1
    assert "p.region =" not in insert
    assert "ST_Intersects" in insert and "&&" in insert
    # the row alias must not be a word a layer could use as a column name
    assert "to_jsonb(zz_src)" in insert
    # the per-region counts name the two land regions; the sea is the rest
    assert f"p.region = '{ga.REGION_ISRAEL}'" in counts
    assert f"p.region = '{ga.REGION_JUDEA_SAMARIA}'" in counts
    assert "in_judea_samaria" in counts


def test_maritime_rings_chain_the_atlas_lines_and_close_overland():
    sql, params = ga._maritime_sql("govmap_232711_f8a56dde_e4d90588")
    # every line of both chains is looked up by name, in chain order
    assert params == [p for c in ga.MARITIME_CLOSURES for p in c["lines"]]
    assert sql.count("ST_MakePolygon") == len(ga.MARITIME_CLOSURES)
    # the closing path ends where the chain starts
    assert "ST_EndPoint" in sql and "ST_StartPoint" in sql
    # the sea is what the rings enclose minus the two land regions
    assert "ST_Difference" in sql
    assert f"'{ga.REGION_ISRAEL}', '{ga.REGION_JUDEA_SAMARIA}'" in sql
    for c in ga.MARITIME_CLOSURES:
        for lon, lat in c["path"]:
            assert 34.0 < lon < 35.6 and 29.4 < lat < 33.2   # inside Israel's span


def test_abroad_is_a_content_field_but_not_a_column_name_flag():
    assert dataset_facets.CONTENT_FIELDS["abroad"][0] == ga.FLAG_KEY
    # field_flags.recompute merges only the keys it computes from column names;
    # has_abroad is written by govmap_abroad and must never be reset by it.
    assert ga.FLAG_KEY not in field_flags.ALL_FIELDS
    assert dataset_facets.content_fields_of({"has_abroad": True}) == ["abroad"]
