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


def test_check_sql_tests_both_regions_through_the_indexed_parts():
    insert, counts = ga._check_sql("govmap_376_41d39d22_fb79e65e")
    assert "govmap_376_41d39d22_fb79e65e" in insert
    # abroad = touches neither region: one NOT EXISTS per region
    assert insert.count("AND NOT EXISTS") == 2
    assert f"p.region = '{ga.REGION_ISRAEL}'" in insert
    assert f"p.region = '{ga.REGION_JUDEA_SAMARIA}'" in insert
    assert "ST_Intersects" in insert and "&&" in insert
    # the row alias must not be a word a layer could use as a column name
    assert "to_jsonb(zz_src)" in insert
    assert "in_israel" in counts


def test_abroad_is_a_content_field_but_not_a_column_name_flag():
    assert dataset_facets.CONTENT_FIELDS["abroad"][0] == ga.FLAG_KEY
    # field_flags.recompute merges only the keys it computes from column names;
    # has_abroad is written by govmap_abroad and must never be reset by it.
    assert ga.FLAG_KEY not in field_flags.ALL_FIELDS
    assert dataset_facets.content_fields_of({"has_abroad": True}) == ["abroad"]
