"""The count on a closed cube must match what the cube shows when opened."""
from app.services import nadlan_layers as nl


def test_count_ignores_noise_only_rows_and_duplicates():
    sql = nl._distinct_count_sql(["objectId", "mt_heb", "codemt", "_row_hash"])
    # only reader-facing columns take part
    assert '"mt_heb"' in sql and '"codemt"' in sql
    assert '"objectId"' not in sql and '"_row_hash"' not in sql
    # identical rows once, rows with nothing to show not at all
    assert sql.startswith("count(DISTINCT md5(")
    assert "FILTER (WHERE coalesce(" in sql


def test_a_layer_with_only_noise_columns_counts_nothing():
    # a regional-committee outline that carries nothing but its objectId
    assert nl._distinct_count_sql(["objectId", "_row_hash", "_first_seen"]) == "0::bigint"


def test_empty_markers_are_empty_like_the_cube_says():
    v = nl._value_sql("x")
    for marker in ("''", "'null'", "'none'", "'nan'"):
        assert marker in v


def test_a_square_metre_column_is_not_captioned_in_square_kilometres():
    assert nl._label("area_sqm", {"area_sqm": 'שטח קמ"ר'}) == "שטח (מ״ר)"
    assert nl._label("dunam", {"dunam": "שטח בדונם"}) == "שטח בדונם"
    assert nl._label("x", {}) == "x"
