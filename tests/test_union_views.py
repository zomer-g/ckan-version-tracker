"""One view over a dataset split into a table per resource (rain forecast)."""
from app.services import union_views as uv

RAIN = uv.UNION_VIEWS["d3d20a89-5c41-4b49-a7a8-cc1c73e070f3"]


def test_station_from_resource_name():
    assert uv.label_of("ELON_pr_15models_rcp45_rcp85_QDM.csv", "t", RAIN["label_regex"]) == "ELON"
    assert uv.label_of("BEER_SHEVA_pr_x.csv", "t", RAIN["label_regex"]) == "BEER_SHEVA"


def test_label_falls_back_to_name_then_table():
    assert uv.label_of("odd name.csv", "t", RAIN["label_regex"]) == "odd name.csv"
    assert uv.label_of(None, "append_x_1", RAIN["label_regex"]) == "append_x_1"


def test_view_sql_unions_with_label_and_hides_internal_columns():
    cols = {"a": ["year", "pr", "first_seen", "row_hash"],
            "b": ["year", "pr", "model", "first_seen", "row_hash"]}
    sql = uv.view_sql(RAIN, [("a", "ELON"), ("b", "O'HARA")], cols)
    parts = sql.split("\nUNION ALL\n")
    assert len(parts) == 2
    assert parts[0] == ('SELECT \'ELON\'::text AS "station", "year", "pr", "first_seen", '
                        'NULL::text AS "model" FROM public."a"')
    assert "'O''HARA'::text" in parts[1] and '"model"' in parts[1]
    assert "row_hash" not in sql


def test_signature_changes_with_tables_or_columns():
    b = [("a", "ELON")]
    s1 = uv.signature(b, {"a": ["x"]})
    assert s1 == uv.signature(b, {"a": ["x"]})
    assert s1 != uv.signature(b, {"a": ["x", "y"]})
    assert s1 != uv.signature([("a", "ELON"), ("b", "B")], {"a": ["x"]})


def test_spec_for_view():
    assert uv.spec_for_view("rain_forecast_stations")[0] == "d3d20a89-5c41-4b49-a7a8-cc1c73e070f3"
    assert uv.spec_for_view("nope") is None
