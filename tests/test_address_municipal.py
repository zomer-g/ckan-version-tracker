"""נדל"ן לעם — folding the municipal address layers into the spine.

These layers are the answer to a measured dead end: GovMap's national search
returned **0 of 64** addresses OVER is missing (against 28 of 32 on a control,
so the service was answering), while the seven municipal layers hold **19,119**
addresses OVER has no row for. What is pinned here is the handful of rules that
decide whether folding them in improves the corpus or quietly corrupts it.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import inspect  # noqa: E402

from app.services import address_municipal as am  # noqa: E402


# ── the layer table ───────────────────────────────────────────────────────────
def test_every_layer_names_its_own_columns():
    """There is no shared schema — each municipality published whatever its GIS
    had. A layer whose columns are assumed rather than read is a layer that
    silently inserts nothing, or the wrong thing."""
    assert len(am.LAYERS) == 7
    seen_tables = set()
    for table, town, street, house, _ot in am.LAYERS:
        assert table.startswith("govmap_")
        assert table not in seen_tables, f"{table} listed twice"
        seen_tables.add(table)
        assert town and street and house


def test_the_layers_are_the_ones_that_were_measured():
    towns = {t for _tbl, t, _s, _h, _o in am.LAYERS}
    assert towns == {"באר שבע", "מודיעין מכבים רעות", "נס ציונה", "ערד",
                     "שדרות", "קרית טבעון", "בית אל"}


# ── never overwrite a point ───────────────────────────────────────────────────
def test_an_existing_point_is_never_overwritten():
    """A coordinate already in the spine is the official register's (exact to
    the centimetre) or a geocode. A municipality's own point is not
    automatically better, and this merge is not the place to adjudicate that."""
    src = inspect.getsource(am.merge)
    assert "o.point IS NULL" in src, "the UPDATE must be restricted to empty points"


def test_the_insert_cannot_clobber_an_existing_row():
    src = inspect.getsource(am.merge)
    assert "ON CONFLICT (address_key) DO NOTHING" in src


# ── provenance ────────────────────────────────────────────────────────────────
def test_every_point_is_labelled_with_ITS_layer():
    """Not a bare 'municipal': seven publishers, seven update cycles. A reader
    tracing a suspect coordinate needs to know which one produced it."""
    src = inspect.getsource(am.merge)
    assert src.count("'municipal_") >= 2, "both the UPDATE and the INSERT must stamp"
    assert "'municipal'" not in src, "the stamp must name the layer, not just 'municipal'"


def test_the_stamp_is_derived_from_the_layer_id():
    src = inspect.getsource(am.merge)
    assert 'table.split("_")[1]' in src


# ── the guard against a republished layer ─────────────────────────────────────
def test_a_layer_that_stops_parsing_is_skipped_not_partially_imported():
    """A layer republished with different column semantics parses as almost
    nothing. Importing 'whatever parsed' would put a handful of rows in and
    report success — the shape of failure this project keeps meeting."""
    src = inspect.getsource(am.merge)
    assert "MIN_KEYED_RATIO" in src
    assert "continue" in src, "a failing layer must be skipped, not imported"
    assert 0.0 < am.MIN_KEYED_RATIO < 1.0


def test_the_skip_is_reported_and_not_silent():
    src = inspect.getsource(am.merge)
    assert 'result["skipped"]' in src


# ── keying ────────────────────────────────────────────────────────────────────
def test_an_unresolvable_street_is_dropped_rather_than_guessed():
    """7 of 2,351 layer street spellings do not resolve. Inventing a key for
    them would file addresses under a street that does not exist."""
    src = inspect.getsource(am.merge)
    assert "street_key IS NOT NULL" in src


def test_the_settlement_name_comes_from_the_code_not_the_layer_spelling():
    """over_settlement() takes a NAME and returns NULL for a numeric code, and
    the layers spell towns their own way (מודיעין מכבים רעות vs the canonical
    מודיעין-מכבים-רעות). Verified live: over_settlement('9000') is NULL."""
    src = inspect.getsource(am.merge)
    assert "over_settlements t ON t.code = d.sc" in src
    assert "over_settlement(d.sc" not in src


def test_the_address_key_matches_the_spine_format():
    """`{settlement}|{street_key}|{house}{suffix}|{entrance}` — anything else
    creates a duplicate row the ON CONFLICT cannot catch."""
    src = inspect.getsource(am.merge)
    assert "d.sc || '|' || d.street_key || '|' ||" in src
    assert "coalesce(d.house_suffix,'') || '|'" in src


def test_a_dry_run_writes_nothing():
    src = inspect.getsource(am.merge)
    assert "if dry_run:" in src
    i_dry = src.index("if dry_run:")
    i_update = src.index("UPDATE public")
    assert i_dry < i_update, "the dry-run check must come before any write"


def test_the_report_does_not_write():
    src = inspect.getsource(am.layer_report)
    for verb in ("INSERT", "UPDATE ", "DELETE"):
        assert verb not in src, f"{verb} has no business in a report"


def test_geometry_is_centroided_because_only_three_layers_are_points():
    """Only 3 of the 7 layers are ST_Point: ערד and קרית טבעון publish
    ST_MultiPoint and שדרות ST_MultiLineString. ST_X/ST_Y reject anything that
    is not a POINT — which is how the first production run died, after three
    layers had already committed. The target column is geometry(Point,4326)
    and would have rejected the raw geometry regardless."""
    import inspect
    src = inspect.getsource(am._layer_cte)
    assert "ST_Centroid" in src
    assert "ST_Y(extensions.ST_Centroid" in src
    assert "ST_Y(l.geom)" not in src, "a bare ST_Y crashes on the 4 non-point layers"
