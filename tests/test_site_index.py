"""The site's own index — public.over_datasets / over_dataset_files.

Covers the pure row-building helpers and the two invariants that make this table
trustworthy: the COPY column list matches the DDL exactly, and nothing in the
table describes HOW anything is collected.
"""
import re

from app.services import site_index


# ── size bands ───────────────────────────────────────────────────────────────

def test_size_class_bands():
    assert site_index._size_class(0) == "small"
    assert site_index._size_class(9_999) == "small"
    assert site_index._size_class(10_000) == "medium"
    assert site_index._size_class(99_999) == "medium"
    assert site_index._size_class(100_000) == "large"
    assert site_index._size_class(5_000_000) == "very_large"


def test_size_class_is_unknown_rather_than_guessed():
    """A dataset whose row count we do not have must not be labelled 'small' —
    that is exactly the gap this index exists to make visible."""
    assert site_index._size_class(None) is None


# ── files + formats ──────────────────────────────────────────────────────────

def test_files_expands_aggregate_keys_and_skips_bookkeeping():
    mappings = {
        "_geojson": ["r2:a/b.geojson"],
        "_gpkg": ["r2:a/b.gpkg"],
        "_parquet": ["r2:a/b.parquet"],
        "_hashes": {"x": "y"},                 # bookkeeping, not a file
        "append_table": "append_something",    # a table name, not a file
        "נתונים.csv": "r2:a/data.csv",
    }
    files = site_index._files_of(mappings, None)
    assert {f["format"] for f in files} == {"GeoJSON", "GPKG", "GeoParquet", "CSV"}
    assert len(files) == 4


def test_format_comes_from_the_source_declaration_when_the_name_has_none():
    """Most named CKAN resources carry no extension; the source declares the
    format in the version's change summary instead."""
    files = site_index._files_of(
        {"רשימת בתי ספר": "r2:x/y"},
        {"resources": [{"name": "רשימת בתי ספר", "format": "xlsx"}]})
    assert files == [{"name": "רשימת בתי ספר", "format": "XLSX"}]


def test_a_short_opaque_value_is_not_counted_as_a_file():
    assert site_index._files_of({"k": "short"}, None) == []


# ── content type ─────────────────────────────────────────────────────────────

def test_a_geopackage_only_layer_is_still_a_mapping_layer():
    """The case the whole index has to get right: a layer too large to hold as a
    queryable table is a mapping layer all the same. Judged by what it HOLDS —
    which is also why this stays true however the file came to be."""
    assert site_index._content_type(
        has_geometry=False, formats={"GPKG", "GeoParquet"}, files=2,
        queryable=False) == "mapping_layer"


def test_a_queryable_table_of_geometry_is_a_mapping_layer_too():
    assert site_index._content_type(True, {"CSV"}, 1, True) == "mapping_layer"


def test_a_pile_of_pdfs_is_a_document_collection():
    assert site_index._content_type(False, {"PDF"}, 400, False) == "document_collection"


def test_a_plain_table_is_a_data_table():
    assert site_index._content_type(False, {"CSV"}, 1, True) == "data_table"


def test_a_dataset_with_nothing_stored_says_so():
    assert site_index._content_type(False, set(), 0, False) == "catalog_only"


# ── geometry status ──────────────────────────────────────────────────────────

def _cols(*names):
    return [{"name": n} for n in names]


def test_geometry_status_reports_the_three_real_states():
    assert site_index._geometry_status(
        "mapping_layer", _cols("a", "geometry_wkt", "geom"), True) == "spatial_queries"
    assert site_index._geometry_status(
        "mapping_layer", _cols("a", "geometry_wkt"), True) == "text_only"
    assert site_index._geometry_status(
        "mapping_layer", [], False) == "download_only"
    assert site_index._geometry_status(
        "data_table", _cols("a"), True) == "not_applicable"


# ── the two structural invariants ────────────────────────────────────────────

def _ddl_columns(ddl: str) -> list[str]:
    body = ddl[ddl.index("(") + 1: ddl.rindex(")")]
    out = []
    for line in body.splitlines():
        m = re.match(r"\s*([a-z_]+)\s+\S", line)
        if m:
            out.append(m.group(1))
    return out


def test_copy_columns_match_the_ddl_exactly_and_in_order():
    """asyncpg's COPY is positional: a column added to one and not the other
    silently shifts every value one place to the left."""
    assert _ddl_columns(site_index._DATASETS_DDL) == site_index._DATASETS_COLS
    assert _ddl_columns(site_index._FILES_DDL) == site_index._FILES_COLS


# Words that would describe HOW data is collected rather than WHAT is held. The
# user's explicit requirement for this table: it is a public index of holdings,
# and the plumbing behind it must not be readable from it — not least because
# the plumbing changes without the holdings changing.
_TECHNIQUE_WORDS = (
    "worker", "scraper", "engine", "govmap", "ckan", "odata", "source_type",
    "storage_mode", "archive_neon", "append_only", "mirror", "poll_", "r2",
    "extension", "crawl", "fetch", "kind", "snapshot", "quad", "wfs",
)


def test_no_column_describes_how_the_data_is_collected():
    for col in site_index._DATASETS_COLS + site_index._FILES_COLS:
        for word in _TECHNIQUE_WORDS:
            assert word not in col, f"column {col!r} leaks collection technique"


def test_no_emitted_value_describes_how_the_data_is_collected():
    """The vocabularies this table writes are closed sets — pin them, so a later
    'just add the source type' cannot slip in as a value instead of a column."""
    values = set()
    for band in (0, 50_000, 500_000, 5_000_000):
        values.add(site_index._size_class(band))
    for args in ((False, {"GPKG"}, 1, False), (True, set(), 1, True),
                 (False, {"PDF"}, 9, False), (False, {"CSV"}, 1, True),
                 (False, set(), 0, False)):
        values.add(site_index._content_type(*args))
    for args in (("mapping_layer", _cols("geom"), True),
                 ("mapping_layer", _cols("geometry_wkt"), True),
                 ("mapping_layer", [], False),
                 ("data_table", [], True)):
        values.add(site_index._geometry_status(*args))
    assert values == {
        "small", "medium", "large", "very_large",
        "mapping_layer", "data_table", "document_collection", "catalog_only",
        "spatial_queries", "text_only", "download_only", "not_applicable",
    }
