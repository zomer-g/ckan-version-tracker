"""Table → dataset / project attribution for the admin storage view."""
from app.api.admin_storage import (
    PROJECT_DATASETS,
    PROJECT_INDEXES,
    PROJECT_LABELS,
    PROJECT_ORPHANS,
    dsid_of,
    project_of,
)

KNOWN = {"f41fb496", "e68c8999", "31da7c29", "cb500000"}


def test_dsid_of_every_table_name_shape():
    # append_store.table_name
    assert dsid_of("append_cbs_index_cb500000", KNOWN) == "cb500000"
    # append_store.table_name_for_resource / _for_scraper_resource
    assert dsid_of("append_taxes_nadlan_full_f41fb496_fd06f5ae", KNOWN) == "f41fb496"
    assert dsid_of("append_ykpubdata_documents_e68c8999_298ff0ab", KNOWN) == "e68c8999"
    # index_mirror: <base>_<dsid8>_<hash8>
    assert dsid_of("govmap_56_31da7c29_6adddf80", KNOWN) == "31da7c29"


def test_dsid_of_unknown_dataset_is_none():
    assert dsid_of("append_something_deadbeef", KNOWN) is None
    assert dsid_of("over_re_parcels", KNOWN) is None


def test_project_of():
    assert project_of("public", "append_x_f41fb496", True) == PROJECT_DATASETS
    assert project_of("public", "append_x_deadbeef", False) == PROJECT_ORPHANS
    assert project_of("idx", "govmap_56_31da7c29_6adddf80", True) == PROJECT_INDEXES
    assert project_of("idx", "whatever", False) == PROJECT_INDEXES
    assert project_of("public", "over_re_parcels", False) == "nadlan"
    assert project_of("public", "over_settlement_aliases", False) == "reference"
    assert project_of("public", "over_authorities", False) == "reference"
    assert project_of("public", "over_datasets", False) == "site_index"
    assert project_of("ocal", "events", False) == "ocal"
    assert project_of("knesset", "kns_bill", False) == "knesset"
    assert project_of("app", "tracked_datasets", False) == "system"
    assert project_of("public", "mystery", False) == "other"
    assert project_of("brand_new_schema", "t", False) == "other"


def test_every_project_has_a_label():
    for schema, table, has in [
        ("public", "append_a_b", True), ("public", "append_a_b", False),
        ("idx", "t", False), ("public", "over_re_x", False), ("ocoi", "t", False),
        ("odata", "t", False), ("public", "over_column_x", False), ("x", "t", False),
    ]:
        assert project_of(schema, table, has) in PROJECT_LABELS
