"""A scraper version's tabular resources are N NEON tables, not one.

The bug: ykpubdata.jerusalem.muni.il publishes a row per BUILDING FILE and a row
per DOCUMENT in the same version — different grains, different column sets, two
separate CSVs on R2. push_version loaded every resource into
``append_store.table_name(ds)``, so NEON held one table with the union of the
column sets and the concatenation of the rows: measured on prod as 199 rows and
65 columns across three resources, where neither grain can be queried.

The read side already knew how to serve several tables (``_append_tables`` +
``?table=``, built for CKAN ``append_db_multi`` datasets); only the scraper write
path didn't produce them. So what is locked here is:

  1. the per-resource table name is stable across versions and derived from the
     resource NAME (a scraper resource has no id, and its r2 key is minted
     fresh every version);
  2. the layout RATCHETS to per-resource and never back, so a partial scrape
     can't re-merge the grains;
  3. ``_append_tables`` as an ordered ARRAY resolves and selects exactly like
     the CKAN dict shape it joins;
  4. a NEON table name in resource_mappings is never mistaken for a file — the
     "long string ⇒ an ODATA resource id" heuristic in three readers had only
     been saved by existing names being 27-29 chars against a >=30 bar.

No DB, no network: every assertion is over a pure function.
"""
import os
import sys
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ.setdefault("JWT_SECRET_KEY", "test")

import app.api.v1 as v1  # noqa: E402
import app.api.versions as versions_api  # noqa: E402
import app.api.worker as worker  # noqa: E402
from app.api.append import _pick  # noqa: E402
from app.services import append_store, storage_client  # noqa: E402
from app.services.r2_backfill import _named_r2_files  # noqa: E402

REGISTER = "נתוני הסורק"
DOCUMENTS = "מסמכי התיקים"
BLOCKS = "גושים וחלקות"


class _DS:
    """Just enough TrackedDataset for the name/mapping helpers."""
    def __init__(self, ckan_name="ykpubdata-documents-e68c8999",
                 ds_id="298ff0ab-909c-4da2-94d2-c89f75c3cd34"):
        self.ckan_name = ckan_name
        self.id = uuid.UUID(ds_id)
        self.resource_id = None
    odata_dataset_id = None


# ── the per-resource table name ──────────────────────────────────────────

def test_each_resource_gets_its_own_table():
    ds = _DS()
    names = {append_store.table_name_for_scraper_resource(ds, n)
             for n in (REGISTER, DOCUMENTS, BLOCKS)}
    assert len(names) == 3


def test_the_name_is_stable_across_versions_and_datasets():
    """Stability is the whole point — an append table accumulates across
    versions, so a name that moved would strand every row already in it. It is
    derived from (dataset, resource name) and nothing else."""
    ds = _DS()
    first = append_store.table_name_for_scraper_resource(ds, DOCUMENTS)
    assert first == append_store.table_name_for_scraper_resource(_DS(), DOCUMENTS)
    other_ds = _DS(ds_id="75df82b2-3a27-43b3-9764-e9557fc6401b")
    assert append_store.table_name_for_scraper_resource(other_ds, DOCUMENTS) != first


def test_the_name_is_a_legal_postgres_identifier():
    """Hebrew resource names cannot be pasted into an identifier: Postgres clips
    at 63 BYTES (2 per Hebrew letter) and truncates silently, so two resources
    would collide on one table. Hence the hash."""
    ds = _DS(ckan_name="a-very-long-scraper-slug-that-eats-the-whole-identifier-budget")
    for name in (REGISTER, DOCUMENTS, "x" * 200):
        table = append_store.table_name_for_scraper_resource(ds, name)
        assert len(table.encode("utf-8")) <= 63
        assert table.isascii() and table.replace("_", "").isalnum()


# ── the layout rule ──────────────────────────────────────────────────────

def test_a_single_resource_dataset_keeps_its_existing_table():
    """52 of the 53 scraper NEON datasets on prod publish one resource. Their
    rows must not move."""
    assert worker.neon_per_resource({"archive_neon": True}, [REGISTER]) is False
    assert worker.neon_per_resource(None, []) is False


def test_several_resources_switch_the_dataset_to_per_resource_tables():
    assert worker.neon_per_resource({"archive_neon": True}, [REGISTER, DOCUMENTS]) is True


def test_the_layout_ratchets_and_never_merges_back():
    """A partial scrape returning one resource must not dump it back into the
    merged table of a dataset that has already been split."""
    already_split = {"archive_neon": True, "neon_tables_per_resource": True}
    assert worker.neon_per_resource(already_split, [REGISTER]) is True
    assert worker.neon_per_resource(already_split, []) is True


# ── resolving and selecting the tables ───────────────────────────────────

def _multi_mappings(ds):
    return {
        "_hashes": {"scraper": "abc"},
        "_resource_ids": [],
        REGISTER: "r2:datasets/x/v1/28fef89b_csv",
        DOCUMENTS: "r2:datasets/x/v1/99984234_csv",
        "_append_tables": [
            {"resource": REGISTER,
             "table": append_store.table_name_for_scraper_resource(ds, REGISTER)},
            {"resource": DOCUMENTS,
             "table": append_store.table_name_for_scraper_resource(ds, DOCUMENTS)},
        ],
    }


def test_the_array_shape_resolves_to_one_entry_per_resource():
    ds = _DS()
    out = append_store.tables_from_mappings(ds, _multi_mappings(ds))
    assert [t["resource_name"] for t in out] == [REGISTER, DOCUMENTS]
    assert len({t["table"] for t in out}) == 2


def test_the_array_shape_keeps_the_order_the_resources_were_scraped_in():
    """jsonb does not preserve object key order, which is why the scraper shape
    is an ARRAY: callers default to the first table, so that choice has to be
    the first resource, not whichever key sorts first."""
    ds = _DS()
    m = _multi_mappings(ds)
    m["_append_tables"] = list(reversed(m["_append_tables"]))
    out = append_store.tables_from_mappings(ds, m)
    assert [t["resource_name"] for t in out] == [DOCUMENTS, REGISTER]


def test_a_table_is_selectable_by_resource_name_exactly_like_a_ckan_one():
    ds = _DS()
    tables = append_store.tables_from_mappings(ds, _multi_mappings(ds))
    assert _pick(tables, DOCUMENTS)["resource_name"] == DOCUMENTS
    assert _pick(tables, tables[1]["table"])["resource_name"] == DOCUMENTS
    assert _pick(tables, None)["resource_name"] == REGISTER  # the default


def test_an_unknown_selector_lists_what_exists_instead_of_serving_another_table():
    from fastapi import HTTPException

    ds = _DS()
    tables = append_store.tables_from_mappings(ds, _multi_mappings(ds))
    try:
        _pick(tables, "no-such-resource")
    except HTTPException as e:
        assert e.status_code == 404
        assert [a["resource_name"] for a in e.detail["available"]] == [REGISTER, DOCUMENTS]
    else:  # pragma: no cover
        raise AssertionError("an unmatched selector must 404, never fall back")


def test_a_single_resource_version_still_resolves_to_one_table():
    ds = _DS()
    single = {"_hashes": {}, "_resource_ids": [],
              REGISTER: "r2:datasets/x/v1/28fef89b_csv",
              "append_table": append_store.table_name(ds)}
    out = append_store.tables_from_mappings(ds, single)
    assert [t["table"] for t in out] == [append_store.table_name(ds)]


def test_the_ckan_dict_shape_is_untouched():
    """append_db_multi datasets keep resolving through _resource_ids."""
    ds = _DS()
    r1, r2 = "56063a99-8a3e-4ff4-912e-5966c0279bad", "bb2355dc-9ec7-4f06-9c3f-3344672171da"
    out = append_store.tables_from_mappings(ds, {
        "_resource_ids": [r1, r2],
        "_append_tables": {r1: "append_a_1", r2: "append_a_2"},
        "_names": {r1: "קובץ א", r2: "קובץ ב"},
    })
    assert [t["resource_id"] for t in out] == [r1, r2]
    assert [t["resource_name"] for t in out] == ["קובץ א", "קובץ ב"]


# ── a table name is not a file ───────────────────────────────────────────

def _neon_mappings(ds):
    return {"_hashes": {}, "_resource_ids": [],
            REGISTER: "r2:datasets/x/v1/28fef89b_csv",
            "append_table": append_store.table_name(ds)}


def test_a_neon_table_is_not_published_as_a_downloadable_resource():
    ds = _DS()
    got = {r.name for r in v1._extract_version_resources(ds, _neon_mappings(ds))}
    assert got == {REGISTER}
    multi = {r.name for r in v1._extract_version_resources(ds, _multi_mappings(ds))}
    assert multi == {REGISTER, DOCUMENTS}


def test_deleting_a_version_never_asks_odata_to_delete_a_table():
    ds = _DS()
    assert versions_api._extract_resource_ids(_neon_mappings(ds)) == []
    assert versions_api._extract_resource_ids(_multi_mappings(ds)) == []


def test_a_drive_export_does_not_try_to_fetch_a_table():
    ds = _DS()
    assert [name for name, _ in storage_client.enumerate_files(_neon_mappings(ds))] \
        == ["data.csv"]


# ── the seed replays every resource, not just the first ──────────────────

def test_the_seed_sees_every_data_file_in_a_version():
    """It used to take the first named r2 value and break, so a rebuild of a
    multi-resource dataset silently dropped all but one grain."""
    ds = _DS()
    files = _named_r2_files(_multi_mappings(ds))
    assert [name for name, _ in files] == [REGISTER, DOCUMENTS]
    assert all(val.startswith("r2:") for _, val in files)
