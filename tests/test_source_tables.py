"""Finding a source's tables when each table is a dataset of its own.

No Postgres: the schema read, the row estimates and the catalog are stubbed.
Pinned here, on the MK-expenses tables (the Ocal sync's consumer):

  * a role is matched by columns, and the summary (whose columns are a subset
    of the transactions') is not mistaken for the transactions or vice versa;
  * a table of a superseded version (not in the catalog) never answers;
  * while the old combined dataset and the new per-table one both exist, the
    per-table one wins;
  * another source's table with the same columns is not picked up;
  * the Ocal sync maps the roles back to the resource names it always used.
"""
import os

os.environ.setdefault("JWT_SECRET_KEY", "test")

import pytest

from app.services import ocal_mk_expenses as ome
from app.services import source_tables

TX = ["שנת הדוח", "שם חבר הכנסת", "שם סעיף הוצאה", "שם בית עסק/ ספק",
      "תאריך ביצוע/ תאריך חשבונית", 'סכום בש"ח', "פרטים/ הערות", "אשראי",
      "אסמכתאות לעסקה", "מספר מופע"]
SUMMARY = ["שנת הדוח", "שם חבר הכנסת", "שם סעיף הוצאה", 'סכום בש"ח']
GLOSSARY = ["שנת הדוח", "שם סעיף הוצאה", "מהות ההוצאה"]
ARCH = ["first_seen", "last_seen"]

OLD = "6ee5fb22-749f-447b-b7b0-ccec5e258b6c"     # the combined dataset
NEW_TX = "aaaaaaaa-0000-4000-8000-000000000001"
PREFIX = "mkexpenses-scraper-"


def _stub(monkeypatch, tables):
    async def _cols(schema):
        return {t: [{"name": c} for c in cs] for t, cs in tables.items()}

    async def _est(table, schema="public"):
        return 100

    monkeypatch.setattr(source_tables.append_store, "schema_table_columns", _cols)
    monkeypatch.setattr(source_tables.append_store, "table_count_estimate", _est)


def _cat(**tables):
    """table=(dataset_id, ckan_id, n_tables)"""
    return {t: {"dataset_id": d, "ckan_id": c, "n_tables": n} for t, (d, c, n) in tables.items()}


def test_the_summary_is_not_the_transactions():
    roles = ome._roles()
    assert roles[ome.DETAIL_RESOURCE].matches(set(TX))
    assert not roles[ome.SUMMARY_RESOURCE].matches(set(TX))
    assert roles[ome.SUMMARY_RESOURCE].matches(set(SUMMARY))
    assert not roles[ome.DETAIL_RESOURCE].matches(set(SUMMARY))
    assert roles[ome.EXPLAIN_RESOURCE].matches(set(GLOSSARY))


@pytest.mark.asyncio
async def test_after_the_split_the_per_table_dataset_answers(monkeypatch):
    _stub(monkeypatch, {
        "append_old_tx": TX + ARCH, "append_old_summary": SUMMARY + ARCH,
        "append_new_tx": TX + ARCH,
        "append_stale_glossary": GLOSSARY + ARCH,          # superseded version
        "append_other_source": SUMMARY + ARCH,             # same columns, other source
    })
    catalog = _cat(
        append_old_tx=(OLD, PREFIX + "page-1", 2),
        append_old_summary=(OLD, PREFIX + "page-1", 2),
        append_new_tx=(NEW_TX, PREFIX + "transactions-2", 1),
        append_other_source=("bbbbbbbb-0000-4000-8000-000000000002", "other-scraper-x", 1),
    )
    found = await source_tables.find(ome._roles(), ckan_prefix=PREFIX, catalog=catalog)
    assert found[ome.DETAIL_RESOURCE]["table"] == "append_new_tx"
    assert found[ome.DETAIL_RESOURCE]["dataset_id"] == NEW_TX
    assert found[ome.SUMMARY_RESOURCE]["table"] == "append_old_summary"
    assert ome.EXPLAIN_RESOURCE not in found


@pytest.mark.asyncio
async def test_the_ocal_sync_gets_its_resource_names_back(monkeypatch):
    async def _find(roles, ckan_prefix=None, catalog=None):
        assert ckan_prefix == ome.MK_EXPENSES_CKAN_PREFIX
        return {ome.DETAIL_RESOURCE: {"table": "append_new_tx"},
                ome.SUMMARY_RESOURCE: {"table": "append_new_summary"}}

    monkeypatch.setattr(source_tables, "find", _find)
    tables = await ome._over_tables(OLD)
    assert tables == {ome.DETAIL_RESOURCE: "append_new_tx",
                      ome.SUMMARY_RESOURCE: "append_new_summary"}
