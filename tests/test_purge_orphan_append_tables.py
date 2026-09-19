"""Dropping the per-resource tables a merge left behind.

Merging a partitioned source into one table moves every row and retargets the
versions, but the tables it emptied stay on disk — invisible in /data and still
paid for. The real-estate corpus left 47 of them at 881 MB.

Dropping tables is the kind of cleanup that is only worth having if it cannot
take the wrong one, so the rules are pinned here rather than trusted: a
candidate must be one of THIS dataset's own per-resource tables by name shape,
and it must be named by no version at all.
"""
import asyncio
import os
import sys
import types
import uuid

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("JWT_SECRET_KEY", "test")

from app.services import append_store, r2_backfill  # noqa: E402

run = asyncio.run

DS_ID = uuid.UUID("fd06f5ae-8a4f-4120-b275-8a514ad23499")
MERGED = "append_taxes_nadlan_full_f41fb496_fd06f5ae"
PART_A = f"{MERGED}_be0d9113"
PART_B = f"{MERGED}_04cfba72"
# Same shape, a DIFFERENT dataset's octet.
OTHER_DATASET = "append_taxes_nadlan_full_f41fb496_aaaaaaaa_04cfba72"
UNRELATED = "append_private_and_commercial_vehicles_e437ab0b"


class _DS:
    id = DS_ID
    ckan_name = "taxes-nadlan-full-f41fb496"
    resource_id = None
    scraper_config = {"kind": "taxes_nadlan_full"}


class _V:
    def __init__(self, mappings):
        self.resource_mappings = mappings


class _DB:
    def __init__(self, versions):
        self.versions = versions

    async def execute(self, stmt):
        text = str(stmt)
        vs = self.versions

        class _R:
            def scalar_one_or_none(self_inner):
                return _DS() if "tracked_datasets" in text else None

            def scalars(self_inner):
                return types.SimpleNamespace(all=lambda: vs)
        return _R()


def _patch(monkeypatch, present, dropped):
    async def _cols(schema):
        return {t: [] for t in present}

    async def _drop(table):
        dropped.append(table)

    monkeypatch.setattr(append_store, "schema_table_columns", _cols)
    monkeypatch.setattr(append_store, "drop_table", _drop)
    monkeypatch.setattr(append_store, "is_configured", lambda: True)
    monkeypatch.setattr(r2_backfill, "invalidate_catalog_cache", lambda: None,
                        raising=False)


PRESENT = [MERGED, PART_A, PART_B, OTHER_DATASET, UNRELATED]


def test_a_retargeted_dataset_drops_the_tables_it_emptied(monkeypatch):
    """Versions all name the merged table, so the partitions are unreferenced."""
    dropped = []
    _patch(monkeypatch, PRESENT, dropped)
    db = _DB([_V({"append_table": MERGED})])
    out = run(r2_backfill.purge_orphan_append_tables(db, DS_ID, apply=True))
    assert sorted(out["orphans"]) == sorted([PART_A, PART_B])
    assert sorted(dropped) == sorted([PART_A, PART_B])


def test_nothing_is_dropped_while_a_version_still_names_it(monkeypatch):
    """Inert on a half-finished migration: if the reseed has not retargeted the
    versions, the tables are still the ones being read and must stay."""
    dropped = []
    _patch(monkeypatch, PRESENT, dropped)
    db = _DB([_V({"_append_tables": [
        {"resource": "a", "table": PART_A},
        {"resource": "b", "table": PART_B},
    ]})])
    out = run(r2_backfill.purge_orphan_append_tables(db, DS_ID, apply=True))
    assert out["orphans"] == []
    assert dropped == []


def test_the_merged_table_is_never_a_candidate(monkeypatch):
    """Even with no versions at all, the table holding the rows is not touched."""
    dropped = []
    _patch(monkeypatch, PRESENT, dropped)
    out = run(r2_backfill.purge_orphan_append_tables(_DB([]), DS_ID, apply=True))
    assert MERGED not in out["orphans"] and MERGED not in dropped


def test_another_datasets_table_is_not_a_candidate(monkeypatch):
    """The name shape carries THIS dataset's id octet, so a table of the same
    source belonging to a different dataset cannot match."""
    dropped = []
    _patch(monkeypatch, PRESENT, dropped)
    out = run(r2_backfill.purge_orphan_append_tables(
        _DB([_V({"append_table": MERGED})]), DS_ID, apply=True))
    assert OTHER_DATASET not in out["orphans"]
    assert UNRELATED not in out["orphans"]
    assert OTHER_DATASET not in dropped and UNRELATED not in dropped


def test_a_dry_run_drops_nothing_but_reports_the_same_plan(monkeypatch):
    dropped = []
    _patch(monkeypatch, PRESENT, dropped)
    db = _DB([_V({"append_table": MERGED})])
    out = run(r2_backfill.purge_orphan_append_tables(db, DS_ID, apply=False))
    assert sorted(out["orphans"]) == sorted([PART_A, PART_B])
    assert out["dropped"] == [] and dropped == []
