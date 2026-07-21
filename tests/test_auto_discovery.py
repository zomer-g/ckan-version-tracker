"""Tests for auto-discovery candidate evaluation (app/services/auto_discovery.py).

Locks the two selection invariants that keep random onboarding safe:
  1. A candidate with no datastore-backed resource is rejected (NEON needs rows).
  2. A candidate whose largest resource exceeds the row cap is rejected whole
     (size guard — never onboard a multi-million-row registry by chance).
Only datastore-active resources with a real schema are chosen.
"""
import asyncio
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from app.config import settings  # noqa: E402
from app.services import auto_discovery as ad  # noqa: E402


def _install_fakes(monkey_pkg: dict, totals: dict[str, int]):
    """Patch the shared ckan_client with in-memory package_show/datastore_info."""
    async def fake_package_show(name):
        return monkey_pkg

    async def fake_datastore_info(rid):
        # A resource we know a total for is datastore-backed (has fields);
        # anything else raises like the real API does for non-datastore ids.
        if rid not in totals:
            raise RuntimeError("not a datastore resource")
        return {"total": totals[rid], "fields": [{"id": "col", "type": "text"}]}

    ad.ckan_client.package_show = fake_package_show
    ad.ckan_client.datastore_info = fake_datastore_info


def test_selects_only_datastore_resources_under_cap():
    settings.auto_discover_max_rows = 2_000_000
    pkg = {
        "id": "pkg-1", "name": "some-dataset",
        "organization": {"name": "some-org"},
        "resources": [
            {"id": "ds-ok", "datastore_active": True},
            {"id": "file-only", "datastore_active": False},   # skipped: not datastore
            {"id": "ds-ok-2", "datastore_active": True},
        ],
    }
    _install_fakes(pkg, {"ds-ok": 1000, "ds-ok-2": 5000})
    info = asyncio.run(ad._evaluate_candidate("some-dataset"))
    assert info is not None
    assert info["resource_ids"] == ["ds-ok", "ds-ok-2"]


def test_rejects_oversized_dataset():
    settings.auto_discover_max_rows = 2_000_000
    pkg = {
        "id": "pkg-2", "name": "huge-registry",
        "resources": [{"id": "ds-huge", "datastore_active": True}],
    }
    _install_fakes(pkg, {"ds-huge": 5_000_000})
    assert asyncio.run(ad._evaluate_candidate("huge-registry")) is None


def test_rejects_dataset_with_no_datastore_resources():
    pkg = {
        "id": "pkg-3", "name": "files-only",
        "resources": [
            {"id": "pdf-1", "datastore_active": False},
            {"id": "pdf-2", "datastore_active": False},
        ],
    }
    _install_fakes(pkg, {})
    assert asyncio.run(ad._evaluate_candidate("files-only")) is None
