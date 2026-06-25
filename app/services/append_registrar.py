"""Register the APPEND-set data.gov.il datasets as tracked append_only sources.

Shared by the CLI (scripts/register_append_datasets.py) and the admin endpoint
(POST /api/admin/register-append-datasets) so production can trigger it from the
authenticated UI without shell access — the same server-side-via-admin-endpoint
pattern used for the R2 backfill.

Two datasets, both archived as a growing APPEND log (never a full re-snapshot),
both reading via the datastore API (delta_archiver) and stamping each appended
row with a ``first_seen`` timestamp:

  flydata                          live flight board (~2k rows), refreshes every
                                   15 min, rows mutate through their lifecycle.
                                   KEYLESS (full-row hash → every state captured)
                                   + WINDOWED seen-set so the 15-min cadence can't
                                   grow bookkeeping without bound. Its file
                                   download is IAP-blocked → datastore API only.

  private-and-commercial-vehicles  ~4.1M rows, slow cadence. KEYED by
                                   mispar_rechev (only new plates appended),
                                   unbounded seen-set.
"""
import logging

from sqlalchemy import select

from app.models.tracked_dataset import TrackedDataset
from app.services.ckan_client import ckan_client

logger = logging.getLogger(__name__)


# Each spec mirrors what POST /datasets would build, pinned to the exact
# resource + append config decided for the APPEND set.
APPEND_DATASET_SPECS = [
    {
        "ckan_id": "flydata",
        "resource_id": "e83f763b-b7d7-479e-b172-ae981ddc6de5",
        "poll_interval": 900,  # 15 min — matches the source refresh
        "scraper_config": {
            # No append_key → keyless full-row-hash dedup (capture every state).
            # ~192 versions ≈ 2 days at 15-min cadence. Safe because a flight's
            # exact (date-stamped) row never recurs once off the board; the
            # window only guards against transient source flakiness.
            "seen_window_versions": 192,
        },
    },
    {
        "ckan_id": "private-and-commercial-vehicles",
        "resource_id": "053cea08-09bc-40ec-8f7a-156f0677aff3",
        "poll_interval": 86400,  # daily — the registry changes slowly
        "scraper_config": {
            "append_key": "mispar_rechev",  # stable per-vehicle identity
            # No window: slow cadence → unbounded seen-set is fine.
        },
    },
]


async def _register_one(db, spec: dict) -> dict:
    ckan_id = spec["ckan_id"]
    resource_id = spec["resource_id"]

    existing = (
        await db.execute(
            select(TrackedDataset).where(TrackedDataset.ckan_id == ckan_id)
        )
    ).scalar_one_or_none()
    if existing:
        return {"ckan_id": ckan_id, "status": "skipped",
                "detail": "already tracked", "id": str(existing.id)}

    # Resolve title / org / resource name from the live source, and assert the
    # resource is datastore-active (the streaming append path depends on it).
    pkg = await ckan_client.package_show(ckan_id)
    res = next((r for r in pkg.get("resources", []) if r["id"] == resource_id), None)
    if res is None:
        return {"ckan_id": ckan_id, "status": "error",
                "detail": f"resource {resource_id} not found at source"}
    if not res.get("datastore_active"):
        return {"ckan_id": ckan_id, "status": "error",
                "detail": f"resource {resource_id} is not datastore_active"}

    res_name = res.get("name") or res.get("description") or resource_id
    title = pkg.get("title", pkg.get("name", ckan_id))
    org = (pkg.get("organization") or {}).get("name", "") or None

    ds = TrackedDataset(
        ckan_id=ckan_id,
        ckan_name=pkg.get("name", ckan_id),
        resource_id=resource_id,
        resource_ids=[resource_id],
        title=f"{title} — {res_name}",
        organization=org,
        odata_dataset_id=None,  # delta_archiver lazily creates the mirror
        poll_interval=spec["poll_interval"],
        status="active",
        storage_mode="append_only",
        scraper_config=spec["scraper_config"],
        last_modified=None,  # None → first poll always creates version 1
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    logger.info("append_registrar: tracked %s (id=%s, resource=%s)",
                ckan_id, ds.id, res_name)
    return {"ckan_id": ckan_id, "status": "created",
            "detail": f"resource={res_name}", "id": str(ds.id),
            "poll_interval": ds.poll_interval}


async def register_append_datasets(db) -> list[dict]:
    """Idempotently register every spec. One failing spec doesn't abort the
    others. Returns a per-dataset result list ({ckan_id, status, detail, id})."""
    results: list[dict] = []
    for spec in APPEND_DATASET_SPECS:
        try:
            results.append(await _register_one(db, spec))
        except Exception as e:  # noqa: BLE001
            await db.rollback()
            logger.exception("append_registrar: failed for %s", spec["ckan_id"])
            results.append({"ckan_id": spec["ckan_id"], "status": "error",
                            "detail": f"{type(e).__name__}: {e}"})
    return results
