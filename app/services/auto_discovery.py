"""Auto-discovery of new data.gov.il datasets.

Maps the FULL data.gov.il CKAN catalog, diffs it against the CKAN datasets
already tracked in OVER, and onboards ONE random untracked dataset per run.
Driven by a 6-hourly scheduler job (see app/worker/scheduler.py), gated behind
``settings.auto_discover_enabled``.

Design choices (see the config block in app/config.py):
  • NEON-only archive — onboarded datasets stream their datastore rows into
    the NEON append DB (SQL-queryable in the /data console); no file/ODATA
    mirror is created, so the marginal cost per dataset is just the rows.
    ``scraper_config = {"storage_backend": "neon"}`` is what routes the poll
    to the NEON streaming path (see storage_client.dataset_archives_neon).
  • Size guard — a candidate whose largest datastore resource exceeds
    ``auto_discover_max_rows`` is skipped and another is drawn, so a random
    pick never lands on a multi-million-row registry and OOMs the dyno.
  • Datastore-backed only — a candidate is onboarded only if it has at least
    one ``datastore_active`` resource (NEON needs tabular rows to archive).
"""

import logging
import random

from sqlalchemy import select

from app.config import settings
from app.database import async_session
from app.models.organization import Organization
from app.models.tracked_dataset import TrackedDataset
from app.services.ckan_client import ckan_client

logger = logging.getLogger(__name__)

# data.gov.il's package_list is not always returned whole; page it so we never
# silently truncate the catalog map.
_PAGE = 1000


async def _all_ckan_names() -> list[str]:
    """Every dataset slug (``name``) in the data.gov.il CKAN catalog."""
    names: list[str] = []
    offset = 0
    while True:
        page = await ckan_client.package_list(limit=_PAGE, offset=offset)
        if not page:
            break
        names.extend(page)
        if len(page) < _PAGE:
            break
        offset += _PAGE
    return names


async def _tracked_ckan_names(db) -> set[str]:
    """Slugs of the CKAN (data.gov.il) datasets already tracked here.

    Only ``source_type == 'ckan'`` rows are real data.gov.il datasets; scraper/
    govmap rows carry synthetic slugs that never collide with a catalog name.
    """
    result = await db.execute(
        select(TrackedDataset.ckan_name).where(
            TrackedDataset.source_type == "ckan",
        )
    )
    return {row[0] for row in result.all() if row[0]}


async def _evaluate_candidate(name: str) -> dict | None:
    """Return onboarding info for ``name`` if it's a suitable candidate, else None.

    Suitable = has ≥1 datastore-backed resource and no single resource exceeds
    the row cap. Returns ``{pkg, resource_ids}`` on success.
    """
    try:
        pkg = await ckan_client.package_show(name)
    except Exception as e:  # noqa: BLE001 — a bad/removed slug shouldn't abort the run
        logger.debug("auto-discover: package_show(%s) failed: %s", name, e)
        return None

    chosen: list[str] = []
    for r in pkg.get("resources", []):
        rid = r.get("id")
        if not rid or not r.get("datastore_active"):
            continue
        try:
            info = await ckan_client.datastore_info(rid)
        except Exception:  # noqa: BLE001 — probe failure ⇒ treat resource as unusable
            continue
        if info.get("total", 0) > settings.auto_discover_max_rows:
            logger.info(
                "auto-discover: skipping %s — resource %s has %d rows (> cap %d)",
                name, rid, info.get("total", 0), settings.auto_discover_max_rows,
            )
            return None  # oversized: pass over the whole dataset
        # datastore-active with a real schema — archivable to NEON
        if info.get("fields"):
            chosen.append(rid)

    if not chosen:
        return None
    return {"pkg": pkg, "resource_ids": chosen}


async def discover_and_onboard_one(force: bool = False) -> dict | None:
    """One discovery pass: map the catalog, then onboard one random untracked
    datastore-backed dataset as a NEON-archived tracked dataset.

    Returns a small summary dict of the onboarded dataset, or None when nothing
    was onboarded (catalog fully covered, or no suitable candidate this run).
    Safe to run concurrently-guarded (max_instances=1 on the scheduler job).

    ``force=True`` bypasses the ``auto_discover_enabled`` gate — used by the
    admin manual-trigger endpoint so a pass can be run even while the scheduled
    job is switched off.
    """
    if not force and not settings.auto_discover_enabled:
        return None

    try:
        all_names = await _all_ckan_names()
    except Exception:  # noqa: BLE001
        logger.exception("auto-discover: failed to fetch data.gov.il catalog")
        return None

    async with async_session() as db:
        tracked = await _tracked_ckan_names(db)

    untracked = [n for n in all_names if n not in tracked]
    logger.info(
        "auto-discover: catalog map — %d datasets total, %d tracked, %d untracked",
        len(all_names), len(all_names) - len(untracked), len(untracked),
    )
    if not untracked:
        logger.info("auto-discover: full catalog coverage reached — nothing to onboard")
        return None

    random.shuffle(untracked)
    attempts = 0
    for name in untracked:
        if attempts >= settings.auto_discover_max_attempts:
            logger.info(
                "auto-discover: no suitable candidate in %d attempts this run",
                attempts,
            )
            return None
        attempts += 1
        info = await _evaluate_candidate(name)
        if info:
            return await _onboard(info["pkg"], info["resource_ids"])
    return None


async def _onboard(pkg: dict, resource_ids: list[str]) -> dict | None:
    """Create an active, NEON-archived TrackedDataset for ``pkg`` and schedule it."""
    ckan_id = pkg["id"]
    ckan_name = pkg["name"]
    org_name = (pkg.get("organization") or {}).get("name", "") or ""
    title = pkg.get("title") or ckan_name

    async with async_session() as db:
        # Race/stale-set guard: another path may have added it since the map.
        exists = await db.execute(
            select(TrackedDataset.id).where(TrackedDataset.ckan_id == ckan_id)
        )
        if exists.first():
            logger.info("auto-discover: %s already tracked (race) — skipping", ckan_name)
            return None

        org_id = None
        if org_name:
            org_row = (await db.execute(
                select(Organization).where(Organization.name == org_name)
            )).scalar_one_or_none()
            if org_row:
                org_id = org_row.id

        ds = TrackedDataset(
            ckan_id=ckan_id,
            ckan_name=ckan_name,
            resource_ids=resource_ids,
            title=title,
            organization=org_name,
            organization_id=org_id,
            odata_dataset_id=None,  # NEON-only: no file/ODATA mirror
            poll_interval=settings.auto_discover_poll_interval,
            status="active",
            is_active=True,
            storage_mode="full_snapshot",
            scraper_config={"storage_backend": "neon", "auto_discovered": True},
            created_by=None,  # system-onboarded (no requesting user)
            last_modified=None,  # first poll always creates version 1
        )
        db.add(ds)
        await db.commit()
        await db.refresh(ds)
        dataset_id = str(ds.id)
        interval = ds.poll_interval

    # Register the recurring poll job; with last_polled_at=None it fires the
    # first poll on the next scheduler tick and then repeats on the interval.
    from app.worker.scheduler import add_poll_job
    add_poll_job(dataset_id, interval)

    logger.info(
        "auto-discover: onboarded %s (%s) — %d datastore resource(s), NEON archive",
        ckan_name, org_name or "?", len(resource_ids),
    )
    return {
        "id": dataset_id,
        "ckan_id": ckan_id,
        "ckan_name": ckan_name,
        "title": title,
        "organization": org_name,
        "resource_ids": resource_ids,
    }


async def coverage_summary() -> dict:
    """Live catalog-coverage map (no persistence): total / tracked / untracked
    counts plus a small sample of untracked slugs. Used by the admin endpoint."""
    all_names = await _all_ckan_names()
    async with async_session() as db:
        tracked = await _tracked_ckan_names(db)
    untracked = [n for n in all_names if n not in tracked]
    return {
        "total": len(all_names),
        "tracked": len(all_names) - len(untracked),
        "untracked": len(untracked),
        "enabled": settings.auto_discover_enabled,
        "interval_hours": settings.auto_discover_interval_hours,
        "sample_untracked": untracked[:50],
    }
