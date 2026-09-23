"""Daily watch for layers and datasets nobody added.

Two catalogs, one job:

  * GovMap — re-read the layer catalog (``govmap_coverage.populate_from_catalog``).
    A new layer lands in the coverage inventory with ``last_triggered_at``
    NULL, and the 3-minute rollout tick scrapes never-triggered layers first,
    so a layer published today is captured today. A layer GovMap republished
    since our last scrape is put back at the head of the queue too.
  * data.gov.il — for each watched organization, every package touched since
    ``catalog_watch_ckan_since`` that no one tracks is onboarded whole, and a
    tracked package that grew a resource gets that resource added.

Why this exists: until 2026-09-23 the GovMap catalog was read only when an
admin pressed "populate", so 83 catalog layers were outside the inventory —
among them the new national cadastre (בנק"ל) that MAPI switched to that
morning. The data.gov.il auto-discovery (auto_discovery.py) could not have
caught the cadastre either: it onboards one RANDOM datastore-backed dataset
per run, and the cadastre is shapefile ZIPs with no datastore at all.
"""
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from app.config import settings
from app.database import async_session
from app.models.organization import Organization
from app.models.tracked_dataset import TrackedDataset
from app.services.ckan_client import ckan_client

logger = logging.getLogger(__name__)

# The newest run's summary, for the admin endpoint. In-process only: the log
# line is the durable record, and a restart re-runs the watch within minutes.
last_result: dict | None = None


def watched_orgs() -> list[str]:
    return [o.strip() for o in (settings.catalog_watch_ckan_orgs or "").split(",") if o.strip()]


def _tracked_ids(row: TrackedDataset) -> set[str]:
    ids = set(row.resource_ids or [])
    if row.resource_id:
        ids.add(row.resource_id)
    return ids


def plan_org(packages: list[dict], rows: list[TrackedDataset], since: str) -> dict:
    """Decide, without touching anything, what the watch does for one org.

    Returns ``{"onboard": [pkg, ...], "extend": [(row, [rid, ...]), ...],
    "skipped": [(name, reason), ...]}``.

    * A package with no tracked row at all is onboarded — if it was touched on
      or after ``since``. A row in ANY status counts as tracked: a package an
      admin rejected stays rejected.
    * A package with an active "whole package" row (``resource_ids`` set, no
      single ``resource_id``) gets every source resource none of its active
      rows track. Split-mode packages (one row per resource) are reported, not
      guessed at.
    """
    by_pkg: dict[str, list[TrackedDataset]] = {}
    for r in rows:
        for key in (r.ckan_id, r.ckan_name):
            if key:
                by_pkg.setdefault(key, []).append(r)

    onboard: list[dict] = []
    extend: list[tuple[TrackedDataset, list[str]]] = []
    skipped: list[tuple[str, str]] = []
    for pkg in packages:
        mine = {id(r): r for r in by_pkg.get(pkg["id"], []) + by_pkg.get(pkg["name"], [])}
        mine = list(mine.values())
        source_ids = [r["id"] for r in pkg.get("resources", []) if r.get("id")]
        if not mine:
            if not source_ids:
                skipped.append((pkg["name"], "no resources"))
            elif (pkg.get("metadata_modified") or "")[:10] >= since:
                onboard.append(pkg)
            else:
                skipped.append((pkg["name"], "untouched since " + since))
            continue
        active = [r for r in mine if r.status == "active"]
        if not active:
            continue  # rejected / hidden / pending — someone already decided
        if any(not r.resource_ids and not r.resource_id for r in active):
            continue  # a legacy row with no subset tracks every resource
        tracked = set().union(*(_tracked_ids(r) for r in active))
        missing = [rid for rid in source_ids if rid not in tracked]
        if not missing:
            continue
        whole = [r for r in active if r.resource_ids and not r.resource_id]
        if not whole:
            skipped.append((pkg["name"], f"{len(missing)} new resource(s) on a split-mode package"))
            continue
        extend.append((whole[0], missing))
    return {"onboard": onboard, "extend": extend, "skipped": skipped}


async def _watch_ckan_org(org: str) -> dict:
    from app.api.datasets import apply_storage_target, default_storage_target
    from app.worker.poll_job import poll_dataset
    from app.worker.scheduler import add_poll_job

    packages = await ckan_client.organization_packages(org)
    ids = [p["id"] for p in packages]
    names = [p["name"] for p in packages]
    async with async_session() as db:
        rows = (await db.execute(
            select(TrackedDataset).where(
                (TrackedDataset.ckan_id.in_(ids)) | (TrackedDataset.ckan_name.in_(names))
            )
        )).scalars().all()
        plan = plan_org(packages, rows, settings.catalog_watch_ckan_since)

        org_id = (await db.execute(
            select(Organization.id).where(Organization.name == org)
        )).scalar_one_or_none()

        created: list[TrackedDataset] = []
        for pkg in plan["onboard"]:
            ds = TrackedDataset(
                ckan_id=pkg["id"],
                ckan_name=pkg["name"],
                resource_ids=[r["id"] for r in pkg["resources"] if r.get("id")],
                title=pkg.get("title") or pkg["name"],
                organization=org,
                organization_id=org_id,
                odata_dataset_id=None,
                poll_interval=settings.catalog_watch_ckan_poll_interval,
                status="active",
                is_active=True,
                storage_mode="full_snapshot",
                scraper_config=apply_storage_target(
                    {"catalog_watch": True},
                    default_storage_target("ckan", None, True),
                ),
                created_by=None,
                last_modified=None,  # first poll always creates version 1
            )
            db.add(ds)
            created.append(ds)

        extended: list[tuple[str, str, list[str]]] = []
        for row, missing in plan["extend"]:
            row.resource_ids = list(row.resource_ids or []) + missing
            row.new_resources_at_source = None
            # An unchanged metadata_modified would otherwise skip the poll that
            # is supposed to fetch the new resource.
            row.last_modified = None
            row.last_error = None
            extended.append((str(row.id), row.ckan_name, missing))
        # A watched dataset whose last poll was refused (data.gov.il's 403 to a
        # server IP) is polled again now rather than at its weekly/monthly
        # cadence: the poll is what routes the files to a worker on a home
        # connection, and once it has, last_error is clear and this stops.
        extended_ids = {row.id for row, _ in plan["extend"]}
        # Likewise a dataset whose blocked files are still missing: the poll
        # re-queues the worker task, so a task that failed (a worker without a
        # browser, an allowlist gone stale) is retried daily, not monthly.
        from app.services import blocked_resources
        refused = []
        for r in rows:
            if (r.status == "active" and r.id not in extended_ids
                    and ("403 Forbidden" in (r.last_error or "")
                         or blocked_resources.pending(blocked_resources.stored(r)))):
                r.last_modified = None  # else the unchanged-metadata shortcut skips it
                refused.append(str(r.id))
        await db.commit()
        onboarded = [(str(d.id), d.ckan_name, d.title, d.poll_interval) for d in created]

    for ds_id, _name, _title, interval in onboarded:
        add_poll_job(ds_id, interval)
    for ds_id, _name, _missing in extended:
        await poll_dataset(ds_id)
    for ds_id in refused:
        await poll_dataset(ds_id)

    return {
        "org": org,
        "packages": len(packages),
        "onboarded": [{"id": i, "name": n, "title": t} for i, n, t, _ in onboarded],
        "resources_added": [{"id": i, "name": n, "resource_ids": m} for i, n, m in extended],
        "repolled_blocked": refused,
        "skipped": [{"name": n, "reason": why} for n, why in plan["skipped"]
                    if not why.startswith("untouched")],
    }


async def run_catalog_watch(force: bool = False) -> dict | None:
    """One pass over both catalogs. Each half fails on its own: GovMap being
    down must not stop the data.gov.il half, nor the other way round."""
    global last_result
    if not force and not settings.catalog_watch_enabled:
        return None
    started = datetime.now(timezone.utc)
    out: dict = {"started_at": started.isoformat()}

    from app.services.govmap_coverage import populate_from_catalog
    try:
        async with async_session() as db:
            out["govmap"] = await populate_from_catalog(db)
    except Exception as e:  # noqa: BLE001
        logger.exception("catalog watch: GovMap catalog refresh failed")
        out["govmap"] = {"error": str(e)[:300]}

    out["ckan"] = []
    for org in watched_orgs():
        try:
            out["ckan"].append(await _watch_ckan_org(org))
        except Exception as e:  # noqa: BLE001
            logger.exception("catalog watch: data.gov.il org %s failed", org)
            out["ckan"].append({"org": org, "error": str(e)[:300]})

    gm = out["govmap"]
    logger.info(
        "catalog watch: GovMap %s new / %s republished of %s; data.gov.il %s",
        len(gm.get("new_layers") or []), len(gm.get("republished") or []),
        gm.get("fetched"),
        "; ".join(
            f"{c['org']}: +{len(c.get('onboarded') or [])} datasets, "
            f"+{sum(len(x['resource_ids']) for x in c.get('resources_added') or [])} resources"
            if "error" not in c else f"{c['org']}: ERROR"
            for c in out["ckan"]
        ) or "no orgs watched",
    )
    for lay in gm.get("new_layers") or []:
        logger.info("catalog watch: new GovMap layer %s %s", lay["layer_id"], lay["caption"])
    out["finished_at"] = datetime.now(timezone.utc).isoformat()
    last_result = out
    return out
