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


# Packages tracked whatever their age, per organization. An org listed here but
# not in ``catalog_watch_ckan_orgs`` is walked for these packages only.
#
# ministry_of_transport: every data.gov.il package the חצב catalog
# (geo.mot.gov.il) links a layer to, read from the live catalog on 2026-09-27.
# The catalog itself is tracked as a scraper dataset with links only, and until
# then 64 of these 88 layers had no dataset of their own; most were last edited
# in 2023-2025, so a date cutoff would have kept them out.
PINNED_PACKAGES: dict[str, frozenset[str]] = {
    "ministry_of_transport": frozenset({
        "accid_taz", "accidents_municipal", "agg_charge_stations", "airport",
        "airstrip", "area_authority", "ashdod_metro_bus", "bike_net_center",
        "bike_net_tlv", "bph_infra_exist", "bph_infra_plan", "brt_line",
        "bus_speed", "bus_terminal_fcl", "bus_terminal_strat", "cong_tax_rings",
        "cvfr", "depo_lrt", "depo_metro", "fast_lane_gate", "fast_lanes",
        "functional_areas", "helipad", "hov_2024", "hoze_zhut", "ims_stat",
        "kishuriyut_metronit", "kishuriyut_nofit", "kishuriyut_prj",
        "levelseparation", "lrt_line", "lrt_stat", "lrt_tunnels",
        "mahirlair_city", "mataan_ashkelon", "mataan_ashkelon_app",
        "mataan_brshva", "mataan_haifa", "mataan_hdr_ntn", "mataan_jlm_2040",
        "mataan_jlm_2050", "mataan_ntn_app", "mataan_tlv", "mataan_tlv_2040",
        "metro_line", "metro_line_jlm", "metro_stat", "metrofan", "metronit",
        "mile_post", "miutim_poi", "miutim_prj", "miutim_risk", "nataz_kayam",
        "nataz_planned", "nti_zhut_dereh", "ofneidan", "park_n_ride",
        "rail_flow_2040", "rail_freq_2026", "rail_freq_2040", "rail_stat",
        "rail_strat_station", "rail_strategic", "rail_tunnels",
        "road_strat_2030", "road_strat_2050", "roadauthority", "roadnumber_tma",
        "roadnumbers", "roundabout", "sfirot", "speed_survey_2022",
        "strat_border", "tma_23", "tma_23a4_dipo", "tma_23a4_lines", "tma_3",
        "tma_3_plans", "tma_3_points", "tma_42", "traffic_light_junction",
        "ttl_transport", "urban_intersections", "urban_road_safety_elements",
        "vor", "wplan_muni", "zmt_names",
    }),
}


# The tag every pinned package of an org carries, as (name, description). The
# watch adds it daily, so a layer pinned later is tagged on the day it lands.
PINNED_TAGS: dict[str, tuple[str, str]] = {
    "ministry_of_transport": (
        "חצב",
        'שכבות מערכת חצב של משרד התחבורה (geo.mot.gov.il), כפי שהן מפורסמות ב-data.gov.il',
    ),
}


async def _ensure_tag(db, name: str, description: str, dataset_ids: list) -> int:
    """Find or create the tag ``name`` and attach it to every dataset in
    ``dataset_ids`` that does not carry it yet. Returns how many it attached."""
    from sqlalchemy import func
    from sqlalchemy.dialects.postgresql import insert

    from app.models.tag import Tag, dataset_tags

    if not dataset_ids:
        return 0
    tag = (await db.execute(
        select(Tag).where(func.lower(Tag.name) == name.lower())
    )).scalars().first()
    if tag is None:
        tag = Tag(name=name, description=description)
        db.add(tag)
        await db.flush()
    result = await db.execute(
        insert(dataset_tags)
        .values([{"dataset_id": i, "tag_id": tag.id} for i in dataset_ids])
        .on_conflict_do_nothing()
    )
    return result.rowcount or 0


async def _unstamp_empty_deliveries(db, rows) -> int:
    """Clear the fetched stamp on every blocked file that arrived empty."""
    from app.services import blocked_resources

    return await blocked_resources.unstamp_empty_deliveries(db, rows)


def watched_orgs() -> list[str]:
    return [o.strip() for o in (settings.catalog_watch_ckan_orgs or "").split(",") if o.strip()]


def orgs_to_walk() -> list[tuple[str, frozenset[str] | None]]:
    """``(org, only)`` pairs: ``only`` is None for a fully watched org, else
    the pinned package names that are all the watch looks at there."""
    whole = watched_orgs()
    return [(o, None) for o in whole] + [
        (o, names) for o, names in PINNED_PACKAGES.items() if o not in whole]


def _tracked_ids(row: TrackedDataset) -> set[str]:
    ids = set(row.resource_ids or [])
    if row.resource_id:
        ids.add(row.resource_id)
    return ids


def plan_org(packages: list[dict], rows: list[TrackedDataset], since: str,
             pinned: frozenset[str] = frozenset()) -> dict:
    """Decide, without touching anything, what the watch does for one org.

    Returns ``{"onboard": [pkg, ...], "extend": [(row, [rid, ...]), ...],
    "skipped": [(name, reason), ...]}``.

    * A package with no tracked row at all is onboarded — if it was touched on
      or after ``since``, or is ``pinned``. A row in ANY status counts as
      tracked: a package an admin rejected stays rejected.
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
            elif pkg["name"] in pinned or (pkg.get("metadata_modified") or "")[:10] >= since:
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


async def _watch_ckan_org(org: str, only: frozenset[str] | None = None) -> dict:
    from app.api.datasets import apply_storage_target, default_storage_target
    from app.worker.poll_job import poll_dataset
    from app.worker.scheduler import add_poll_job

    packages = await ckan_client.organization_packages(org)
    if only is not None:
        packages = [p for p in packages if p["name"] in only]
    ids = [p["id"] for p in packages]
    names = [p["name"] for p in packages]
    async with async_session() as db:
        rows = (await db.execute(
            select(TrackedDataset).where(
                (TrackedDataset.ckan_id.in_(ids)) | (TrackedDataset.ckan_name.in_(names))
            )
        )).scalars().all()
        plan = plan_org(packages, rows, settings.catalog_watch_ckan_since,
                        PINNED_PACKAGES.get(org, frozenset()))

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
        # A file a worker "delivered" as 0 bytes is not archived: make it
        # pending again so the re-poll below asks for it once more.
        unstamped = await _unstamp_empty_deliveries(db, rows)
        # Missing blocked files are NOT re-polled here: polling a whole org at
        # once is the burst data.gov.il answers with empty files (2026-10-01,
        # ministry_of_transport: ~30 tasks in a minute, nearly all empty). They
        # go through the paced _retry_blocked_everywhere instead. A plain 403
        # with nothing assessed yet still polls now — that poll is what turns
        # the refusal into a worker task.
        refused = []
        for r in rows:
            if (r.status == "active" and r.id not in extended_ids
                    and "403 Forbidden" in (r.last_error or "")
                    and not blocked_resources.pending(blocked_resources.stored(r))):
                r.last_modified = None  # else the unchanged-metadata shortcut skips it
                refused.append(str(r.id))
        tagged = 0
        if org in PINNED_TAGS:
            await db.flush()  # the new rows' ids
            pinned = PINNED_PACKAGES.get(org, frozenset())
            pinned_ids = {p["id"] for p in packages if p["name"] in pinned}
            tagged = await _ensure_tag(db, *PINNED_TAGS[org], [
                r.id for r in list(rows) + created
                if r.ckan_name in pinned or r.ckan_id in pinned_ids])
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
        "empty_files_requeued": unstamped,
        "tagged": tagged,
        "skipped": [{"name": n, "reason": why} for n, why in plan["skipped"]
                    if not why.startswith("untouched")],
    }


async def _retry_blocked_everywhere(skip_ids: set[str]) -> dict:
    """Re-poll tracked CKAN datasets whose blocked files are still missing.

    The org walk above retries only the packages it watches, so a file that a
    worker failed to get anywhere else waited for its dataset's own cadence —
    90 days for most. The poll is what queues the worker task, so polling is
    the whole retry. Paced and capped: data.gov.il turns a burst into empty
    files, and a run that fails everything teaches nothing.
    """
    import asyncio

    from app.models.scrape_task import ScrapeTask
    from app.services import blocked_resources
    from app.worker.poll_job import poll_dataset

    limit = max(0, int(settings.catalog_watch_blocked_retry_limit))
    if not limit:
        return {"repolled": [], "waiting": 0, "empty_files_requeued": 0}
    async with async_session() as db:
        rows = (await db.execute(
            select(TrackedDataset).where(
                TrackedDataset.source_type == "ckan",
                TrackedDataset.status == "active",
                TrackedDataset.is_active.is_(True),
            )
        )).scalars().all()
        rows = [r for r in rows if blocked_resources.stored(r) and str(r.id) not in skip_ids]
        busy = set((await db.execute(
            select(ScrapeTask.tracked_dataset_id).where(
                ScrapeTask.status.in_(["pending", "running"]))
        )).scalars().all())
        unstamped = await blocked_resources.unstamp_empty_deliveries(db, rows)
        due = [r for r in rows
               if r.id not in busy and blocked_resources.pending(blocked_resources.stored(r))]
        due.sort(key=lambda r: r.last_polled_at.timestamp() if r.last_polled_at else 0.0)
        chosen = due[:limit]
        for r in chosen:
            r.last_modified = None  # else the unchanged-metadata shortcut skips it
        await db.commit()
        chosen_ids = [(str(r.id), r.ckan_name) for r in chosen]

    for i, (ds_id, name) in enumerate(chosen_ids):
        if i:
            await asyncio.sleep(settings.catalog_watch_blocked_retry_gap_s)
        try:
            await poll_dataset(ds_id)
        except Exception:  # noqa: BLE001 — one dataset must not stop the rest
            logger.exception("catalog watch: blocked-file retry of %s failed", name)
    return {"repolled": [n for _, n in chosen_ids], "waiting": len(due) - len(chosen_ids),
            "empty_files_requeued": unstamped}


_retry_task = None
last_retry_result: dict | None = None


async def _run_blocked_retry(skip_ids: set[str]) -> None:
    global last_retry_result
    try:
        last_retry_result = await _retry_blocked_everywhere(skip_ids)
        logger.info("catalog watch: blocked-file retry %s", last_retry_result)
    except Exception as e:  # noqa: BLE001
        logger.exception("catalog watch: blocked-file retry failed")
        last_retry_result = {"error": str(e)[:300]}


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
    for org, only in orgs_to_walk():
        try:
            out["ckan"].append(await _watch_ckan_org(org, only))
        except Exception as e:  # noqa: BLE001
            logger.exception("catalog watch: data.gov.il org %s failed", org)
            out["ckan"].append({"org": org, "error": str(e)[:300]})

    # Paced over up to an hour, so it runs beside the pass rather than inside
    # it — the admin "run now" button awaits this function.
    global _retry_task
    done = {i for c in out["ckan"] for i in c.get("repolled_blocked") or []}
    if _retry_task is None or _retry_task.done():
        import asyncio

        _retry_task = asyncio.create_task(_run_blocked_retry(done))
        out["blocked_retry"] = "started"
    else:
        out["blocked_retry"] = "already running"

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
