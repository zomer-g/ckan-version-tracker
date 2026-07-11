"""Full-coverage rollout of the GovMap layer catalog, sized to the worker fleet.

Goal: eventually scrape every GovMap vector layer. The rollout is a TOP-UP
queue: a frequent scheduler tick keeps up to ``GOVMAP_COVERAGE_CONCURRENCY``
coverage scrape-tasks active at once (never-triggered layers first, then
stalest). The operator runs several OVER workers on several machines — the
old "one layer per tick, only when the (single) worker is idle" gating fed
exactly one of them and starved the rest.

Pieces:
  * ``populate_from_catalog`` — fetch GovMap's public layers catalog and upsert
    one ``GovmapCoverage`` row per vector layer.
  * ``scrape_next_layer`` — the scheduler tick: top up the active coverage
    tasks to the concurrency target, lazily creating a govmap TrackedDataset
    per layer (marked ``coverage_managed`` so the normal per-dataset scheduler
    leaves it to us).

Coverage datasets are ordinary govmap datasets once created, so their versions,
storage plan, streaming/resume scrape path, etc. are all the existing pipeline.
"""
import logging
import uuid
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import func, select

from app.config import settings
from app.database import async_session
from app.models.govmap_coverage import GovmapCoverage
from app.models.scrape_task import ScrapeTask
from app.models.tracked_dataset import TrackedDataset

logger = logging.getLogger(__name__)

CATALOG_URL = "https://www.govmap.gov.il/api/layers-catalog/catalog?lang=he"
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")


def _catalog_headers() -> dict:
    rid = uuid.uuid4().hex
    return {
        "content-type": "application/json",
        "x-fingerprint-id": rid,
        "x-user-id": rid,
        "x-trace-id": uuid.uuid4().hex,
        "Referer": "https://www.govmap.gov.il/",
        "User-Agent": _UA,
    }


def _publisher_name(item: dict, groups: dict) -> str | None:
    """Resolve a catalog item's publisher display name. ``publisherId`` indexes
    into DIFFERENT lookup tables depending on ``publicPublishType``: 1 = a
    local authority (``groups.settlements``), 0 = a government office
    (``groups.offices``). The key spaces overlap, so the type is load-bearing.
    Other types (e.g. 3) have no known lookup → None."""
    pid = str(item.get("publisherId"))
    ptype = item.get("publicPublishType")
    if ptype == 1:
        return (groups.get("settlements", {}).get(pid) or {}).get("name")
    if ptype == 0:
        return (groups.get("offices", {}).get(pid) or {}).get("name")
    return None


def _extract_layers(catalog: dict) -> list[dict]:
    """Return one dict per unique vector layer (deduped by id):
    {layer_id, caption, layer_kind, complexity}.

    GovMap's catalog reuses the same caption across MANY distinct layers —
    every municipality publishes its own "מקלטים"/"רחובות"/"גני ילדים" — so a
    bare caption makes different layers look like duplicates on the site.
    When a caption is shared by ≥2 layers, disambiguate it with the publisher
    (city / ministry): "מקלטים — עיריית צפת". If caption+publisher still
    collide (a publisher listing the same caption twice), append the layer id.
    """
    found: dict[str, dict] = {}

    items = catalog.get("catalog") if isinstance(catalog, dict) else None
    groups = catalog.get("groups") if isinstance(catalog, dict) else None
    if isinstance(items, list) and items and isinstance(groups, dict):
        # Modern flat catalog: count caption reuse, then suffix the ambiguous.
        caption_count: dict[str, int] = {}
        for it in items:
            if isinstance(it, dict) and "layerKind" in it and it.get("id") is not None:
                cap = (it.get("caption") or "").strip()
                if cap:
                    caption_count[cap] = caption_count.get(cap, 0) + 1
        seen_titles: set[str] = set()
        for it in items:
            if not (isinstance(it, dict) and "layerKind" in it and it.get("id") is not None):
                continue
            lid = str(it["id"])
            if lid in found:
                continue
            cap = (it.get("caption") or "").strip()
            title = cap or None
            if cap and caption_count.get(cap, 0) > 1:
                pub = _publisher_name(it, groups)
                title = f"{cap} — {pub}" if pub else f"{cap} (שכבה {lid})"
                if title in seen_titles:
                    title = f"{title} (שכבה {lid})"
            if title:
                seen_titles.add(title)
            found[lid] = {
                "layer_id": lid,
                "caption": (title or "")[:500] or None,
                "layer_kind": it.get("layerKind"),
                "complexity": it.get("complexity"),
            }
        if found:
            return list(found.values())

    # Fallback: generic walk for older/unknown catalog shapes.
    def walk(o):
        if isinstance(o, dict):
            if "layerKind" in o and "caption" in o and o.get("id") is not None:
                lid = str(o["id"])
                if lid not in found:
                    found[lid] = {
                        "layer_id": lid,
                        "caption": (o.get("caption") or "")[:500] or None,
                        "layer_kind": o.get("layerKind"),
                        "complexity": o.get("complexity"),
                    }
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)

    walk(catalog)
    return list(found.values())


async def populate_from_catalog(db) -> dict:
    """Fetch the GovMap catalog and upsert the coverage inventory. Idempotent:
    inserts new layers, refreshes caption/kind on existing ones, preserves
    last_triggered_at / tracked_dataset_id. Returns a small summary."""
    async with httpx.AsyncClient(timeout=90, follow_redirects=True) as client:
        r = await client.get(CATALOG_URL, headers=_catalog_headers())
        r.raise_for_status()
        catalog = r.json()
    layers = _extract_layers(catalog)
    if not layers:
        return {"error": "no layers found in catalog", "fetched": 0}

    existing = {
        row.layer_id: row
        for row in (await db.execute(select(GovmapCoverage))).scalars().all()
    }
    inserted = 0
    retitled = 0
    for i, lay in enumerate(sorted(layers, key=lambda x: int(x["layer_id"]) if x["layer_id"].isdigit() else 0)):
        row = existing.get(lay["layer_id"])
        if row is None:
            db.add(GovmapCoverage(
                layer_id=lay["layer_id"],
                caption=lay["caption"],
                layer_kind=lay["layer_kind"],
                complexity=lay["complexity"],
                sort_order=i,
            ))
            inserted += 1
        else:
            # Propagate a caption change to the linked dataset's title, but
            # only when the title still equals the OLD caption (i.e. it was
            # auto-generated); a user-customized title is never overwritten.
            if (lay["caption"] and row.tracked_dataset_id
                    and lay["caption"] != row.caption):
                ds = (await db.execute(
                    select(TrackedDataset)
                    .where(TrackedDataset.id == row.tracked_dataset_id)
                )).scalar_one_or_none()
                if ds and row.caption and ds.title == row.caption:
                    ds.title = lay["caption"]
                    retitled += 1
            row.caption = lay["caption"]
            row.layer_kind = lay["layer_kind"]
            row.complexity = lay["complexity"]
    await db.commit()
    total = (await db.execute(select(func.count()).select_from(GovmapCoverage))).scalar() or 0
    return {"fetched": len(layers), "inserted": inserted,
            "updated": len(layers) - inserted, "retitled": retitled,
            "total": int(total)}


def _active_coverage_tasks_q():
    """Active (pending/running) scrape tasks that belong to COVERAGE-managed
    datasets. Regular datasets' tasks don't count against the rollout's
    concurrency target — they share the queue but have their own cadence."""
    return (
        select(ScrapeTask.tracked_dataset_id)
        .where(
            ScrapeTask.status.in_(("pending", "running")),
            ScrapeTask.tracked_dataset_id.in_(
                select(GovmapCoverage.tracked_dataset_id).where(
                    GovmapCoverage.tracked_dataset_id.is_not(None)
                )
            ),
        )
    )


async def _ensure_dataset(db, row: GovmapCoverage) -> TrackedDataset:
    """Return the TrackedDataset for this layer, creating it lazily the first
    time. Coverage datasets are marked ``coverage_managed`` so the normal
    per-dataset scheduler skips them — this coverage rollout is their sole
    driver."""
    if row.tracked_dataset_id:
        ds = (await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == row.tracked_dataset_id)
        )).scalar_one_or_none()
        if ds:
            return ds

    from app.api.govmap import build_govmap_title
    from app.api.utils import scraper_url_slug

    source_url = f"https://www.govmap.gov.il/?lay={row.layer_id}"
    # An existing dataset may already track this layer (added manually) —
    # ADOPT it instead of creating a duplicate. Manual URLs carry extra params
    # ("?c=...&z=...&lay=N"), so match on the layer id itself: the config's
    # layer_id, or lay=N appearing in the URL as a whole token. Prefer an
    # active dataset, then the oldest (the one with the history).
    lay_token = str(row.layer_id)
    ds = (await db.execute(
        select(TrackedDataset)
        .where(
            TrackedDataset.source_type == "govmap",
            (TrackedDataset.scraper_config["layer_id"].as_string() == lay_token)
            | TrackedDataset.source_url.op("~")(f"[?&]lay={lay_token}($|[^0-9])"),
        )
        .order_by(TrackedDataset.is_active.desc(), TrackedDataset.created_at.asc())
        .limit(1)
    )).scalars().first()
    if ds is not None:
        # Mark adopted so the rollout (not the per-dataset scheduler) drives it.
        cfg = dict(ds.scraper_config or {})
        if not cfg.get("coverage_managed"):
            cfg["coverage_managed"] = True
            ds.scraper_config = cfg
            ds.poll_interval = 7776000
    if ds is None:
        slug = scraper_url_slug(f"govmap-{row.layer_id}", source_url)
        ds = TrackedDataset(
            ckan_id=f"govmap-{slug}",
            ckan_name=slug,
            title=row.caption or build_govmap_title(row.layer_id),
            organization="govmap.gov.il",
            source_type="govmap",
            source_url=source_url,
            scraper_config={
                "kind": "govmap",
                "layer_id": str(row.layer_id),
                "download_files": False,
                "coverage_managed": True,
            },
            storage_mode="full_snapshot",
            # One quarter (90d). These datasets are coverage_managed (init_scheduler
            # skips per-dataset polling for them — the twice/4×-daily coverage
            # rollout drives re-scrapes stalest-first), so poll_interval is the
            # DISPLAYED "check frequency" / target cadence, not a live per-dataset
            # timer. A quarter reads sensibly in the UI (was 10y → "41 רבעונים").
            poll_interval=7776000,  # 90 days = 1 quarter
            status="active",
            is_active=True,
            # Stamp last_polled_at so init_scheduler never treats it as
            # "never polled → fire immediately" even if the skip guard is missed.
            last_polled_at=datetime.now(timezone.utc),
        )
        db.add(ds)
        await db.flush()
    row.tracked_dataset_id = ds.id
    return ds


async def scrape_next_layer() -> dict:
    """Scheduler tick: top up to ``GOVMAP_COVERAGE_CONCURRENCY`` active
    coverage tasks — but only from layers that are actually DUE:

      * never triggered (new inventory / newly discovered catalog layers), or
      * last triggered ≥ ``GOVMAP_COVERAGE_REFRESH_DAYS`` ago (the ongoing
        quarterly-style refresh — coverage datasets are skipped by the normal
        per-dataset scheduler, this tick is their ONLY driver), or
      * their LATEST attempt failed and ≥ ``GOVMAP_COVERAGE_RETRY_HOURS``
        passed (bounded retry loop for flap victims / transient errors).

    Without the DUE filter, the drain-mode behavior (always refill to
    target) would keep re-scraping the stalest layers forever once the
    initial 859-layer import finished — a permanent treadmill cycling the
    whole catalog every ~2-3 days instead of quarterly. With it, a fully
    fresh & healthy inventory makes this tick a cheap no-op."""
    target = max(1, int(settings.govmap_coverage_concurrency))
    refresh_cutoff = datetime.now(timezone.utc) - timedelta(
        days=float(settings.govmap_coverage_refresh_days))
    retry_cutoff = datetime.now(timezone.utc) - timedelta(
        hours=float(settings.govmap_coverage_retry_hours))
    async with async_session() as db:
        active_ids = [
            r[0] for r in (await db.execute(_active_coverage_tasks_q())).all()
        ]
        slots = target - len(active_ids)
        if slots <= 0:
            logger.info("govmap coverage: %d task(s) active ≥ target %d — skip",
                        len(active_ids), target)
            return {"skipped": "at_concurrency", "active": len(active_ids)}

        # Datasets whose LATEST scrape attempt failed — eligible for the
        # shorter retry cadence.
        last_status = (
            select(
                ScrapeTask.tracked_dataset_id,
                ScrapeTask.status,
            )
            .distinct(ScrapeTask.tracked_dataset_id)
            .order_by(ScrapeTask.tracked_dataset_id, ScrapeTask.created_at.desc())
            .subquery()
        )
        failed_ids = [
            r[0] for r in (await db.execute(
                select(last_status.c.tracked_dataset_id)
                .where(last_status.c.status == "failed")
            )).all()
        ]

        due = (
            GovmapCoverage.last_triggered_at.is_(None)
            | (GovmapCoverage.last_triggered_at < refresh_cutoff)
        )
        if failed_ids:
            due = due | (
                GovmapCoverage.tracked_dataset_id.in_(failed_ids)
                & (GovmapCoverage.last_triggered_at < retry_cutoff)
            )

        # Next DUE layers: NULL last_triggered_at first (never scraped), then
        # the stalest, tie-broken by sort_order. Exclude layers whose dataset
        # already has an active task (their last_triggered_at may be old —
        # e.g. a giant layer still scraping since yesterday's tick).
        q = (
            select(GovmapCoverage)
            .where(due)
            .order_by(
                GovmapCoverage.last_triggered_at.asc().nullsfirst(),
                GovmapCoverage.sort_order.asc(),
            )
            .limit(slots)
        )
        if active_ids:
            q = q.where(
                (GovmapCoverage.tracked_dataset_id.is_(None))
                | (GovmapCoverage.tracked_dataset_id.not_in(active_ids))
            )
        rows = (await db.execute(q)).scalars().all()
        if not rows:
            logger.debug("govmap coverage: nothing due — inventory fresh")
            return {"skipped": "nothing_due"}

        triggered = []
        for row in rows:
            ds = await _ensure_dataset(db, row)
            row.last_triggered_at = datetime.now(timezone.utc)
            triggered.append((str(ds.id), row.layer_id, row.caption))
        await db.commit()

    # Trigger the scrapes (each creates a pending task a free worker claims).
    # poll_dataset's single-flight guard + the DB's one-active-task-per-dataset
    # unique index make double-triggers harmless.
    from app.worker.poll_job import poll_dataset
    for ds_id, layer_id, caption in triggered:
        await poll_dataset(ds_id)
        logger.info("govmap coverage: triggered layer %s (%s) → ds %s",
                    layer_id, caption, ds_id)
    return {
        "triggered": [
            {"layer_id": lid, "caption": cap, "dataset_id": dsid}
            for dsid, lid, cap in triggered
        ],
        "active_before": len(active_ids),
        "target": target,
    }


async def coverage_status(db) -> dict:
    """Rollout progress for the admin view."""
    total = (await db.execute(select(func.count()).select_from(GovmapCoverage))).scalar() or 0
    triggered = (await db.execute(
        select(func.count()).select_from(GovmapCoverage).where(
            GovmapCoverage.last_triggered_at.isnot(None)
        )
    )).scalar() or 0
    with_ds = (await db.execute(
        select(func.count()).select_from(GovmapCoverage).where(
            GovmapCoverage.tracked_dataset_id.isnot(None)
        )
    )).scalar() or 0
    return {
        "total_layers": int(total),
        "ever_triggered": int(triggered),
        "not_yet_triggered": int(total) - int(triggered),
        "datasets_created": int(with_ds),
    }
