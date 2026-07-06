"""Throttled full-coverage rollout of the GovMap layer catalog.

Goal: eventually scrape every GovMap vector layer, but gently — two a day
(morning + evening), and never while the single self-hosted worker is already
busy, so we don't pile up a queue or hammer GovMap (which rate-limits hard).

Pieces:
  * ``populate_from_catalog`` — fetch GovMap's public layers catalog and upsert
    one ``GovmapCoverage`` row per vector layer.
  * ``scrape_next_layer`` — the twice-daily scheduler tick: if the worker is
    idle, pick the next layer (never-triggered first, then stalest), lazily
    create a govmap TrackedDataset for it (marked ``coverage_managed`` so the
    normal per-dataset scheduler leaves it to us), and trigger its scrape.

Coverage datasets are ordinary govmap datasets once created, so their versions,
storage plan, streaming/resume scrape path, etc. are all the existing pipeline.
"""
import logging
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy import func, select

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


def _extract_layers(catalog: dict) -> list[dict]:
    """Walk the catalog JSON, returning one dict per unique vector layer
    (deduped by id): {layer_id, caption, layer_kind, complexity}."""
    found: dict[str, dict] = {}

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
            row.caption = lay["caption"]
            row.layer_kind = lay["layer_kind"]
            row.complexity = lay["complexity"]
    await db.commit()
    total = (await db.execute(select(func.count()).select_from(GovmapCoverage))).scalar() or 0
    return {"fetched": len(layers), "inserted": inserted,
            "updated": len(layers) - inserted, "total": int(total)}


async def _worker_busy(db) -> bool:
    """True if any scrape task is pending or running — the single worker is
    occupied, so we must not stack another coverage scrape on top of it."""
    n = (await db.execute(
        select(func.count()).select_from(ScrapeTask).where(
            ScrapeTask.status.in_(("pending", "running"))
        )
    )).scalar() or 0
    return n > 0


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
    # An existing dataset may already track this layer (added manually) — reuse.
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.source_url == source_url)
    )).scalar_one_or_none()
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
    """Twice-daily scheduler tick: if the worker is idle, scrape the next layer
    in the coverage rollout (never-triggered first, then stalest). One layer per
    tick. Own session so it's independent of any request context."""
    async with async_session() as db:
        if await _worker_busy(db):
            logger.info("govmap coverage: worker busy, skipping this tick")
            return {"skipped": "worker_busy"}

        # Next layer: NULL last_triggered_at first (never scraped), then the
        # stalest, tie-broken by sort_order.
        row = (await db.execute(
            select(GovmapCoverage)
            .order_by(
                GovmapCoverage.last_triggered_at.asc().nullsfirst(),
                GovmapCoverage.sort_order.asc(),
            )
            .limit(1)
        )).scalar_one_or_none()
        if row is None:
            logger.info("govmap coverage: inventory empty (run populate first)")
            return {"skipped": "empty_inventory"}

        ds = await _ensure_dataset(db, row)
        row.last_triggered_at = datetime.now(timezone.utc)
        await db.commit()
        ds_id = str(ds.id)
        layer_id = row.layer_id
        caption = row.caption

    # Trigger the scrape (creates a pending scrape task the worker picks up).
    from app.worker.poll_job import poll_dataset
    await poll_dataset(ds_id)
    logger.info("govmap coverage: triggered layer %s (%s) → ds %s",
                layer_id, caption, ds_id)
    return {"triggered_layer": layer_id, "caption": caption, "dataset_id": ds_id}


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
