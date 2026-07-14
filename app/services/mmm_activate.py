"""Activate the MMM (ממ״מ) dataset's daily INCREMENTAL archive mode.

The MMM corpus is ONE dataset of ~6,500 docs behind the Radware WAF; a full
re-scrape is ~2.7h, so it polls DAILY in incremental archive mode instead:
``archive_type="mmm"`` + a ``checkpoint.known_rids`` seeded from the rids already
mirrored into ``knesset.mmm_documents`` so the first poll re-downloads nothing
and only genuinely-new documents land in each small delta version.

This module is called from BOTH the admin endpoint (manual / dry-run) and a
non-blocking startup task, so a deploy auto-activates without an admin token.

Guards (startup path):
  * **already archive mode** → NO-OP. Never re-seed once active — the worker
    grows ``known_rids`` on every poll, and re-seeding from the (possibly staler)
    catalog would lose that ground and risk re-downloading files.
  * **empty catalog** → SKIP. Activating with an empty seed would make the first
    poll treat all ~6,500 docs as new (the exact 2.7h full scrape we avoid). We
    only activate once ``mmm_documents`` is populated; a later deploy retries.
"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

logger = logging.getLogger(__name__)

MMM_DAILY_INTERVAL = 86_400  # seconds — poll once a day


async def all_mmm_rids() -> list[str]:
    """Every rid catalogued in ``knesset.mmm_documents`` — the checkpoint seed."""
    from app.services import append_store
    from app.services.knesset_db import _qtable

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                f"SELECT rid FROM {_qtable('mmm_documents')} "
                f"WHERE rid IS NOT NULL ORDER BY rid"
            )
        except Exception as e:  # table/schema not created yet
            logger.warning("MMM activate: mmm_documents unreadable (%s) — empty seed", e)
            return []
    return [str(r["rid"]) for r in rows]


async def find_mmm_datasets(db) -> list:
    """The tracked MMM dataset(s) (``ckan_name`` like ``knesset-mmm%``)."""
    from app.models.tracked_dataset import TrackedDataset

    return (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name.like("knesset-mmm%"))
    )).scalars().all()


def apply_mmm_archive(ds, known_rids: list[str]) -> None:
    """Mutate ``ds`` into daily incremental archive mode, preserving every other
    scraper_config key. Caller commits."""
    cfg = dict(ds.scraper_config or {})
    cfg["archive"] = True
    cfg["archive_type"] = "mmm"
    cfg["checkpoint"] = {"known_rids": known_rids, "total_docs": len(known_rids)}
    cfg.pop("mmm_full_rescan", None)  # clear any stale one-shot flag
    ds.scraper_config = cfg
    flag_modified(ds, "scraper_config")
    ds.poll_interval = MMM_DAILY_INTERVAL
    ds.is_active = True
    ds.status = "active"


async def activate_mmm_archive_if_needed() -> dict:
    """Startup auto-activation. Idempotent + guarded (see module docstring).

    Never raises — a failure just logs and leaves the dataset untouched so boot
    is never blocked and a later deploy can retry."""
    try:
        from app.database import async_session

        async with async_session() as db:
            rows = await find_mmm_datasets(db)
            if not rows:
                logger.info("MMM activate: no knesset-mmm%% dataset yet — skipping")
                return {"skipped": "no dataset"}
            if len(rows) > 1:
                logger.warning(
                    "MMM activate: %d MMM datasets found — skipping auto-activation",
                    len(rows),
                )
                return {"skipped": "multiple datasets"}
            ds = rows[0]
            cfg = ds.scraper_config or {}
            if cfg.get("archive") and cfg.get("archive_type") == "mmm":
                logger.info("MMM activate: already in archive mode — no-op")
                return {"skipped": "already archive mode"}

            known_rids = await all_mmm_rids()
            if not known_rids:
                logger.warning(
                    "MMM activate: catalog empty (mmm_documents not synced yet) — "
                    "deferring activation to avoid a full re-scrape")
                return {"skipped": "empty catalog"}

            apply_mmm_archive(ds, known_rids)
            await db.commit()
            logger.info(
                "MMM activate: enabled DAILY incremental archive on %s "
                "(known_rids seeded=%d, interval=%ds)",
                ds.id, len(known_rids), MMM_DAILY_INTERVAL,
            )
            return {"activated": True, "dataset_id": str(ds.id),
                    "known_rids": len(known_rids)}
    except Exception:  # noqa: BLE001
        logger.exception("MMM activate: auto-activation failed (non-fatal)")
        return {"error": "auto-activation failed"}
