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
import uuid as _uuid

from sqlalchemy.orm.attributes import flag_modified

logger = logging.getLogger(__name__)

MMM_DAILY_INTERVAL = 86_400  # seconds — poll once a day

# The tracked-dataset UUID of the FULL ("מלא") MMM corpus — the same
# ``over_dataset_id`` TAG-IT pulls from (smart-dms seeds it into scope 14). We
# target this exact id rather than a ``knesset-mmm%`` name prefix because a
# stray empty duplicate (``knesset-mmm-81570410``, 0 versions, the default.aspx
# landing page) also matches that prefix — making the pattern ambiguous.
MMM_OVER_DATASET_ID = "5541c4a2-0736-43c7-9392-e050081bcddc"


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


async def get_mmm_dataset(db):
    """The tracked FULL MMM dataset (by its exact UUID), or None."""
    from app.models.tracked_dataset import TrackedDataset

    try:
        return await db.get(TrackedDataset, _uuid.UUID(MMM_OVER_DATASET_ID))
    except Exception as e:  # bad UUID / DB error
        logger.warning("MMM activate: dataset lookup failed (%s)", e)
        return None


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
            ds = await get_mmm_dataset(db)
            if ds is None:
                logger.info("MMM activate: dataset %s not found — skipping",
                            MMM_OVER_DATASET_ID)
                return {"skipped": "dataset not found"}
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

            last_polled_at = ds.last_polled_at
            apply_mmm_archive(ds, known_rids)
            await db.commit()

            # Register the poll job in the ALREADY-RUNNING scheduler. init_scheduler
            # ran before this background task and (if the dataset was inactive)
            # skipped it, so without this the daily poll wouldn't start until the
            # next restart. add_poll_job replaces any existing job and, since the
            # dataset is overdue (last polled 2026-07-11, now daily), fires on the
            # next tick — kicking off the first incremental delta right away.
            from app.worker.scheduler import add_poll_job
            add_poll_job(str(ds.id), MMM_DAILY_INTERVAL, last_polled_at=last_polled_at)

            logger.info(
                "MMM activate: enabled DAILY incremental archive on %s "
                "(known_rids seeded=%d, interval=%ds) + poll job scheduled",
                ds.id, len(known_rids), MMM_DAILY_INTERVAL,
            )
            return {"activated": True, "dataset_id": str(ds.id),
                    "known_rids": len(known_rids)}
    except Exception:  # noqa: BLE001
        logger.exception("MMM activate: auto-activation failed (non-fatal)")
        return {"error": "auto-activation failed"}
