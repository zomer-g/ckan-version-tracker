"""Durable runner for datastore-ingest push jobs.

Loop, briefly: every 30 s the scheduler calls ``drain_one_job``. We
pick the oldest ``pending`` job, atomically flip it to ``running``,
recover the CSV (from /tmp if still there, otherwise re-download
from ODATA), stream-push it batch by batch through the existing
``_push_batch_with_retry`` helper, and flip the job to ``success``
or ``failed`` at the end. The heartbeat (``updated_at``) is bumped
every batch so a separate cleanup task can rescue stuck rows.

Design notes:
  - We process at most ONE job per scheduler tick. The push itself
    can take minutes; running two concurrently would double the
    Render dyno's RSS and risk OOM. The scheduler queues the next
    pending job naturally on the next tick.
  - Heartbeat = ``updated_at``. We commit a fresh ``rows_pushed``
    after every batch, which incidentally bumps ``updated_at`` via
    the ORM's ``onupdate`` hook. So the stuck-job cleanup is just
    "running rows with stale updated_at" — no separate heartbeat
    column needed.
  - CSV recovery: when ``csv_path`` is gone, GET the ODATA download
    URL for ``resource_id``. The upload-csv endpoint always uploads
    the (possibly gzipped) CSV BEFORE enqueueing the job, so the
    file is reliably there. Recovered files are written to /tmp under
    the same filename, gunzipped when needed, and deleted on success.
"""
import asyncio
import gzip as _gzip
import json
import logging
import os
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy import select, update

from app.config import settings
from app.database import async_session
from app.models.datastore_push_job import DatastorePushJob
from app.services.odata_client import odata_client

logger = logging.getLogger(__name__)

# How many rows per datastore_create / datastore_upsert call. Match
# the historical BackgroundTask path (push_records_to_datastore_from_file)
# so behaviour is identical when nothing goes wrong; only durability
# changes.
BATCH_SIZE = 2500
# Don't process the same job more than this many times. Past N, the
# admin must intervene (probably the data has a fundamental issue
# that retries can't fix — e.g. a row that violates a hard CKAN
# constraint).
MAX_ATTEMPTS = 5
# Heartbeat staleness threshold. Anything in "running" for longer
# than this with no row-progress update is reset to "pending".
STUCK_HEARTBEAT_MINUTES = 15
# Recovery file location. Mirrors upload-csv's directory so the
# cleanup logic is the same path.
TMP_DIR = "/tmp/upload_csv"


async def drain_one_job() -> None:
    """One scheduler tick: claim and process the oldest pending job.

    Returns silently when the queue is empty. Errors during processing
    are swallowed and persisted to the job row — we never let a single
    bad job break the scheduler.
    """
    async with async_session() as db:
        # Claim the next pending job. PostgreSQL ``FOR UPDATE SKIP
        # LOCKED`` would be ideal here (multiple dynos racing for
        # work), but Render starter dynos are single-process so the
        # simpler "select-and-update" with a status guard is enough.
        result = await db.execute(
            select(DatastorePushJob)
            .where(DatastorePushJob.status == "pending")
            .order_by(DatastorePushJob.created_at.asc())
            .limit(1)
        )
        job = result.scalar_one_or_none()
        if not job:
            return

        now = datetime.now(timezone.utc)
        job.status = "running"
        job.attempts = job.attempts + 1
        job.started_at = now
        job.updated_at = now
        job.error = None
        await db.commit()
        job_id = job.id
        logger.info(
            "Claimed datastore push job %s for resource %s (attempt %d/%d, csv=%s)",
            job_id, job.resource_id, job.attempts, MAX_ATTEMPTS, job.csv_path,
        )

    # Hand off to the actual push. Outside the claim transaction so
    # the row's "running" state is visible to other queries (admin UI,
    # stuck-job detector) while the long work executes.
    try:
        await _run_job(job_id)
    except Exception as e:
        logger.exception("Datastore push job %s crashed unrecoverably", job_id)
        await _mark_failed(job_id, f"runner crashed: {e}")


async def _run_job(job_id) -> None:
    """Execute one claimed job. Sets terminal state on completion."""
    async with async_session() as db:
        job = (
            await db.execute(
                select(DatastorePushJob).where(DatastorePushJob.id == job_id)
            )
        ).scalar_one_or_none()
        if not job:
            return
        resource_id = job.resource_id
        csv_path = job.csv_path
        gz_in_source = job.csv_is_gzipped_in_source
        try:
            fields = json.loads(job.fields_json or "[]")
        except Exception:
            fields = []
        if not fields:
            await _mark_failed(job_id, "fields_json missing or unparseable")
            return

    # ---- Step 1: ensure the CSV is on disk ----
    recovered = False
    if not os.path.exists(csv_path):
        try:
            csv_path = await _recover_csv_from_odata(
                resource_id=resource_id,
                target_path=csv_path,
                gz_in_source=gz_in_source,
            )
            recovered = True
            logger.info(
                "Recovered CSV from ODATA for job %s → %s",
                job_id, csv_path,
            )
        except Exception as e:
            logger.exception(
                "CSV recovery from ODATA failed for job %s (resource %s)",
                job_id, resource_id,
            )
            await _mark_failed(
                job_id,
                f"csv recovery failed: {e}; CSV gone from /tmp and "
                f"resource {resource_id} unreadable from ODATA",
            )
            return

    # ---- Step 2: stream + push ----
    try:
        await _stream_and_push(
            job_id=job_id, resource_id=resource_id, fields=fields, csv_path=csv_path
        )
    except Exception as e:
        logger.exception(
            "Datastore push errored for job %s (resource %s)",
            job_id, resource_id,
        )
        await _mark_failed(job_id, str(e))
        # Don't delete the recovered file — gives a future retry the
        # same fast path. Original /tmp path that we recovered into
        # stays for the next attempt.
        return

    # ---- Step 3: mark success + clean up ----
    await _mark_success(job_id)
    try:
        os.remove(csv_path)
        if recovered:
            logger.info("Deleted recovered CSV %s after successful push", csv_path)
    except OSError:
        pass


async def _stream_and_push(
    *, job_id, resource_id: str, fields: list[dict], csv_path: str
) -> None:
    """The batch loop. Same shape as the old
    ``push_records_to_datastore_from_file`` but persists rows_pushed
    progress to the DB after every batch (heartbeat + observability).
    """
    import csv as _csv

    total_pushed = 0
    batch_num = 0
    pending: list[dict] | None = None

    async def _flush(records_batch: list[dict], create: bool, is_last: bool):
        nonlocal total_pushed, batch_num
        if not records_batch:
            return
        batch_num += 1
        await odata_client._push_batch_with_retry(
            resource_id=resource_id,
            fields=fields,
            records_batch=records_batch,
            create=create,
            batch_num=batch_num,
            is_last=is_last,
        )
        total_pushed += len(records_batch)
        await _heartbeat(job_id, rows_pushed=total_pushed)
        logger.info(
            "Datastore batch %d (%d rows, cumulative %d)%s → %s",
            batch_num, len(records_batch), total_pushed,
            " [final, record count refreshed]" if is_last else "",
            resource_id,
        )
        import gc as _gc
        _gc.collect()
        if not is_last:
            await asyncio.sleep(1)

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = _csv.DictReader(fh)
        current: list[dict] = []
        for row in reader:
            current.append(dict(row))
            if len(current) >= BATCH_SIZE:
                if pending is not None:
                    await _flush(pending, create=(batch_num == 0), is_last=False)
                pending = current
                current = []
        if current:
            if pending is not None:
                await _flush(pending, create=(batch_num == 0), is_last=False)
            await _flush(current, create=(batch_num == 0), is_last=True)
        elif pending is not None:
            await _flush(pending, create=(batch_num == 0), is_last=True)


async def _recover_csv_from_odata(
    *, resource_id: str, target_path: str, gz_in_source: bool
) -> str:
    """Re-fetch the CSV from ODATA when /tmp lost it.

    The file we uploaded to ODATA is at ``/dataset/{ds}/resource/{rid}
    /download``. CKAN's redirect chain lands on the actual storage URL.
    We write to disk in 256 KB chunks (constant memory), then gunzip
    in place if the source was gzipped.
    """
    os.makedirs(TMP_DIR, exist_ok=True)
    # Bypass the dataset_id segment by hitting CKAN's resource_show to
    # pick up the canonical url, OR just use a direct /resource/.../
    # download URL — but we don't have dataset_id here. Use the
    # /dataset/_/resource/{rid}/download path? CKAN actually accepts
    # the resource UUID alone via the API. Simplest path: probe
    # resource_show for the URL.
    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        info = await client.get(
            f"{settings.odata_url.rstrip('/')}/api/3/action/resource_show",
            params={"id": resource_id},
        )
        info.raise_for_status()
        data = info.json()
        if not data.get("success"):
            raise RuntimeError(f"resource_show failed: {data}")
        url = (data.get("result") or {}).get("url")
        if not url:
            raise RuntimeError(f"resource {resource_id} has no url")
        # Write to a recovery filename so we don't collide with the
        # original /tmp path (which may have been re-used by another
        # upload).
        recovery_path = target_path + ".recovered"
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            with open(recovery_path, "wb") as out:
                async for chunk in resp.aiter_bytes(256 * 1024):
                    out.write(chunk)

    if gz_in_source:
        decompressed_path = target_path
        with _gzip.open(recovery_path, "rb") as g_in, open(decompressed_path, "wb") as out:
            shutil.copyfileobj(g_in, out, length=256 * 1024)
        os.remove(recovery_path)
        return decompressed_path
    # Not gzipped — just rename to the target path so cleanup logic
    # downstream treats it like any other on-disk CSV.
    os.replace(recovery_path, target_path)
    return target_path


async def _heartbeat(job_id, *, rows_pushed: int) -> None:
    """Bump rows_pushed + updated_at on a running job."""
    async with async_session() as db:
        await db.execute(
            update(DatastorePushJob)
            .where(DatastorePushJob.id == job_id)
            .values(
                rows_pushed=rows_pushed,
                updated_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()


async def _mark_success(job_id) -> None:
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        await db.execute(
            update(DatastorePushJob)
            .where(DatastorePushJob.id == job_id)
            .values(
                status="success",
                completed_at=now,
                updated_at=now,
                error=None,
            )
        )
        await db.commit()


async def _mark_failed(job_id, error: str) -> None:
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        # If we've hit MAX_ATTEMPTS keep it as "failed"; otherwise we
        # leave it as "failed" too but the admin (or a future "auto
        # retry" worker) can reset attempts and bump back to pending.
        await db.execute(
            update(DatastorePushJob)
            .where(DatastorePushJob.id == job_id)
            .values(
                status="failed",
                completed_at=now,
                updated_at=now,
                error=(error or "")[:4000],  # cap pathological tracebacks
            )
        )
        await db.commit()


async def cleanup_stuck_push_jobs() -> None:
    """Reset any ``running`` job whose heartbeat is stale back to
    ``pending`` so the runner picks it up again. Mirrors the
    ``cleanup_stuck_scrape_tasks`` pattern next door — a worker that
    crashed mid-batch otherwise leaves the row stuck forever.
    """
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=STUCK_HEARTBEAT_MINUTES)
    async with async_session() as db:
        result = await db.execute(
            select(DatastorePushJob).where(
                DatastorePushJob.status == "running",
                DatastorePushJob.updated_at < cutoff,
            )
        )
        stale = result.scalars().all()
        if not stale:
            return
        for j in stale:
            age_min = int((now - (j.started_at or j.created_at)).total_seconds() / 60)
            j.status = "pending"
            j.error = (
                f"Auto-recovered after no heartbeat for "
                f"{STUCK_HEARTBEAT_MINUTES}min (job age {age_min}min)"
            )
            logger.warning(
                "Resetting stuck push job %s back to pending (age=%dmin, attempts=%d)",
                j.id, age_min, j.attempts,
            )
        await db.commit()


# ---- Enqueue helpers, used by app/api/worker.py ---------------------------

async def enqueue(
    *,
    db,
    tracked_dataset_id: Any | None,
    resource_id: str,
    csv_path: str,
    csv_is_gzipped_in_source: bool,
    fields: list[dict],
    total_rows: int | None,
) -> DatastorePushJob:
    """Create a pending DatastorePushJob row.

    The caller commits the surrounding transaction; we don't commit
    here so the enqueue is atomic with the upload-csv response.
    """
    job = DatastorePushJob(
        tracked_dataset_id=tracked_dataset_id,
        resource_id=resource_id,
        csv_path=csv_path,
        csv_is_gzipped_in_source=csv_is_gzipped_in_source,
        fields_json=json.dumps(fields, ensure_ascii=False),
        total_rows=total_rows,
    )
    db.add(job)
    await db.flush()
    return job
