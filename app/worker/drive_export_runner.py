"""Durable runner for "export a version's files to Google Drive" jobs.

Every 30 s the scheduler calls ``drain_one_drive_export``. We claim the
oldest ``pending`` job, flip it to ``running``, then for each file in the
version: stage it to /tmp (stream from R2, or download from ODATA),
resumable-upload it into the target Drive folder, bump ``completed_files``
(which doubles as the heartbeat), and delete the temp file. On the last
file we flip to ``success``; any unhandled error flips to ``failed`` and a
later retry (manual or the stuck-job rescue) resumes from
``completed_files`` — the file list is enumerated deterministically, so
already-uploaded files are skipped rather than duplicated.

Design notes mirror the datastore-push runner next door:
  - One job per tick (``max_instances=1``). The work is IO-bound (await),
    so it doesn't block other scheduler jobs; only one heavy export runs
    at a time to keep dyno disk/RSS bounded (one staged file at a time).
  - Heartbeat = ``updated_at``, bumped per file via the per-file progress
    commit. Stale running jobs are rescued by ``cleanup_stuck_drive_exports``.
"""
import asyncio
import logging
import os
import time
from datetime import datetime, timedelta, timezone

import httpx
from sqlalchemy import select, update

from app.config import settings
from app.database import async_session
from app.models.drive_export_job import DriveExportJob
from app.models.user import User
from app.models.version_index import VersionIndex
from app.services import drive_client
from app.services import storage_client as storage
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

MAX_ATTEMPTS = 3
STUCK_HEARTBEAT_MINUTES = 20
TMP_DIR = "/tmp/drive_export"
# Re-mint the access token after this long; Google access tokens last ~1h
# and a large export can outrun a single one.
TOKEN_REFRESH_SECONDS = 50 * 60


async def drain_one_drive_export() -> None:
    """One scheduler tick: claim and process the oldest pending export."""
    async with async_session() as db:
        result = await db.execute(
            select(DriveExportJob)
            .where(DriveExportJob.status == "pending")
            .order_by(DriveExportJob.created_at.asc())
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
            "Claimed Drive export job %s (version %s, attempt %d/%d)",
            job_id, job.version_id, job.attempts, MAX_ATTEMPTS,
        )

    try:
        await _run_job(job_id)
    except Exception as e:
        logger.exception("Drive export job %s crashed unrecoverably", job_id)
        await _mark_failed(job_id, f"runner crashed: {e}")


async def _run_job(job_id) -> None:
    # Load everything we need up front.
    async with async_session() as db:
        job = (
            await db.execute(
                select(DriveExportJob).where(DriveExportJob.id == job_id)
            )
        ).scalar_one_or_none()
        if not job:
            return
        folder_id = job.folder_id
        already_done = job.completed_files
        version_id = job.version_id
        user_id = job.user_id

        version = None
        if version_id:
            version = (
                await db.execute(
                    select(VersionIndex).where(VersionIndex.id == version_id)
                )
            ).scalar_one_or_none()
        if version is None:
            await _mark_failed(job_id, "version no longer exists")
            return

        refresh_token = None
        if user_id:
            user = (
                await db.execute(select(User).where(User.id == user_id))
            ).scalar_one_or_none()
            refresh_token = user.google_refresh_token if user else None
        if not refresh_token:
            await _mark_failed(job_id, "no Google Drive connection for this user — reconnect Drive")
            return

        files = storage.enumerate_files(version.resource_mappings)

    if not files:
        await _mark_success(job_id)
        return

    # Mint the first access token. expiry tracked by wall-clock since mint.
    try:
        access_token = await drive_client.get_access_token(refresh_token)
    except Exception as e:
        await _mark_failed(job_id, str(e))
        return
    token_minted_at = time.monotonic()

    os.makedirs(TMP_DIR, exist_ok=True)

    # Resume: skip files already uploaded on a previous attempt.
    for idx in range(already_done, len(files)):
        filename, value = files[idx]

        # Refresh the access token if it's getting old.
        if time.monotonic() - token_minted_at > TOKEN_REFRESH_SECONDS:
            try:
                access_token = await drive_client.get_access_token(refresh_token)
                token_minted_at = time.monotonic()
            except Exception as e:
                await _mark_failed(job_id, f"token refresh failed mid-export: {e}")
                return

        # Stage the file to /tmp. Use the index in the temp name to avoid any
        # collision between same-named entries.
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in filename)
        tmp_path = os.path.join(TMP_DIR, f"{str(job_id)[:8]}_{idx}_{safe}")
        try:
            ok = await _stage_file(value, tmp_path)
            if not ok:
                # Source file unreachable (e.g. a 404-deleted resource). Don't
                # abort the whole export over one missing file — skip it and
                # advance so a resume won't retry it. Upload errors below ARE
                # systemic and do fail the job.
                logger.warning(
                    "Drive export job %s: skipping unreachable file '%s' (%s)",
                    job_id, filename, value,
                )
                await _progress(job_id, completed=idx + 1, current_file=filename)
                continue
            await drive_client.upload_file(access_token, folder_id, filename, tmp_path)
        except Exception as e:
            await _mark_failed(job_id, f"file '{filename}': {e}")
            return
        finally:
            try:
                os.remove(tmp_path)
            except OSError:
                pass

        await _progress(job_id, completed=idx + 1, current_file=filename)
        logger.info(
            "Drive export job %s: uploaded %d/%d (%s)",
            job_id, idx + 1, len(files), filename,
        )

    await _mark_success(job_id)


async def _stage_file(value: str, dest_path: str) -> bool:
    """Download one file to ``dest_path``. R2-marked values stream straight
    from the object store; bare ODATA resource_ids are fetched via the CKAN
    resource download URL."""
    if storage.is_storage_value(value):
        return await storage_client.download_to_file(value, dest_path)
    return await _download_odata_to_file(value, dest_path)


async def _download_odata_to_file(resource_id: str, dest_path: str) -> bool:
    """Fetch an ODATA resource's file to disk (256 KB chunks). Resolves the
    real storage URL via resource_show, mirroring datastore_push_runner."""
    base = settings.odata_url.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            info = await client.get(
                f"{base}/api/3/action/resource_show", params={"id": resource_id}
            )
            info.raise_for_status()
            data = info.json()
            url = (data.get("result") or {}).get("url") if data.get("success") else None
            if not url:
                return False
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as out:
                    async for chunk in resp.aiter_bytes(256 * 1024):
                        out.write(chunk)
        return True
    except Exception:
        logger.exception("ODATA download failed for resource %s", resource_id)
        return False


async def _progress(job_id, *, completed: int, current_file: str | None) -> None:
    async with async_session() as db:
        await db.execute(
            update(DriveExportJob)
            .where(DriveExportJob.id == job_id)
            .values(
                completed_files=completed,
                current_file=(current_file or "")[:512],
                updated_at=datetime.now(timezone.utc),
            )
        )
        await db.commit()


async def _mark_success(job_id) -> None:
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        await db.execute(
            update(DriveExportJob)
            .where(DriveExportJob.id == job_id)
            .values(status="success", completed_at=now, updated_at=now,
                    current_file=None, error=None)
        )
        await db.commit()


async def _mark_failed(job_id, error: str) -> None:
    now = datetime.now(timezone.utc)
    async with async_session() as db:
        await db.execute(
            update(DriveExportJob)
            .where(DriveExportJob.id == job_id)
            .values(status="failed", completed_at=now, updated_at=now,
                    error=(error or "")[:4000])
        )
        await db.commit()
    logger.warning("Drive export job %s failed: %s", job_id, error)


async def cleanup_stuck_drive_exports() -> None:
    """Reset ``running`` exports whose heartbeat is stale back to ``pending``
    so the runner resumes them (from ``completed_files``)."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=STUCK_HEARTBEAT_MINUTES)
    async with async_session() as db:
        result = await db.execute(
            select(DriveExportJob).where(
                DriveExportJob.status == "running",
                DriveExportJob.updated_at < cutoff,
            )
        )
        stale = result.scalars().all()
        if not stale:
            return
        for j in stale:
            if j.attempts >= MAX_ATTEMPTS:
                j.status = "failed"
                j.error = f"Gave up after {j.attempts} attempts (stalled mid-export)"
            else:
                j.status = "pending"
                j.error = f"Auto-recovered after no heartbeat for {STUCK_HEARTBEAT_MINUTES}min"
            logger.warning(
                "Stuck Drive export job %s → %s (attempts=%d)",
                j.id, j.status, j.attempts,
            )
        await db.commit()
