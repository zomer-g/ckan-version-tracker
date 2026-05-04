"""Conditional (HEAD-first) archive technique.

The legacy ``poll_job`` path always downloads every tracked resource
just to compute SHA-256 and compare against the previous hash. For the
common case where data.gov.il bumps ``metadata_modified`` without any
resource bytes changing, that's wasted bandwidth and wasted ODATA
storage (each new version uploads identical bytes again).

This module is the "more correct technique" that runs FIRST. It uses
two cheap probes — ``datastore_info`` for datastore-backed resources
and HTTP HEAD for direct files — to ask "did anything actually change
since last version?" without downloading. When the answer is a clean
"no", it writes a new VersionIndex row with ``source="conditional"``
that points at the same ODATA resources as the previous version (zero
new uploads). When the answer is "yes" or "unverifiable", it returns
``Result.FALLBACK`` so the legacy download-and-hash pipeline runs
exactly as today.

Design constraints:

- Stays inside the existing FastAPI process. No new infrastructure.
- Writes to the same ODATA CKAN as legacy (by reusing resource_ids).
- All commits happen here on success; on FALLBACK the caller's session
  is untouched so the legacy path can run cleanly.
- Defensive: any unexpected exception → FALLBACK, never raise.

Probe semantics:

- Datastore (``datastore_info`` returns total + fields): if both the
  row count AND the field-id list match the previous snapshot, the
  resource is treated as unchanged. This is not cryptographic, but
  data.gov.il's datastore_search export is deterministic over the same
  underlying table — matching shape + count is a strong signal.
- Direct file: HEAD response. ``ETag`` match is the strong signal;
  ``Last-Modified`` + ``Content-Length`` both matching is the backup
  signal. Anything weaker → FALLBACK.

The probe results are persisted on ``TrackedDataset.resource_probes``
(NOT on the version row). This decouples the baseline from version
creation: every successful probe updates the baseline regardless of
whether it ended in CREATED, NO_CHANGE, or FALLBACK, so the legacy
path doesn't have to know anything about probes for the optimization
to bootstrap and survive across versions.
"""

from __future__ import annotations

import enum
import logging
import uuid
from datetime import datetime, timezone

from sqlalchemy import select

from app.config import settings
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services.ckan_client import ckan_client
from app.services.version_detector import has_metadata_changed

logger = logging.getLogger(__name__)


class Result(str, enum.Enum):
    CREATED = "created"      # new conditional version row written
    NO_CHANGE = "no_change"  # nothing changed, last_polled_at touched
    FALLBACK = "fallback"    # caller should run the legacy pipeline


def is_enabled() -> bool:
    return bool(settings.conditional_archive_enabled)


async def try_conditional_archive(ds: TrackedDataset, db) -> Result:
    """Try the cheap probe path. See module docstring for semantics."""
    if not is_enabled():
        return Result.FALLBACK

    try:
        return await _try(ds, db)
    except Exception:
        # Belt-and-suspenders: any unexpected error → silent fallback.
        logger.exception(
            "Conditional archiver crashed for %s — falling back",
            ds.ckan_name,
        )
        return Result.FALLBACK


async def _try(ds: TrackedDataset, db) -> Result:
    # 1. Cheap metadata fetch from data.gov.il
    pkg = await ckan_client.package_show(ds.ckan_id)
    new_modified = pkg.get("metadata_modified", "")

    # 2. Has anything at the package level changed at all?
    if not has_metadata_changed(ds.last_modified, new_modified):
        ds.last_polled_at = datetime.now(timezone.utc)
        await db.commit()
        logger.info(
            "Conditional: %s metadata unchanged (modified=%s)",
            ds.ckan_name, new_modified,
        )
        return Result.NO_CHANGE

    # 3. Filter to the actually-tracked resources (same precedence as
    #    poll_job: resource_ids > resource_id > all).
    all_resources = pkg.get("resources", []) or []
    tracked_ids: set[str] | None = None
    if ds.resource_ids:
        tracked_ids = set(ds.resource_ids)
        resources = [r for r in all_resources if r.get("id") in tracked_ids]
    elif ds.resource_id:
        tracked_ids = {ds.resource_id}
        resources = [r for r in all_resources if r.get("id") == ds.resource_id]
    else:
        resources = list(all_resources)

    if not resources:
        # Tracked subset isn't on the source anymore — let legacy log
        # this properly via its own path.
        return Result.FALLBACK

    # 4. Append-only mode requires row-level work; conditional probe
    #    doesn't apply. Let legacy run.
    if ds.storage_mode == "append_only":
        return Result.FALLBACK

    # 5. Probe each tracked resource against the persistent baseline
    #    on TrackedDataset. Every probe also UPDATES the baseline so
    #    the optimization bootstraps from FALLBACK on the first poll
    #    and engages on the second poll, without depending on what
    #    the legacy path writes.
    old_probes: dict = ds.resource_probes or {}
    new_probes: dict = dict(old_probes)
    now_iso = datetime.now(timezone.utc).isoformat()

    all_unchanged = True
    have_baseline_for_all = bool(old_probes)

    for r in resources:
        rid = r.get("id")
        url = r.get("url") or ""
        if not rid:
            return Result.FALLBACK

        # 5a. Try datastore_info first.
        ds_signal = await _probe_datastore(rid)
        if ds_signal is not None:
            new_probes[rid] = {
                "datastore": ds_signal,
                "http": None,
                "observed_at": now_iso,
            }
            old = (old_probes.get(rid) or {}).get("datastore")
            if old is None:
                have_baseline_for_all = False
                all_unchanged = False
            elif old != ds_signal:
                all_unchanged = False
            continue

        # 5b. Direct file: HEAD on the URL.
        if not url:
            return Result.FALLBACK
        http_signal = await _probe_http(url)
        if http_signal is None:
            return Result.FALLBACK
        new_probes[rid] = {
            "datastore": None,
            "http": http_signal,
            "observed_at": now_iso,
        }
        old = (old_probes.get(rid) or {}).get("http") or {}
        if not old:
            have_baseline_for_all = False
            all_unchanged = False
        elif not _http_signals_match(old, http_signal):
            all_unchanged = False

    # 6. Persist the new baselines unconditionally — even on FALLBACK,
    #    so the next poll has something to compare against.
    ds.resource_probes = new_probes

    # 7. If we couldn't confirm everything unchanged, hand off to legacy.
    #    We still committed the probe baseline above so the next cycle
    #    can short-circuit.
    if not have_baseline_for_all or not all_unchanged:
        await db.commit()
        return Result.FALLBACK

    # 8. Every tracked resource confirmed unchanged. Mint a new
    #    "metadata-only" version that reuses the previous mappings —
    #    no new uploads, no new ODATA resources, but the version row
    #    is recorded so the user can see "v(N+1) is metadata-only,
    #    bytes identical to v(N)".
    latest_q = await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc())
        .limit(1)
    )
    latest_version: VersionIndex | None = latest_q.scalar_one_or_none()
    if latest_version is None:
        # No previous version to point at — shouldn't happen since
        # legacy would have created v1, but guard defensively.
        await db.commit()
        return Result.FALLBACK

    next_version = (latest_version.version_number or 0) + 1
    version = VersionIndex(
        id=uuid.uuid4(),
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=new_modified,
        odata_metadata_resource_id=latest_version.odata_metadata_resource_id,
        change_summary={
            "type": "metadata_only",
            "no_resource_changes": True,
            "resources_added": [],
            "resources_removed": [],
            "resources_modified": [],
            "probes_used": list(new_probes.keys()),
        },
        resource_mappings=dict(latest_version.resource_mappings or {}),
        source="conditional",
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_modified = new_modified
    ds.last_error = None
    await db.commit()

    logger.info(
        "Conditional: created metadata-only v%d for %s "
        "(%d resources confirmed unchanged via probes)",
        next_version, ds.ckan_name, len(resources),
    )
    return Result.CREATED


async def _probe_datastore(resource_id: str) -> dict | None:
    """Return ``{total, fields}`` if the resource is datastore-backed,
    else ``None`` (signalling 'fall through to HTTP HEAD')."""
    try:
        info = await ckan_client.datastore_info(resource_id)
        # Empty datastore is ambiguous — could be a non-datastore
        # resource that returned 0/[] rather than failing. Fall through.
        if not info.get("fields"):
            return None
        return {
            "total": info.get("total", 0),
            "fields": [f["id"] for f in info["fields"] if "id" in f],
        }
    except Exception:
        return None


async def _probe_http(url: str) -> dict | None:
    """Return ``{etag, last_modified, content_length}`` from a HEAD,
    or ``None`` if the HEAD failed or returned no useful headers."""
    try:
        meta = await ckan_client.head_resource(url)
    except Exception:
        return None
    etag = meta.get("etag")
    lm = meta.get("last_modified")
    cl = meta.get("content_length")
    # No useful header at all → not enough to confirm anything.
    if not etag and not (lm and cl):
        return None
    return {"etag": etag, "last_modified": lm, "content_length": cl}


def _http_signals_match(old: dict, new: dict) -> bool:
    """Strong signal: same ETag. Backup: Last-Modified AND Content-Length both match."""
    if new.get("etag") and new["etag"] == old.get("etag"):
        return True
    new_lm, new_cl = new.get("last_modified"), new.get("content_length")
    old_lm, old_cl = old.get("last_modified"), old.get("content_length")
    if new_lm and new_cl and new_lm == old_lm and new_cl == old_cl:
        return True
    return False
