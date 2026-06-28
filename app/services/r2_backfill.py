"""Migrate a dataset's version history from the ODATA mirror onto R2.

Shared core for both the CLI (``scripts/backfill_dataset_to_r2.py``) and the
admin endpoint (``POST /api/admin/datasets/{id}/migrate-r2``). See the CLI
module docstring for the full rationale; in short:

- Dates + metadata already live in Postgres (version_index) — only FILE BYTES
  move. ODATA originals are KEPT as a backup.
- We collect the UNIQUE ODATA mirror resource_ids referenced across all
  versions (named ``source_id -> odata_id`` mappings, ``backfilled``,
  ``_zip``/``_zip_parts``/``_geojson``, and the ``odata_metadata_resource_id``
  column), download each ONCE, upload to a DETERMINISTIC R2 key, and repoint
  every version's mappings at the ``r2:<key>`` markers.
- ``_resource_ids`` and ``_hashes`` are EXCLUDED — they hold data.gov.il
  *source* ids / hashes, not ODATA mirror ids.
- Idempotent: already-``r2:`` values are skipped; deterministic keys mean a
  re-run overwrites rather than duplicates.
"""
from __future__ import annotations

import asyncio
import logging
import re
import uuid as uuidlib
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from app.config import settings
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import storage_client as storage
from app.services.storage_client import storage_client

logger = logging.getLogger(__name__)

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)

# resource_mappings keys whose values are NOT ODATA mirror ids (data.gov.il
# source ids / hashes / bookkeeping) — never migrate these.
SKIP_KEYS = {"_hashes", "_resource_ids", "_appendonly_seen", "_large_dataset_info",
             "_names", "_filedates"}

TABULAR = {"csv", "tsv", "txt"}

# Bounded concurrency for the ODATA round-trips (resource_show + download) and
# R2 uploads. Keeps the dyno's memory/socket use sane while collapsing dozens
# of sequential round-trips into a few seconds.
MAX_CONCURRENCY = 6


def _looks_like_odata_id(v) -> bool:
    return (
        isinstance(v, str)
        and not storage.is_storage_value(v)
        and bool(_UUID_RE.match(v))
    )


def collect_odata_ids(versions: list[VersionIndex]) -> set[str]:
    """Every unique ODATA mirror id referenced across the given versions."""
    ids: set[str] = set()
    for v in versions:
        m = v.resource_mappings or {}
        for key, value in m.items():
            if key in SKIP_KEYS:
                continue
            if isinstance(value, list):
                for el in value:
                    if _looks_like_odata_id(el):
                        ids.add(el)
                continue
            if _looks_like_odata_id(value):
                ids.add(value)
        if _looks_like_odata_id(v.odata_metadata_resource_id):
            ids.add(v.odata_metadata_resource_id)
    return ids


def backfill_key(ds_id: str, odata_id: str, name: str, fmt: str) -> str:
    """Deterministic R2 key for a migrated ODATA resource (re-run overwrites)."""
    ext = (fmt or "dat").lower().strip().lstrip(".") or "dat"
    base = storage._safe_filename(name) or "file"
    return f"datasets/{ds_id}/backfill/{odata_id}_{base}.{ext}"


def rewrite_mappings(mappings: dict, id_to_r2: dict[str, str]) -> tuple[dict, int]:
    """Return (new_mappings, n_replaced) with migrated ODATA ids swapped for
    their ``r2:<key>``. Untouched keys/values are preserved exactly."""
    out: dict = {}
    n = 0
    for key, value in (mappings or {}).items():
        if key in SKIP_KEYS:
            out[key] = value
            continue
        if isinstance(value, list):
            new_list = []
            for el in value:
                if isinstance(el, str) and el in id_to_r2:
                    new_list.append(id_to_r2[el])
                    n += 1
                else:
                    new_list.append(el)
            out[key] = new_list
            continue
        if isinstance(value, str) and value in id_to_r2:
            out[key] = id_to_r2[value]
            n += 1
        else:
            out[key] = value
    return out, n


async def _resource_show(client: httpx.AsyncClient, odata_id: str) -> dict | None:
    url = f"{settings.odata_url}/api/3/action/resource_show"
    try:
        r = await client.get(url, params={"id": odata_id}, timeout=30)
        r.raise_for_status()
        body = r.json()
        if not body.get("success"):
            return None
        return body.get("result") or None
    except Exception as e:
        logger.warning("resource_show(%s) failed: %s", odata_id, e)
        return None


async def _download(client: httpx.AsyncClient, res: dict) -> bytes | None:
    """Download a resource's bytes. Prefer its file ``url``; for datastore-only
    resources fall back to the CKAN datastore dump."""
    odata_id = res.get("id")
    candidates = []
    if res.get("url"):
        candidates.append(res["url"])
    if res.get("datastore_active"):
        candidates.append(f"{settings.odata_url}/datastore/dump/{odata_id}")
    for u in candidates:
        try:
            r = await client.get(u, timeout=180, follow_redirects=True)
            if r.status_code == 200 and r.content:
                ctype = r.headers.get("content-type", "")
                if "text/html" in ctype and b"<html" in r.content[:200].lower():
                    continue
                return r.content
        except Exception as e:
            logger.warning("download %s failed: %s", str(u)[:80], e)
    return None


async def backfill_dataset_to_r2(
    db,
    ds_uuid: uuidlib.UUID,
    *,
    apply: bool,
    activate: bool,
) -> dict:
    """Run (or simulate) the ODATA→R2 migration for one dataset.

    Returns a JSON-serialisable summary. With ``apply=False`` nothing is
    uploaded or written (resource_show is still called to size the plan).
    """
    if not storage_client.is_configured():
        return {"error": "R2 is not configured (S3_* env missing)"}

    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_uuid)
    )).scalar_one_or_none()
    if not ds:
        return {"error": "dataset not found"}

    versions = list((await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds_uuid)
        .order_by(VersionIndex.version_number.asc())
    )).scalars().all())

    odata_ids = collect_odata_ids(versions)
    summary: dict = {
        "dataset_id": str(ds_uuid),
        "title": ds.title,
        "source_type": ds.source_type,
        "is_active": ds.is_active,
        "storage_target": storage.dataset_storage_target(ds),
        "versions": len(versions),
        "unique_odata_resources": len(odata_ids),
        "apply": apply,
        "activate": activate,
        "plan": [],
        "migrated": 0,
        "failed": [],
        "repointed_values": 0,
        "committed": False,
        "activated": False,
    }
    if not odata_ids:
        summary["note"] = "nothing to migrate (already on R2 or no files)"
        return summary

    # Process the unique resources CONCURRENTLY (bounded) — 55+ sequential
    # ODATA round-trips would otherwise blow past any reasonable request
    # timeout. Each task does resource_show (+ download+upload when applying).
    id_to_r2: dict[str, str] = {}
    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _process(client: httpx.AsyncClient, oid: str) -> dict:
        async with sem:
            res = await _resource_show(client, oid)
            if not res:
                return {"id": oid, "ok": False, "reason": "no resource_show"}
            name = res.get("name") or oid
            fmt = res.get("format") or ""
            key = backfill_key(str(ds_uuid), oid, name, fmt)
            entry = {"odata_id": oid, "name": name, "format": fmt,
                     "size": res.get("size"), "key": key}
            if not apply:
                return {"id": oid, "ok": True, "key": key, "plan": entry}
            data = await _download(client, res)
            if not data:
                return {"id": oid, "ok": False, "reason": "download failed",
                        "plan": entry}
            ctype = (
                "text/csv; charset=utf-8" if (fmt or "").lower() in TABULAR
                else (res.get("mimetype") or None)
            )
            await storage_client.upload_object(
                key, file_content=data, content_type=ctype,
            )
            return {"id": oid, "ok": True, "key": key, "plan": entry,
                    "uploaded": True}

    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            *(_process(client, oid) for oid in sorted(odata_ids))
        )
    # Reassemble in stable id order.
    for r in sorted(results, key=lambda x: x["id"]):
        if r.get("plan"):
            summary["plan"].append(r["plan"])
        if r["ok"]:
            id_to_r2[r["id"]] = storage.mark(r["key"])
            if r.get("uploaded"):
                summary["migrated"] += 1
        else:
            summary["failed"].append({"id": r["id"], "reason": r["reason"]})

    # Rewrite every version's mappings + metadata column.
    total_repl = 0
    for v in versions:
        new_map, n = rewrite_mappings(v.resource_mappings or {}, id_to_r2)
        new_meta = v.odata_metadata_resource_id
        meta_repl = 0
        if v.odata_metadata_resource_id in id_to_r2:
            new_meta = id_to_r2[v.odata_metadata_resource_id]
            meta_repl = 1
        total_repl += n + meta_repl
        if apply:
            v.resource_mappings = new_map
            v.odata_metadata_resource_id = new_meta
    summary["repointed_values"] = total_repl

    if apply and activate:
        sc = dict(ds.scraper_config or {})
        sc["storage_backend"] = "r2"
        ds.scraper_config = sc
        ds.is_active = True
        summary["activated"] = True

    if apply:
        await db.commit()
        summary["committed"] = True

    return summary


# ── post-migration repair / enrichment ──────────────────────────────────────
# After the bytes are on R2, two cosmetic/data-quality passes the UI depends on:
#   1. RECOVER dead refs: a version may still point at an ODATA id that was
#      deleted upstream (404). If that resource's CONTENT (its sha256 in
#      `_hashes`) is identical to one we DID migrate (same file appears in a
#      later surviving version), relink the dead value to that existing r2
#      object — recovering the historical bytes with zero new upload.
#   2. NAME capture: the mapping keys are opaque source UUIDs; the human name
#      lives only on the (soon-to-be-abandoned) ODATA resource. We resource_show
#      each migrated id ONCE, strip the "YYYY-MM-DD_HH-MM vN - " prefix, and
#      store a `_names` map {mapping_key -> clean name} the frontend renders.

# Leading archive-date/version prefix on every ODATA resource name, covering
# both "2026-05-03_10-43 v17 - ..." and "2026-01-24 - ..." (backfill) shapes.
_NAME_PREFIX_RE = re.compile(r"^\d{4}-\d{2}-\d{2}(_\d{2}-\d{2})?(\s+v\d+)?\s*-\s*")
# Trailing "(1282 שורות)" row-count suffix on backfill names.
_NAME_ROWS_SUFFIX_RE = re.compile(r"\s*\(\s*[\d,]+\s*שורות\s*\)\s*$")
_R2_ODATA_ID_RE = re.compile(r"/backfill/([0-9a-f-]{36})_", re.I)
# Date that prefixes every ODATA resource name (the archive date), e.g.
# "2026-05-03_10-43 v17 - ..." or "2026-01-24 - ... (1282 שורות)".
_NAME_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})")


def _clean_resource_name(name: str | None) -> str | None:
    if not name:
        return None
    s = _NAME_PREFIX_RE.sub("", name)
    s = _NAME_ROWS_SUFFIX_RE.sub("", s)
    return s.strip() or None


def _date_from_name(name: str | None) -> str | None:
    """The archive date (YYYY-MM-DD) embedded at the start of an ODATA resource
    name. This is the AUTHORITATIVE marker of which version a file belongs to —
    NOT the version number, which carry-forward/recovery can misattribute."""
    if not name:
        return None
    m = _NAME_DATE_RE.match(name)
    return m.group(1) if m else None


def _odata_id_from_r2(value: str) -> str | None:
    """Pull the original ODATA id back out of a backfill r2 key."""
    m = _R2_ODATA_ID_RE.search(value or "")
    return m.group(1) if m else None


def _iter_resource_keys(mappings: dict):
    """Yield (key, value, is_list_element_index) for every resource-bearing
    mapping entry (named, backfilled, _zip*, _geojson), skipping bookkeeping."""
    for key, value in (mappings or {}).items():
        if key in SKIP_KEYS:
            continue
        yield key, value


async def repair_dataset_r2(db, ds_uuid: uuidlib.UUID, *, apply: bool) -> dict:
    """Recover dead ODATA refs via content-hash match + capture friendly names.

    Idempotent. ``apply=False`` reports what it would do without writing.
    """
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_uuid)
    )).scalar_one_or_none()
    if not ds:
        return {"error": "dataset not found"}

    versions = list((await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds_uuid)
        .order_by(VersionIndex.version_number.asc())
    )).scalars().all())

    # 1. Build content-hash -> r2 value index from every surviving r2 mapping.
    #    The per-key sha256 lives in each version's `_hashes`.
    hash_to_r2: dict[str, str] = {}
    for v in versions:
        m = v.resource_mappings or {}
        hh = m.get("_hashes") or {}
        for key, value in _iter_resource_keys(m):
            if isinstance(value, str) and storage.is_storage_value(value):
                h = hh.get(key)
                if h and h != "download_failed":
                    hash_to_r2.setdefault(h, value)

    summary = {
        "dataset_id": str(ds_uuid), "title": ds.title,
        "versions": len(versions), "apply": apply,
        "recovered": 0, "unrecoverable": [], "named": 0, "dated": 0,
        "committed": False,
    }

    # 2. Collect the unique ODATA ids we still need names/dates for, then
    #    resource_show each once (cached), concurrently.
    name_cache: dict[str, str | None] = {}
    date_cache: dict[str, str | None] = {}
    odata_ids_for_names: set[str] = set()
    for v in versions:
        for key, value in _iter_resource_keys(v.resource_mappings or {}):
            if isinstance(value, str) and storage.is_storage_value(value):
                oid = _odata_id_from_r2(value)
                if oid:
                    odata_ids_for_names.add(oid)

    sem = asyncio.Semaphore(MAX_CONCURRENCY)

    async def _fetch_name(client, oid):
        async with sem:
            res = await _resource_show(client, oid)
            raw = res.get("name") if res else None
            name_cache[oid] = _clean_resource_name(raw)
            date_cache[oid] = _date_from_name(raw)

    async with httpx.AsyncClient() as client:
        await asyncio.gather(*(_fetch_name(client, o) for o in odata_ids_for_names))

    # 3. Per version: recover dead refs + build _names + build _filedates.
    for v in versions:
        m = dict(v.resource_mappings or {})
        hh = m.get("_hashes") or {}
        names: dict[str, str] = {}
        filedates: dict[str, str] = {}
        changed = False

        for key, value in list(_iter_resource_keys(m)):
            # recover a dead bare-ODATA value via hash match
            if isinstance(value, str) and _UUID_RE.match(value) and not storage.is_storage_value(value):
                h = hh.get(key)
                repl = hash_to_r2.get(h) if h else None
                if repl:
                    m[key] = repl
                    value = repl
                    changed = True
                    summary["recovered"] += 1
                else:
                    summary["unrecoverable"].append(
                        {"version": v.version_number, "key": key}
                    )
            # name + date capture (for r2 values, incl. just-recovered ones)
            if isinstance(value, str) and storage.is_storage_value(value):
                oid = _odata_id_from_r2(value)
                nm = name_cache.get(oid) if oid else None
                if nm:
                    names[key] = nm
                dt = date_cache.get(oid) if oid else None
                if dt:
                    filedates[key] = dt

        if names:
            m["_names"] = names
            changed = True
            summary["named"] += len(names)
        if filedates:
            m["_filedates"] = filedates
            changed = True
            summary["dated"] += len(filedates)

        if apply and changed:
            v.resource_mappings = m

    if apply:
        await db.commit()
        summary["committed"] = True

    return summary


# ── clean rebuild of a dataset's version history ─────────────────────────────
# For datasets whose source publishes ONE live "updating" resource plus many
# same-titled static archive copies (e.g. residents-by-town), the per-poll
# carry-forward + full-snapshot captures produce noisy versions (the same file
# shown many times, opaque names, version numbers tied to poll order). This
# rebuilds the history into a clean, deduplicated, date-ordered series:
#   * one version per DISTINCT file (deduped by content sha256, or by date for
#     hash-less backfill snapshots) — no file appears twice;
#   * versions renumbered 1..N strictly by date (oldest first);
#   * each file labelled "<source title> — DD.MM.YYYY";
#   * going forward, the dataset tracks ONLY the live resource (one file/version).
#
# Destructive: it deletes the dataset's VersionIndex rows and recreates them.
# The R2 objects (the bytes) are untouched — only the index is rebuilt. Always
# dry-run first.

_LIVE_NAME_HINT = "מתעדכן"


def _fmt_ddmmyyyy(date_str: str) -> str:
    try:
        y, m, d = date_str.split("-")
        return f"{d}.{m}.{y}"
    except Exception:
        return date_str


async def rebuild_dataset_versions(db, ds_uuid: uuidlib.UUID, *, apply: bool) -> dict:
    """Rebuild a dataset's version history clean + deduped + date-numbered.

    Returns the proposed (or applied) timeline. ``apply=False`` changes nothing.
    """
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_uuid)
    )).scalar_one_or_none()
    if not ds:
        return {"error": "dataset not found"}

    versions = list((await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds_uuid)
        .order_by(VersionIndex.version_number.asc())
    )).scalars().all())

    # 1. Live resource id (data.gov.il source) for forward tracking.
    live_id: str | None = None
    try:
        url = f"{settings.data_gov_il_url}/api/3/action/package_show"
        async with httpx.AsyncClient() as client:
            r = await client.get(url, params={"id": ds.ckan_id}, timeout=40)
            r.raise_for_status()
            for x in (r.json().get("result", {}) or {}).get("resources", []) or []:
                nm = x.get("name") or ""
                if _LIVE_NAME_HINT in nm and (x.get("format") or "").upper() == "CSV":
                    live_id = x["id"]
                    break
    except Exception as e:
        return {"error": f"failed to fetch source package: {e}"}

    # 2. The ODATA mirror is the authoritative record of WHAT was archived WHEN:
    #    every resource name carries its real archive date (all 2026) + vN. Map
    #    the original odata resource id -> (date, clean name, is_live), skipping
    #    the metadata JSON. We date snapshots from here, NOT from data.gov.il's
    #    resource `created` (which is 2016-2020 — when the publisher first posted
    #    the file, not when OVER captured it; mixing those produced a date salad).
    odata: dict[str, dict] = {}
    if ds.odata_dataset_id:
        try:
            url = f"{settings.odata_url}/api/3/action/package_show"
            async with httpx.AsyncClient() as client:
                r = await client.get(url, params={"id": ds.odata_dataset_id}, timeout=40)
                r.raise_for_status()
                for x in (r.json().get("result", {}) or {}).get("resources", []) or []:
                    if (x.get("format") or "").upper() == "JSON":
                        continue
                    nm = x.get("name") or ""
                    odata[x["id"]] = {
                        "date": _date_from_name(nm),
                        "name": _clean_resource_name(nm),
                        "is_live": _LIVE_NAME_HINT in nm,
                    }
        except Exception as e:
            return {"error": f"failed to fetch ODATA mirror: {e}"}

    # 3. Gather one snapshot per ARCHIVE DATE (all 2026). Each r2 object embeds
    #    its original odata id; date + name come from the ODATA record above.
    #    Dedup by date guarantees no file appears twice and collapses the
    #    same-day static-archive copies into the live "updating" file (preferred).
    snaps: dict[str, dict] = {}  # date -> entry
    live_hash: str | None = None  # latest live content hash (forward detection)

    def _score(e: dict) -> int:
        return 1 if e["is_live"] else 0

    def _add(date_str, base_name, r2, is_live):
        if not date_str:
            return
        cand = {"date": date_str, "name": base_name or "", "r2": r2, "is_live": is_live}
        prev = snaps.get(date_str)
        if prev is None or _score(cand) > _score(prev):
            snaps[date_str] = cand

    for v in versions:
        m = v.resource_mappings or {}
        hh = m.get("_hashes") or {}
        if live_id and hh.get(live_id) and hh.get(live_id) != "download_failed":
            live_hash = hh[live_id]  # versions are asc, so the last seen wins
        for key, value in _iter_resource_keys(m):
            if not (isinstance(value, str) and storage.is_storage_value(value)):
                continue
            oid = _odata_id_from_r2(value)
            o = odata.get(oid) if oid else None
            if not o or not o.get("date"):
                continue
            _add(o["date"], o["name"], value, o["is_live"])

    ordered = sorted(snaps.values(), key=lambda e: e["date"])

    timeline = []
    for i, e in enumerate(ordered, 1):
        timeline.append({
            "version": i, "date": e["date"], "is_live": e["is_live"],
            "label": f"{e['name']} — {_fmt_ddmmyyyy(e['date'])}",
            "r2": e["r2"],
        })

    summary = {
        "dataset_id": str(ds_uuid), "title": ds.title, "apply": apply,
        "live_resource_id": live_id,
        "old_version_count": len(versions),
        "new_version_count": len(ordered),
        "timeline": timeline,
        "committed": False,
    }
    if not apply:
        return summary
    if live_id is None:
        return {**summary, "error": "live resource not found — refusing to apply"}

    # 3. Apply: delete old rows, recreate clean ones, retrack the live resource.
    for v in versions:
        await db.delete(v)
    await db.flush()

    n = len(ordered)
    for i, e in enumerate(ordered, 1):
        is_latest = (i == n)
        # All snapshots are the one logical residents resource over time, so the
        # mapping key is the live source id throughout (keeps forward carry-
        # forward + change-detection consistent on the latest row).
        key = live_id
        label = f"{e['name']} — {_fmt_ddmmyyyy(e['date'])}"
        dt = datetime.fromisoformat(e["date"] + "T12:00:00+00:00")
        mappings = {
            key: e["r2"],
            "_names": {key: label},
            "_filedates": {key: e["date"]},
            # Only the latest row feeds forward change-detection; give it the
            # live resource's last-known hash + tracked id so the next poll
            # diffs cleanly (empty is fine — it just re-downloads once).
            "_hashes": ({live_id: live_hash} if (is_latest and live_hash) else {}),
            "_resource_ids": ([live_id] if is_latest else []),
        }
        db.add(VersionIndex(
            tracked_dataset_id=ds_uuid,
            version_number=i,
            metadata_modified=(ds.last_modified or e["date"]) if is_latest else e["date"],
            detected_at=dt,
            odata_metadata_resource_id=None,
            change_summary={
                "type": "rebuilt", "date": e["date"], "is_live": e["is_live"],
                "resources_added": [], "resources_removed": [], "resources_modified": [],
            },
            resource_mappings=mappings,
            source="legacy",
        ))

    # Track ONLY the live resource going forward (one file per future version).
    ds.resource_ids = [live_id]
    ds.resource_probes = None  # reset probe baseline for the new tracked set

    await db.commit()
    summary["committed"] = True
    return summary


# ── retroactive NEON seed from existing per-version R2 snapshots ──────────────
# A full-snapshot dataset already has its history as per-version CSVs on R2. To
# ALSO make it queryable as a NEON append table (with the same `first_seen`/SEEN
# column as the live append datasets), we replay those snapshots oldest→newest
# into the NEON table: keyless full-row-hash dedup so every distinct row STATE is
# captured once, stamped with the DATE OF THE VERSION it first appeared in (not
# now()). Then flip the dataset to the r2+neon plan so future polls dual-write
# (R2 file snapshot + NEON rows) automatically (delta_archiver._archive_streaming_to_db).
#
# Forward dedup consistency: the latest version's CSV columns equal the live
# datastore columns, so a row the seed inserted from the newest snapshot hashes
# identically to the same row the forward poll reads — no duplicate on the next
# poll. Older versions with a different column set (e.g. an age-bucket rename)
# hash in their own space and are kept as distinct historical states.

async def seed_neon_from_versions(db, ds_uuid: uuidlib.UUID, *, apply: bool) -> dict:
    """Replay a dataset's per-version R2 CSV snapshots into its NEON append table
    with historical ``first_seen``, then enable the r2+neon dual-write going
    forward. Idempotent (ON CONFLICT DO NOTHING). ``apply=False`` reports the plan
    without writing to NEON or changing config."""
    from app.services import append_store
    from app.services.csv_parser import parse_csv

    if not append_store.is_configured():
        return {"error": "NEON append DB is not configured (APPEND_DATABASE_URL missing)"}

    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == ds_uuid)
    )).scalar_one_or_none()
    if not ds:
        return {"error": "dataset not found"}

    versions = list((await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds_uuid)
        .order_by(VersionIndex.version_number.asc())  # oldest first → earliest first_seen wins
    )).scalars().all())

    table = append_store.table_name(ds)
    summary = {
        "dataset_id": str(ds_uuid), "title": ds.title, "apply": apply,
        "table": table, "versions": len(versions),
        "rows_inserted": 0, "per_version": [], "skipped": [],
        "archive_neon_enabled": False, "table_total": None, "committed": False,
    }

    for v in versions:
        m = v.resource_mappings or {}
        # the single data file for this version (snapshot key → r2 value)
        r2val = None
        for k, val in m.items():
            if k.startswith("_"):
                continue
            if isinstance(val, str) and storage.is_storage_value(val):
                r2val = val
                break
        if not r2val:
            summary["skipped"].append({"version": v.version_number, "reason": "no r2 file"})
            continue
        # first_seen = the version's archive date (the date it represents)
        fs = v.detected_at.astimezone(timezone.utc).isoformat()
        data = await storage_client.get_object_bytes(r2val)
        if not data:
            summary["skipped"].append({"version": v.version_number, "reason": "download failed"})
            continue
        try:
            fields, records = parse_csv(data)
        except Exception as e:
            summary["skipped"].append({"version": v.version_number, "reason": f"parse: {e}"})
            continue
        cols = [f["id"] for f in fields]
        n = 0
        if apply and records and cols:
            try:
                await append_store.ensure_table(table, cols, key_col=None, keyless=True)
                n = await append_store.append_rows(
                    table, cols, records, key_col=None, keyless=True, first_seen=fs,
                )
                summary["rows_inserted"] += n
            except Exception as e:
                logger.exception("seed_neon: version %d insert failed", v.version_number)
                summary["skipped"].append(
                    {"version": v.version_number, "reason": f"insert: {type(e).__name__}: {e}"}
                )
                continue
        summary["per_version"].append({
            "version": v.version_number, "date": fs[:10],
            "rows_in_file": len(records), "inserted": n,
        })

    if apply:
        # Enable r2+neon dual-write going forward (keep file snapshots on R2,
        # also stream rows to NEON on each future poll).
        sc = dict(ds.scraper_config or {})
        sc["archive_neon"] = True
        ds.scraper_config = sc
        summary["archive_neon_enabled"] = True
        try:
            summary["table_total"] = await append_store.table_count(table)
        except Exception:
            pass
        await db.commit()
        summary["committed"] = True

    return summary
