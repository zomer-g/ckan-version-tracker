#!/usr/bin/env python3
"""One-off backfill: copy a CKAN dataset's full version history from the ODATA
mirror onto Cloudflare R2, then repoint every version at the R2 objects.

Why
---
OVER's CKAN datasets archive their files to the odata.org.il CKAN mirror. We're
abandoning ODATA for an independent object store (R2 — see
app/services/storage_client.py). The forward path (poll_job +
create_version_snapshot) is now R2-capable, but the *existing* history still
lives on ODATA. This script moves those bytes.

What it does NOT touch
----------------------
- ``detected_at`` / ``metadata_modified`` / ``change_summary`` — the dates and
  metadata already live in Postgres (version_index). Nothing to migrate; they
  are preserved verbatim.
- The ODATA originals — left in place as a backup. This only ADDS R2 copies and
  rewrites the mapping VALUES.

Mechanics
---------
1. Collect every UNIQUE ODATA mirror resource_id referenced across all versions:
   the named ``source_id -> odata_id`` mappings, ``backfilled``, ``_zip`` /
   ``_zip_parts`` / ``_geojson``, and each version's ``odata_metadata_resource_id``
   column. We deliberately EXCLUDE ``_resource_ids`` and ``_hashes`` — those hold
   data.gov.il SOURCE ids (and hashes), not ODATA mirror ids. The conditional
   archiver carries the same odata_id forward across many versions, so the same
   id appears repeatedly — we download/upload it ONCE.
2. For each unique id: ODATA ``resource_show`` → download the file → upload to R2
   under a DETERMINISTIC key (``datasets/<ds>/backfill/<odata_id>_<name>``) so a
   re-run overwrites rather than duplicates.
3. Rewrite every version's ``resource_mappings`` (and the metadata column),
   replacing each migrated ODATA id with its ``r2:<key>`` marker. Already-``r2:``
   values are skipped, so the script is idempotent.

Safety
------
- DRY-RUN by default: prints the plan, uploads nothing, writes nothing. Pass
  ``--apply`` to actually upload to R2 and commit the DB rewrite.
- ``--activate`` (with ``--apply``) also flips the dataset to the R2 backend
  (``scraper_config.storage_backend=r2``) and re-activates it (``is_active=true``)
  so future polls archive straight to R2.

Requires DATABASE_URL + S3_* env (same as the app). Run from the repo root:

    python -m scripts.backfill_dataset_to_r2 <dataset_uuid> [--apply] [--activate]
"""
from __future__ import annotations

import argparse
import asyncio
import re
import sys
import uuid as uuidlib

import httpx
from sqlalchemy import select

from app.config import settings
from app.database import async_session
from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import storage_client as storage
from app.services.storage_client import storage_client

DEFAULT_DATASET = "81c9013d-a167-41f5-b71a-fcc8d6dd592d"

_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I
)

# resource_mappings keys whose values are NOT ODATA mirror ids (they're
# data.gov.il source ids / hashes / bookkeeping) — never migrate these.
SKIP_KEYS = {"_hashes", "_resource_ids", "_appendonly_seen", "_large_dataset_info"}


def _looks_like_odata_id(v) -> bool:
    """A migratable ODATA mirror id: a bare UUID string, not already r2:-marked
    and not a sentinel (``download_failed`` etc.)."""
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
                # _zip_parts / _geojson — lists of odata ids
                for el in value:
                    if _looks_like_odata_id(el):
                        ids.add(el)
                continue
            if _looks_like_odata_id(value):
                ids.add(value)
        if _looks_like_odata_id(v.odata_metadata_resource_id):
            ids.add(v.odata_metadata_resource_id)
    return ids


def _safe(name: str) -> str:
    return storage._safe_filename(name)  # reuse the app's key sanitiser


def _backfill_key(ds_id: str, odata_id: str, name: str, fmt: str) -> str:
    """Deterministic R2 key for a migrated ODATA resource. Deterministic so a
    re-run overwrites the same object instead of leaking a new one."""
    ext = (fmt or "dat").lower().strip().lstrip(".") or "dat"
    base = _safe(name) or "file"
    return f"datasets/{ds_id}/backfill/{odata_id}_{base}.{ext}"


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
        print(f"    resource_show({odata_id}) failed: {e}", file=sys.stderr)
        return None


async def _download(client: httpx.AsyncClient, res: dict) -> bytes | None:
    """Download a resource's bytes. Prefer its file ``url``; for datastore-only
    resources with no usable file, fall back to the CKAN datastore dump."""
    odata_id = res.get("id")
    candidates = []
    if res.get("url"):
        candidates.append(res["url"])
    if res.get("datastore_active"):
        candidates.append(f"{settings.odata_url}/datastore/dump/{odata_id}")
    for u in candidates:
        try:
            r = await client.get(u, timeout=120, follow_redirects=True)
            if r.status_code == 200 and r.content:
                # A CKAN HTML error page is not a file.
                ctype = r.headers.get("content-type", "")
                if "text/html" in ctype and b"<html" in r.content[:200].lower():
                    continue
                return r.content
        except Exception as e:
            print(f"    download {u[:80]} failed: {e}", file=sys.stderr)
    return None


def _rewrite_mappings(mappings: dict, id_to_r2: dict[str, str]) -> tuple[dict, int]:
    """Return (new_mappings, n_replaced) with migrated ODATA ids swapped for
    their r2:<key>. Untouched keys/values are preserved exactly."""
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


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("dataset", nargs="?", default=DEFAULT_DATASET,
                    help="OVER tracked_dataset UUID")
    ap.add_argument("--apply", action="store_true",
                    help="actually upload to R2 and commit the DB rewrite "
                         "(default: dry-run)")
    ap.add_argument("--activate", action="store_true",
                    help="with --apply: set storage_backend=r2 and is_active=true")
    args = ap.parse_args()

    if not storage_client.is_configured():
        print("ERROR: R2 is not configured (S3_* env missing). Aborting.",
              file=sys.stderr)
        return 2

    ds_uuid = uuidlib.UUID(args.dataset)
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"=== Backfill dataset {ds_uuid} to R2 [{mode}] ===")
    print(f"R2 bucket={settings.s3_bucket}  public={settings.s3_public_base_url}")

    async with async_session() as db:
        ds = (await db.execute(
            select(TrackedDataset).where(TrackedDataset.id == ds_uuid)
        )).scalar_one_or_none()
        if not ds:
            print("ERROR: dataset not found", file=sys.stderr)
            return 2
        versions = list((await db.execute(
            select(VersionIndex)
            .where(VersionIndex.tracked_dataset_id == ds_uuid)
            .order_by(VersionIndex.version_number.asc())
        )).scalars().all())

        print(f"Dataset: {ds.title!r}  source_type={ds.source_type}  "
              f"is_active={ds.is_active}  storage={storage.dataset_storage_target(ds)}")
        print(f"Versions: {len(versions)} "
              f"(v{versions[0].version_number}..v{versions[-1].version_number})"
              if versions else "Versions: 0")

        odata_ids = collect_odata_ids(versions)
        print(f"Unique ODATA resources to migrate: {len(odata_ids)}")
        if not odata_ids:
            print("Nothing to migrate (already on R2 or no files).")
            return 0

        # 1) Download each unique ODATA resource once, upload to R2.
        id_to_r2: dict[str, str] = {}
        failures: list[str] = []
        async with httpx.AsyncClient() as client:
            for i, oid in enumerate(sorted(odata_ids), 1):
                res = await _resource_show(client, oid)
                if not res:
                    failures.append(oid)
                    print(f"[{i}/{len(odata_ids)}] {oid}  SKIP (no resource_show)")
                    continue
                name = res.get("name") or oid
                fmt = res.get("format") or ""
                key = _backfill_key(str(ds_uuid), oid, name, fmt)
                size = res.get("size")
                print(f"[{i}/{len(odata_ids)}] {oid}  {fmt:5} "
                      f"size={size}  -> {key}")
                if not args.apply:
                    id_to_r2[oid] = storage.mark(key)
                    continue
                data = await _download(client, res)
                if not data:
                    failures.append(oid)
                    print(f"      DOWNLOAD FAILED — left on ODATA")
                    continue
                ctype = (
                    "text/csv; charset=utf-8"
                    if (fmt or "").lower() in ("csv", "tsv", "txt")
                    else (res.get("mimetype") or None)
                )
                await storage_client.upload_object(
                    key, file_content=data, content_type=ctype,
                )
                id_to_r2[oid] = storage.mark(key)
                print(f"      uploaded {len(data)} bytes to R2")

        print(f"\nMigrated {len(id_to_r2)}/{len(odata_ids)} resources; "
              f"{len(failures)} failed.")

        # 2) Rewrite every version's mappings + metadata column.
        total_repl = 0
        for v in versions:
            new_map, n = _rewrite_mappings(v.resource_mappings or {}, id_to_r2)
            meta_repl = 0
            new_meta = v.odata_metadata_resource_id
            if v.odata_metadata_resource_id in id_to_r2:
                new_meta = id_to_r2[v.odata_metadata_resource_id]
                meta_repl = 1
            if n or meta_repl:
                total_repl += n + meta_repl
                print(f"  v{v.version_number}: {n} mapping(s)"
                      + (f" + metadata" if meta_repl else "") + " -> R2")
            if args.apply:
                v.resource_mappings = new_map
                v.odata_metadata_resource_id = new_meta
        print(f"Total mapping values repointed: {total_repl}")

        # 3) Optionally flip the dataset to R2 + reactivate.
        if args.apply and args.activate:
            sc = dict(ds.scraper_config or {})
            sc["storage_backend"] = "r2"
            ds.scraper_config = sc
            ds.is_active = True
            print("Set scraper_config.storage_backend=r2 and is_active=true")

        if args.apply:
            await db.commit()
            print("\nCOMMITTED.")
        else:
            print("\nDRY-RUN — no uploads, no DB changes. Re-run with --apply.")

        if failures:
            print(f"\nWARNING: {len(failures)} ODATA resources could not be "
                  f"migrated and were left pointing at ODATA:")
            for f in failures:
                print(f"  - {f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
