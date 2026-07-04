"""Independent object-storage backend (Cloudflare R2 / any S3-compatible).

Why this exists
---------------
Historically every file OVER archives — attachment ZIPs (hundreds of GB of
documents), GeoJSON, CSVs — is pushed to the CKAN mirror at odata.org.il via
``resource_create`` (see ``odata_client.upload_resource``). That couples our
file storage to a third-party CKAN instance and means every public download
flows through it.

This module is the first step of decoupling: a thin async wrapper around an
S3-compatible object store (Cloudflare R2 by default — zero egress fees, so a
public download site stays cheap and predictable regardless of traffic).

Key design points
------------------
* **Direct downloads.** Files are served straight from the store's public
  custom domain (``S3_PUBLIC_BASE_URL``, e.g. ``https://files.over.org.il``).
  The OVER backend only ever issues a 302 redirect to that URL — the file
  bytes never proxy through our dyno (no egress cost, no load).
* **Default backend = R2.** ``STORAGE_BACKEND`` defaults to ``r2`` — every
  dataset not explicitly pinned otherwise is archived as a full independent
  snapshot on R2. ``is_enabled()`` is True once the S3 credentials are present;
  if they're missing, ``is_configured()`` is False and callers fall back to the
  ODATA path rather than erroring. Set ``STORAGE_BACKEND=odata`` to revert the
  global default.
* **Marker convention.** A resource stored here is recorded in a version's
  ``resource_mappings`` as the string ``"r2:<object-key>"`` (see
  ``mark`` / ``is_storage_value`` / ``key_of``). That lets the download,
  delete and size paths tell an R2 object apart from a legacy ODATA
  resource_id (a bare UUID) without a schema change.

boto3 is synchronous; every network call is wrapped in ``asyncio.to_thread``
so it never blocks the event loop.
"""
import asyncio
import logging
import re
import uuid
from typing import Any

from app.config import settings

logger = logging.getLogger(__name__)

# Marker prefix stamped onto resource_mappings values that point at an object
# in this store (rather than at an ODATA resource_id). Kept deliberately short
# and distinctive — a legacy ODATA value is a bare UUID, never starts with
# "r2:".
STORAGE_PREFIX = "r2:"


def mark(key: str) -> str:
    """Wrap an object key as a resource_mappings value (``r2:<key>``)."""
    return f"{STORAGE_PREFIX}{key}"


def is_storage_value(value: Any) -> bool:
    """True if ``value`` is an R2-marked mapping value (``r2:...``)."""
    return isinstance(value, str) and value.startswith(STORAGE_PREFIX)


def key_of(value: str) -> str:
    """Strip the ``r2:`` marker, returning the bare object key."""
    return value[len(STORAGE_PREFIX):] if is_storage_value(value) else value


_SAFE_KEY_PART = re.compile(r"[^A-Za-z0-9._\-]+")


def _safe_filename(filename: str) -> str:
    """Make a filename safe to embed in an object key.

    Object keys are also URL path segments served from the public domain, so
    we keep ASCII-clean characters and collapse everything else to ``_``.
    Hebrew/space-laden names round-trip poorly through CDNs and command-line
    download tools, so a stable ASCII key is preferable; the human-readable
    name lives in the DB/UI, not the storage key.
    """
    name = (filename or "file").strip().replace("/", "_").replace("\\", "_")
    name = _SAFE_KEY_PART.sub("_", name).strip("._-")
    return name or "file"


def build_key(dataset_id: str, version_number: int, filename: str) -> str:
    """Canonical object-key scheme:
    ``datasets/<dataset>/v<n>/<rand8>_<filename>``.

    The ``v<n>`` segment groups a dataset's history under one prefix (handy
    for listing / lifecycle rules / a future bulk migration). The random
    8-char component guarantees uniqueness: the worker pre-uploads ZIP/CSV/
    GeoJSON with a *placeholder* ``version_number=1`` (it can't know the real
    next_version yet), so without it two different versions would derive the
    same key and the later upload would silently overwrite the earlier
    version's file. Each object is recorded by its full key in
    ``resource_mappings``, so a fresh random key per upload is exactly right.
    """
    unique = uuid.uuid4().hex[:8]
    return f"datasets/{dataset_id}/v{version_number}/{unique}_{_safe_filename(filename)}"


# Mapping keys that are internal bookkeeping, not downloadable files.
_NON_FILE_KEYS = {"_hashes", "_resource_ids", "_filedates", "_probes"}
# A staged-object key's tail looks like ``<8 hex>_<original-name>`` (see
# build_key). This strips the random prefix back to a human filename.
_KEY_PREFIX_RE = re.compile(r"^[0-9a-f]{8}_(.+)$")


def _filename_from_value(value: str, fallback: str) -> str:
    """Best-effort human filename for a stored file. For an R2 value we
    recover the original name from the object key tail; for a bare ODATA
    resource_id (a UUID, no name available here) we use ``fallback``."""
    if is_storage_value(value):
        tail = key_of(value).rsplit("/", 1)[-1]
        m = _KEY_PREFIX_RE.match(tail)
        name = (m.group(1) if m else tail) or fallback
        # The CSV is stored under the key tail "csv" (no extension) — give
        # it one so it opens correctly in Drive.
        if name.lower() == "csv":
            name = "data.csv"
        return name
    return fallback


def enumerate_files(mappings: dict | None) -> list[tuple[str, str]]:
    """Flatten a version's ``resource_mappings`` into an ordered, de-duplicated
    list of ``(filename, storage_value)`` pairs — every downloadable file in
    the version, ready to be pushed to Drive.

    ``storage_value`` is either an ``r2:<key>`` marker or a bare ODATA
    resource_id (UUID). Internal bookkeeping keys (``_hashes`` etc.) and
    non-file scalars are skipped. Named resources are walked before the
    underscore-prefixed aggregate lists (``_zip_parts`` …) so a file that
    appears in both keeps its nicer name; the dedupe drops the second copy.
    """
    if not mappings:
        return []
    out: list[tuple[str, str]] = []
    seen: set[str] = set()

    def _is_file(v: object) -> bool:
        return isinstance(v, str) and bool(v) and (
            is_storage_value(v) or len(v) >= 30
        )

    # Named (human) keys first, then the underscore aggregates.
    ordered = sorted(mappings.items(), key=lambda kv: kv[0].startswith("_"))
    for key, value in ordered:
        if key in _NON_FILE_KEYS:
            continue
        if isinstance(value, list):
            idx = 0
            for v in value:
                if not _is_file(v) or v in seen:
                    continue
                seen.add(v)
                idx += 1
                out.append((_filename_from_value(v, f"{key.lstrip('_')}-{idx}"), v))
        elif _is_file(value):
            if value in seen:
                continue
            seen.add(value)
            out.append((_filename_from_value(value, key.lstrip("_") or "file"), value))
    return out


class StorageClient:
    """Async wrapper around an S3-compatible object store (Cloudflare R2)."""

    def __init__(self) -> None:
        self._client = None  # lazily created boto3 S3 client

    # ── configuration ───────────────────────────────────────────────────

    def is_configured(self) -> bool:
        """True when R2 credentials are fully present — i.e. R2 *can* be used,
        regardless of the global default. Per-dataset routing decides whether a
        given dataset actually goes to R2; this only answers "are we able to?".
        Used by the read/delete/size paths (which must work for any object
        already stored in R2) and by per-dataset upload routing.
        """
        return (
            bool(settings.s3_endpoint)
            and bool(settings.s3_bucket)
            and bool(settings.s3_access_key)
            and bool(settings.s3_secret_key)
            and bool(settings.s3_public_base_url)
        )

    def is_enabled(self) -> bool:
        """True when R2 is the *global default* backend AND configured. This is
        the fallback for datasets that don't pin a per-dataset choice. Missing
        any credential ⇒ disabled (fall back to ODATA rather than erroring).
        """
        return settings.storage_backend == "r2" and self.is_configured()

    def public_url(self, key_or_value: str) -> str:
        """Public download URL for an object key (or an ``r2:``-marked value).

        Built from ``S3_PUBLIC_BASE_URL`` so downloads hit the store's CDN/
        custom domain directly, never the OVER backend.
        """
        key = key_of(key_or_value)
        base = settings.s3_public_base_url.rstrip("/")
        return f"{base}/{key.lstrip('/')}"

    # ── lazy boto3 client ───────────────────────────────────────────────

    def _get_client(self):
        if self._client is None:
            # Imported lazily so the dependency is only needed when the R2
            # backend is actually used (keeps `odata`-only deploys and the
            # unit tests free of a hard boto3 import).
            import boto3
            from botocore.config import Config

            self._client = boto3.client(
                "s3",
                endpoint_url=settings.s3_endpoint,
                aws_access_key_id=settings.s3_access_key,
                aws_secret_access_key=settings.s3_secret_key,
                region_name=settings.s3_region or "auto",
                config=Config(
                    signature_version="s3v4",
                    retries={"max_attempts": 4, "mode": "standard"},
                ),
            )
        return self._client

    # ── operations ──────────────────────────────────────────────────────

    async def upload_object(
        self,
        key: str,
        *,
        file_content: bytes | None = None,
        file_path: str | None = None,
        content_type: str | None = None,
    ) -> str:
        """Upload bytes (``file_content``) or a file on disk (``file_path``)
        to ``key``. Returns the bare object key on success.

        ``file_path`` is preferred for large files — boto3's managed transfer
        streams from disk in parts (constant memory even for multi-GB ZIPs).
        """
        if file_content is None and file_path is None:
            raise ValueError("upload_object: need file_content or file_path")
        if not self.is_configured():
            raise RuntimeError("R2 storage is not configured")

        extra: dict[str, Any] = {}
        if content_type:
            extra["ContentType"] = content_type

        def _do() -> None:
            client = self._get_client()
            if file_path is not None:
                client.upload_file(
                    file_path, settings.s3_bucket, key,
                    ExtraArgs=extra or None,
                )
            else:
                client.put_object(
                    Bucket=settings.s3_bucket, Key=key, Body=file_content,
                    **extra,
                )

        await asyncio.to_thread(_do)
        logger.info("Uploaded object to R2: %s", key)
        return key

    # ── presigned multipart (direct worker→R2 uploads) ──────────────────
    # Multi-GB scraper outputs (GovMap heavy layers: 3.6GB CSV, 3.9GB GeoJSON)
    # cannot travel through the OVER dyno: over.org.il sits behind Cloudflare
    # and the giant POST destabilises the dyno (502s that also starve task
    # heartbeats). Instead the server only ORCHESTRATES: it creates an S3
    # multipart upload and presigns per-part PUT URLs against the R2 S3
    # endpoint — the worker PUTs the bytes straight to R2, then asks the
    # server to complete. The file bytes never touch OVER.

    async def create_multipart(self, key: str, content_type: str | None = None) -> str:
        """Start a multipart upload for ``key``; returns the S3 UploadId."""
        if not self.is_configured():
            raise RuntimeError("R2 storage is not configured")

        def _do() -> str:
            client = self._get_client()
            kwargs: dict[str, Any] = {"Bucket": settings.s3_bucket, "Key": key}
            if content_type:
                kwargs["ContentType"] = content_type
            return client.create_multipart_upload(**kwargs)["UploadId"]

        return await asyncio.to_thread(_do)

    async def presign_part(self, key: str, upload_id: str, part_number: int,
                           expires_s: int = 7200) -> str:
        """Presigned PUT URL for one part (1-based part_number)."""
        if not self.is_configured():
            raise RuntimeError("R2 storage is not configured")

        def _do() -> str:
            client = self._get_client()
            return client.generate_presigned_url(
                "upload_part",
                Params={
                    "Bucket": settings.s3_bucket, "Key": key,
                    "UploadId": upload_id, "PartNumber": part_number,
                },
                ExpiresIn=expires_s,
            )

        return await asyncio.to_thread(_do)

    async def complete_multipart(self, key: str, upload_id: str,
                                 parts: list[dict]) -> None:
        """Finish a multipart upload. ``parts`` = [{"PartNumber": n, "ETag": e}]."""
        if not self.is_configured():
            raise RuntimeError("R2 storage is not configured")

        def _do() -> None:
            client = self._get_client()
            client.complete_multipart_upload(
                Bucket=settings.s3_bucket, Key=key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )

        await asyncio.to_thread(_do)
        logger.info("Completed multipart upload to R2: %s (%d parts)",
                    key, len(parts))

    async def abort_multipart(self, key: str, upload_id: str) -> None:
        """Abort a multipart upload (frees R2's stored parts). Best-effort."""
        if not self.is_configured():
            return

        def _do() -> None:
            client = self._get_client()
            try:
                client.abort_multipart_upload(
                    Bucket=settings.s3_bucket, Key=key, UploadId=upload_id)
            except Exception:  # noqa: BLE001
                logger.warning("abort_multipart failed for %s", key, exc_info=True)

        await asyncio.to_thread(_do)

    async def delete_object(self, key_or_value: str) -> None:
        """Delete an object by key (or ``r2:``-marked value). Idempotent —
        deleting a missing key is not an error on S3."""
        if not self.is_configured():
            raise RuntimeError("R2 storage is not configured")
        key = key_of(key_or_value)

        def _do() -> None:
            client = self._get_client()
            client.delete_object(Bucket=settings.s3_bucket, Key=key)

        await asyncio.to_thread(_do)
        logger.info("Deleted object from R2: %s", key)

    async def download_to_file(self, key_or_value: str, dest_path: str) -> bool:
        """Stream an object straight to a file on disk (constant memory via
        boto3's managed transfer). Returns True on success, False if the
        object is missing / unreachable. Used by the Drive-export runner to
        stage one file at a time before uploading it to Google Drive."""
        if not self.is_configured():
            return False
        key = key_of(key_or_value)

        def _do() -> bool:
            client = self._get_client()
            try:
                client.download_file(settings.s3_bucket, key, dest_path)
                return True
            except Exception:
                logger.exception("R2 download_to_file failed for key %s", key)
                return False

        return await asyncio.to_thread(_do)

    async def get_object_bytes(self, key_or_value: str) -> bytes | None:
        """Download an object's full content as bytes, or None if missing /
        unreachable. Used by the append-only path to read the current
        cumulative CSV before appending new rows and re-uploading it."""
        if not self.is_configured():
            return None
        key = key_of(key_or_value)

        def _do() -> bytes | None:
            client = self._get_client()
            try:
                resp = client.get_object(Bucket=settings.s3_bucket, Key=key)
                return resp["Body"].read()
            except Exception:
                return None

        return await asyncio.to_thread(_do)

    async def object_size(self, key_or_value: str) -> int | None:
        """Return the object's size in bytes via HEAD, or None if it's
        missing / unreachable (callers treat None as 'unknown size')."""
        if not self.is_configured():
            return None
        key = key_of(key_or_value)

        def _do() -> int | None:
            client = self._get_client()
            try:
                head = client.head_object(Bucket=settings.s3_bucket, Key=key)
                return int(head.get("ContentLength", 0))
            except Exception:
                return None

        return await asyncio.to_thread(_do)


# Module-level singleton, mirroring `odata_client`.
storage_client = StorageClient()


# ── per-dataset storage routing ─────────────────────────────────────────
# A dataset's file destination is resolved from its ``scraper_config`` (an
# admin choice made at approval / in the panel) falling back to the global
# ``STORAGE_BACKEND`` default. These two helpers are the single source of
# truth for that decision, shared by the worker path (scraper/govmap) AND
# the CKAN poll path (snapshot_service / poll_job) so every backend routes
# identically. Kept here, rather than in app.api.worker, to avoid an import
# cycle (poll_job / snapshot_service must not import the worker API module).

def dataset_storage_target(ds) -> str:
    """Resolve a dataset's FILE-snapshot destination:
    ``'odata'`` | ``'r2'`` | ``'local'`` | ``'neon'``.

    A per-dataset choice (``scraper_config.storage_backend``) overrides the
    global ``STORAGE_BACKEND`` default. ``'local'`` is the legacy
    ``upload_mode='local_only'`` (worker keeps files, no upload). ``'neon'``
    means NEON-only: tabular rows are streamed to the append DB and NO file
    snapshot is written (see ``dataset_stores_files``).
    """
    sc = getattr(ds, "scraper_config", None) or {}
    if sc.get("upload_mode") == "local_only":
        return "local"
    return sc.get("storage_backend") or settings.storage_backend


def dataset_uses_r2(ds) -> bool:
    """True if THIS dataset's files should be written to R2 (and R2 is usable).
    Routes each dataset independently of the global default; requires the R2
    credentials to actually be present (else falls back to the ODATA path
    rather than erroring)."""
    return dataset_storage_target(ds) == "r2" and storage_client.is_configured()


def dataset_archives_neon(ds) -> bool:
    """True if THIS dataset should stream its tabular rows to the NEON append
    DB — either as the sole archive (``storage_backend='neon'``) or alongside a
    file snapshot (the ``r2+neon`` / ``odata+neon`` combos set
    ``scraper_config.archive_neon``). Independent of ``storage_mode``: an admin
    can opt a full-snapshot dataset into a queryable NEON mirror."""
    sc = getattr(ds, "scraper_config", None) or {}
    return sc.get("storage_backend") == "neon" or bool(sc.get("archive_neon"))


def dataset_stores_files(ds) -> bool:
    """True if THIS dataset writes a file snapshot at all. False only for the
    NEON-only plan (``storage_backend='neon'``), where the archive is the
    queryable row table and there is no per-version CSV/ZIP object."""
    return dataset_storage_target(ds) != "neon"
