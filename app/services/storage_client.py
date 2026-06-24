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
* **Flag-gated.** Nothing changes until ``STORAGE_BACKEND=r2`` and the S3
  credentials are configured. With the default (``odata``) ``is_enabled()`` is
  False and callers fall back to the existing ODATA path, so production is
  untouched by merely deploying this code.
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
