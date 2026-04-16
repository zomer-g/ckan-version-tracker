import hashlib
import re
import uuid

from fastapi import HTTPException


def parse_uuid(value: str, label: str = "ID") -> uuid.UUID:
    """Parse a UUID string, raising 400 if invalid."""
    try:
        return uuid.UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid {label}")


def sanitize_ckan_name(name: str) -> str:
    """Create a CKAN-safe dataset name (lowercase alphanumeric, hyphens, underscores)."""
    safe = re.sub(r"[^a-z0-9_-]", "-", name.lower())
    safe = re.sub(r"-+", "-", safe).strip("-")
    return safe[:80]


def scraper_url_slug(collector_name: str, source_url: str) -> str:
    """Unique slug for a scraper dataset: collector_name + short URL hash.

    Two URLs sharing the same collector path (e.g. /he/collectors/policies with
    different officeId query params) would otherwise collide on the same slug.
    Appending a short hash of the full URL keeps them separate while staying
    readable."""
    digest = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:8]
    return f"{sanitize_ckan_name(collector_name)}-{digest}"
