import hashlib
import re
import uuid

from fastapi import HTTPException

# Hard ceiling on the ``offset`` a public paginated endpoint will accept. Deep
# OFFSET in Postgres is O(offset) — it still walks and discards every skipped
# row — so an unbounded offset lets a client make the DB do arbitrarily
# expensive work with a trivial request. Past this, callers should narrow their
# filters or use the bulk CSV / SQL endpoints instead of paging. Enforced as an
# ``le=`` bound on the Query param, so an over-limit value is a clean 422 (never
# a silent clamp that would quietly return the wrong page).
MAX_API_OFFSET = 100_000


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
