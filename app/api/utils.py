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


def require_http_url(value):
    """Pydantic validator: a URL (or list of them) a person submitted must be
    http(s) — or, for file pickers, a server-relative path.

    The source parsers check the host only, so ``javascript://www.gov.il/%0a…``
    passed them and was stored, then rendered as a link on the versions page
    and the admin queue. React blocks ``javascript:`` hrefs today; the stored
    value should not depend on that.
    """
    from urllib.parse import urlparse

    if value is None:
        return value
    items = value if isinstance(value, list) else [value]
    for item in items:
        v = (item or "").strip()
        if not v or (v.startswith("/") and not v.startswith("//")):
            continue
        parsed = urlparse(v)
        if parsed.scheme.lower() not in ("http", "https") or not parsed.netloc:
            raise ValueError("URL must start with http:// or https://")
    return value


def public_error(exc: BaseException, status_code: int = 400, *, where: str = "") -> HTTPException:
    """The HTTPException a PUBLIC route raises for an unexpected error.

    These routes used to answer ``f"{type(e).__name__}: {e}"``. For a SQL
    error that is the useful answer, and it stays: Postgres and DuckDB errors
    are about the caller's own query. Anything else — a connection failure
    naming the database host, a library's internal message — is logged here
    and answered generically.
    """
    import asyncio
    import logging

    import asyncpg

    log = logging.getLogger("app.api.public_error")
    if isinstance(exc, (asyncio.TimeoutError, TimeoutError, asyncpg.QueryCanceledError)):
        return HTTPException(status_code=status_code,
                             detail="השאילתה חרגה מזמן הריצה המותר (query timed out)")
    if isinstance(exc, asyncpg.PostgresError) and not isinstance(
            exc, (asyncpg.InsufficientPrivilegeError, asyncpg.InvalidAuthorizationSpecificationError)):
        return HTTPException(status_code=status_code, detail=f"{type(exc).__name__}: {exc}")
    try:
        import duckdb
        if isinstance(exc, (duckdb.ParserException, duckdb.BinderException,
                            duckdb.CatalogException, duckdb.ConversionException,
                            duckdb.InvalidInputException)):
            return HTTPException(status_code=status_code, detail=f"{type(exc).__name__}: {exc}")
    except ImportError:
        pass
    log.exception("public route error%s", f" ({where})" if where else "")
    return HTTPException(status_code=status_code,
                         detail="שגיאה פנימית; היא נרשמה ביומן (internal error, logged)")
