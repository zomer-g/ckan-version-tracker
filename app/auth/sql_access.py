"""Who is running free SQL against a dataset: a trusted service, a signed-in person, or nobody.

The per-dataset SQL endpoints (POST /api/append/{id}/sql and
GET /api/append/{id}/datastore_search_sql) have been anonymous since they
shipped, and TAG-IT (SMART DMS, source `over_sql_rows`) imports government
decisions through them with no credentials. Closing them to anonymous callers
therefore happens in three steps, and this module is the part that makes the
order safe:

  1. OVER accepts SQL_SERVICE_TOKEN (this code) while nothing is closed.
  2. SMART DMS starts sending `Authorization: Bearer <SQL_SERVICE_TOKEN>`, and
     an import is seen succeeding under caller=service in the audit log.
  3. APPEND_SQL_REQUIRE_AUTH=true closes both endpoints to anonymous callers.
     An environment variable, not a deploy of new code.

SQL_SERVICE_TOKEN is deliberately separate from MCP_SERVICE_TOKEN: that one is
unscoped across every MCP resource (see app/mcp/auth.py), and this one opens
only these two read-only endpoints. Whoever presents it still runs as the
console's least-privilege read-only role, like everyone else.

Security invariants, as for MCP_SERVICE_TOKEN: never log the token or the
Authorization header; store it only in the platform's secrets; rotate it in
OVER and in every client together. An empty configured token matches nothing.
"""
import hashlib
import hmac
import logging
from dataclasses import dataclass

from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.dependencies import get_optional_user, optional_bearer_scheme
from app.config import settings
from app.database import get_db

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SqlCaller:
    kind: str   # "service" | "user" | "anonymous"
    label: str  # "sql-service", a user id, or "anonymous"; never a token


ANONYMOUS = SqlCaller("anonymous", "anonymous")


def service_token_matches(presented: str | None) -> bool:
    configured = settings.sql_service_token or ""
    return bool(configured) and bool(presented) and hmac.compare_digest(presented, configured)


async def sql_caller(
    credentials: HTTPAuthorizationCredentials | None = Depends(optional_bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> SqlCaller:
    """The service token is checked first, so a service never needs a user
    session; any other bearer is tried as a sign-in token; anything else is
    anonymous, including a wrong service token."""
    token = credentials.credentials if credentials else None
    if service_token_matches(token):
        return SqlCaller("service", "sql-service")
    if token:
        user = await get_optional_user(credentials, db)
        if user is not None:
            return SqlCaller("user", str(user.id))
    return ANONYMOUS


def admit(caller: SqlCaller, *, dataset_id: str, sql: str) -> None:
    """Refuse an anonymous caller when APPEND_SQL_REQUIRE_AUTH is on; record known callers.

    The audit line names the dataset, the caller kind and label, and a short
    hash of the SQL. The token is never part of it, and neither is a person's
    email: the user id is enough to attribute a query and keeps logs free of it.
    """
    if caller.kind == "anonymous":
        if settings.append_sql_require_auth:
            raise HTTPException(status_code=401, detail="Not authenticated",
                                headers={"WWW-Authenticate": "Bearer"})
        return
    digest = hashlib.sha256((sql or "").encode("utf-8")).hexdigest()[:16]
    logger.info("append sql: dataset=%s caller=%s:%s sql_sha256=%s",
                dataset_id, caller.kind, caller.label, digest)
