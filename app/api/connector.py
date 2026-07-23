"""Looker Studio community-connector API — key-gated SQL over the append DB.

Backs the Apps Script connector in looker-connector/ (not for direct public
use — the anonymous consoles at /api/tables serve that). Same read-only SQL
machinery and guards as /api/tables/sql, with two differences that exist only
because all Looker Studio traffic egresses from a handful of Google IPs:

  * a shared secret (env CONNECTOR_API_KEY, header X-Connector-Key) instead of
    per-IP anonymity — the key routes traffic to its own byte-budget bucket in
    ApiBudgetMiddleware so dashboards neither starve on the 2GB per-IP cap nor
    eat bystanders' quota;
  * a higher row cap (10k default / 50k max vs the console's 1k) because Looker
    charts aggregate client-side over the full result.

Empty CONNECTOR_API_KEY = feature off (503).

  POST /api/connector/sql     → {sql, max_rows?} → run_readonly_sql envelope
  GET  /api/connector/tables  → trimmed catalog for the connector's dropdown
"""
import logging
import secrets

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.rate_limit import limiter
from app.services import append_store, data_catalog

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/connector", tags=["connector"])

DEFAULT_MAX_ROWS = 10_000
HARD_MAX_ROWS = 50_000
TIMEOUT_MS = 30_000


class ConnectorSqlBody(BaseModel):
    sql: str
    max_rows: int | None = None


def _require_key(request: Request) -> None:
    key = getattr(settings, "connector_api_key", "") or ""
    if not key:
        raise HTTPException(status_code=503, detail="Connector API is not enabled")
    supplied = request.headers.get("X-Connector-Key", "")
    if not secrets.compare_digest(supplied, key):
        raise HTTPException(status_code=401, detail="Invalid connector key")


def _require_enabled() -> None:
    if not append_store.is_configured():
        raise HTTPException(status_code=409, detail="Append archive DB is not configured")


@router.post("/sql")
@limiter.limit("60/minute")
async def run_sql(request: Request, body: ConnectorSqlBody):
    """Read-only SELECT spanning the public/knesset/idx schemas, sized for
    Looker Studio getData calls."""
    _require_key(request)
    _require_enabled()
    max_rows = min(max(1, body.max_rows or DEFAULT_MAX_ROWS), HARD_MAX_ROWS)
    try:
        return await append_store.run_readonly_sql(
            body.sql,
            search_path=data_catalog.CONSOLE_SEARCH_PATH,
            max_rows=max_rows,
            timeout_ms=TIMEOUT_MS,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:  # noqa: BLE001 — SQL/timeout errors go to the user
        raise HTTPException(status_code=400, detail=f"{type(e).__name__}: {e}")


@router.get("/tables")
@limiter.limit("60/minute")
async def tables(request: Request, db: AsyncSession = Depends(get_db)):
    """The table catalog trimmed for the connector's config dropdown — no
    column lists (getSchema derives fields from the SQL itself)."""
    _require_key(request)
    _require_enabled()
    try:
        records = await data_catalog.build_catalog(db)
    except Exception as e:  # noqa: BLE001 — surface init errors readably
        logger.exception("/api/connector/tables catalog failed")
        raise HTTPException(status_code=503, detail=f"{type(e).__name__}: {e}")
    return {
        "tables": [
            {k: r.get(k) for k in ("table", "schema", "title", "kind", "est_rows")}
            for r in records
        ]
    }
