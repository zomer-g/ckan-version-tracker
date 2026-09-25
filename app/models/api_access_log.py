"""One row per request to the public API surfaces (/api/*, every MCP server).

Written by app/api_access_log_middleware.py through the batching buffer in
app/services/api_access_log.py, read by the admin "גישה ל-API" tab. Rows are
never updated; a daily scheduler job deletes rows older than the retention.

No token, key or Authorization header is ever stored: ``actor_id`` is a user or
MCP-user id, and the query string is scrubbed of credential parameters.
"""
from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Index, Integer, SmallInteger, String
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ApiAccessLog(Base):
    __tablename__ = "api_access_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
    method: Mapped[str] = mapped_column(String(8), nullable=False)
    path: Mapped[str] = mapped_column(String(500), nullable=False)
    # The route template ("/api/v1/datasets/{dataset_id}"), so one endpoint is one
    # group however many ids it is called with. NULL when nothing matched (404).
    route: Mapped[str | None] = mapped_column(String(300))
    # Which API: "v1", "append", "tables", "mcp:deals", ...
    area: Mapped[str] = mapped_column(String(60), nullable=False)
    query: Mapped[str | None] = mapped_column(String(500))
    # The dataset / table / id the call was about, from the route's path params.
    target: Mapped[str | None] = mapped_column(String(200))
    status: Mapped[int] = mapped_column(SmallInteger, nullable=False)
    duration_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    bytes_out: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    ip: Mapped[str | None] = mapped_column(String(64))
    country: Mapped[str | None] = mapped_column(String(8))
    user_agent: Mapped[str | None] = mapped_column(String(400))
    # Short client family ("curl", "python-requests", "Chrome", "Claude", ...).
    client: Mapped[str | None] = mapped_column(String(60))
    # How it came: site (our own frontend) | mcp | script | browser | bot | apps_script | other
    channel: Mapped[str] = mapped_column(String(20), nullable=False)
    # Who: anonymous | user | mcp_user | mcp_service | sql_service | connector | bearer_invalid
    actor_kind: Mapped[str] = mapped_column(String(20), nullable=False)
    actor_id: Mapped[str | None] = mapped_column(String(100))
    actor_label: Mapped[str | None] = mapped_column(String(200))
    referer_host: Mapped[str | None] = mapped_column(String(200))

    __table_args__ = (
        Index("ix_api_access_log_ts", "ts"),
        Index("ix_api_access_log_area_ts", "area", "ts"),
        Index("ix_api_access_log_ip_ts", "ip", "ts"),
        Index("ix_api_access_log_actor_ts", "actor_id", "ts"),
    )
