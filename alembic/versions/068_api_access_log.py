"""API access log: one row per request to /api/* and the MCP servers.

Revision ID: 068
Revises: 067
Create Date: 2026-09-25

Until now the only per-call record was mcp_usage_events (MCP tool calls, no IP).
Who pulls the bulk API, with what client and for which dataset was not
answerable. Written by app/api_access_log_middleware.py, read by the admin
"גישה ל-API" tab, purged daily past API_ACCESS_LOG_RETENTION_DAYS.

The table is created in schema `app` BY NAME: it holds client IP addresses and
user ids, and `public` is readable by the public SQL console's role (see 066).
"""
from alembic import op

revision = "068"
down_revision = "067"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.api_access_log (
            id           bigserial PRIMARY KEY,
            ts           timestamptz NOT NULL DEFAULT now(),
            method       varchar(8)   NOT NULL,
            path         varchar(500) NOT NULL,
            route        varchar(300),
            area         varchar(60)  NOT NULL,
            query        varchar(500),
            target       varchar(200),
            status       smallint     NOT NULL,
            duration_ms  integer      NOT NULL,
            bytes_out    bigint       NOT NULL DEFAULT 0,
            ip           varchar(64),
            country      varchar(8),
            user_agent   varchar(400),
            client       varchar(60),
            channel      varchar(20)  NOT NULL,
            actor_kind   varchar(20)  NOT NULL,
            actor_id     varchar(100),
            actor_label  varchar(200),
            referer_host varchar(200)
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_api_access_log_ts ON app.api_access_log (ts)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_api_access_log_area_ts ON app.api_access_log (area, ts)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_api_access_log_ip_ts ON app.api_access_log (ip, ts)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_api_access_log_actor_ts ON app.api_access_log (actor_id, ts)")
    # Belt and braces: whatever default privileges the platform grants, nobody
    # but the owner reads this table or its sequence.
    op.execute("REVOKE ALL ON TABLE app.api_access_log FROM PUBLIC")
    op.execute("REVOKE ALL ON SEQUENCE app.api_access_log_id_seq FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.api_access_log")
