"""Health runs: the log of the daily / weekly / manual system checks.

Revision ID: 074
Revises: 073
Create Date: 2026-10-31

See docs/health-checks.md and app/services/health_checks.py. In schema `app`
by name, like every app table since 066: the public SQL console's role reads
`public`, and this is bookkeeping (error messages, worker names), not data.
"""
from alembic import op

revision = "074"
down_revision = "073"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
    op.execute("""
        CREATE TABLE IF NOT EXISTS app.health_runs (
            id           uuid PRIMARY KEY,
            tier         varchar(16)  NOT NULL,
            trigger      varchar(255) NOT NULL,
            status       varchar(16)  NOT NULL DEFAULT 'running',
            started_at   timestamptz  NOT NULL DEFAULT now(),
            finished_at  timestamptz,
            duration_ms  integer,
            summary      jsonb,
            results      jsonb
        )
    """)
    op.execute("CREATE INDEX IF NOT EXISTS ix_health_runs_started "
               "ON app.health_runs (started_at)")
    op.execute("CREATE INDEX IF NOT EXISTS ix_health_runs_tier_started "
               "ON app.health_runs (tier, started_at)")
    op.execute("REVOKE ALL ON TABLE app.health_runs FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS app.health_runs")
