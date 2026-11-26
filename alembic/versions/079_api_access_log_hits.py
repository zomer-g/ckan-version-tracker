"""app.api_access_log.hits: how many calls one row stands for.

Past 300 rows per IP per minute the access log used to write ONE
"(suppressed)" row (status 0, no route) for the rest of that minute, so a
flood's calls dropped out of every error and per-route count: on 25.11 the admin
page showed 46,231 nadlan errors of the 54,317 served. The overflow is now
folded into aggregate rows per route and status, each carrying ``hits``, and the
admin queries sum it.

The old summary rows keep their status 0, but get back the number of calls they
stood for (it is in their label), so totals over the retained days are right.

Revision ID: 079
Revises: 078
Create Date: 2026-11-26
"""
from alembic import op

revision = "079"
down_revision = "078"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # A constant default: no table rewrite on Postgres 11+.
    op.execute("ALTER TABLE app.api_access_log "
               "ADD COLUMN IF NOT EXISTS hits integer NOT NULL DEFAULT 1")
    op.execute(r"""
        UPDATE app.api_access_log
           SET hits = substring(actor_label FROM '^(\d+) requests over')::int
         WHERE path = '(suppressed)'
           AND actor_label ~ '^\d+ requests over'
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE app.api_access_log DROP COLUMN IF EXISTS hits")
