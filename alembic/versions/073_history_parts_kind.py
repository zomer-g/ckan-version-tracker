"""History parts: kind ('closed' | 'full').

Revision ID: 073
Revises: 072
Create Date: 2026-10-17

Phase 4 of docs/history-storage-strategy.md (plan: docs/history-phase4-plan.md).
A ``closed`` part (phase 2, the default) holds rows closed in one month. A
``full`` part holds EVERY row of a table moved to the cold tier, open rows
included (``last_seen`` NULL); its ``closed_month`` is the month of the move.
Every existing row is a closed part, hence the default.
"""
from alembic import op

revision = "073"
down_revision = "072"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE app.history_parts "
               "ADD COLUMN IF NOT EXISTS kind text NOT NULL DEFAULT 'closed'")
    op.execute("""
        DO $$ BEGIN
            ALTER TABLE app.history_parts ADD CONSTRAINT history_parts_kind_check
                CHECK (kind IN ('closed', 'full'));
        EXCEPTION WHEN duplicate_object THEN NULL;
        END $$
    """)


def downgrade() -> None:
    op.execute("ALTER TABLE app.history_parts DROP CONSTRAINT IF EXISTS history_parts_kind_check")
    op.execute("ALTER TABLE app.history_parts DROP COLUMN IF EXISTS kind")
