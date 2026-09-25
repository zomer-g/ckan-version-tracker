"""Drop three יומן לעם indexes that another index already covers.

Revision ID: 069
Revises: 068
Create Date: 2026-09-25

The ocal schema came over from the Node app with 22 indexes on its two big
tables. Three of them are a strict leading prefix of another index on the same
table, so every lookup they serve the longer index serves too:

  idx_event_entities_event       (event_id)
      ⊂ idx_event_entities_unique (event_id, entity_type, entity_name, role)
  diary_events_source_id_index   (source_id)
      ⊂ idx_events_source_date_id (source_id, event_date, id)
  idx_events_source_date         (source_id, event_date)
      ⊂ idx_events_source_date_id (source_id, event_date, id)

~47 MB together. The large ones (name aggregation, LOWER(TRIM(name)),
confidence >= 0.5 partials, trigram) stay: app/api/ocal.py queries by exactly
those shapes.

A lock that cannot be had in 10 s skips that index rather than stall boot; a
later run finds it again (IF EXISTS).
"""
from alembic import op

revision = "069"
down_revision = "068"
branch_labels = None
depends_on = None

_REDUNDANT = (
    "idx_event_entities_event",
    "diary_events_source_id_index",
    "idx_events_source_date",
)


def upgrade() -> None:
    for name in _REDUNDANT:
        op.execute(f"""
            DO $$
            BEGIN
                SET LOCAL lock_timeout = '10s';
                EXECUTE 'DROP INDEX IF EXISTS ocal.{name}';
            EXCEPTION WHEN lock_not_available THEN
                RAISE NOTICE 'ocal.{name} is busy; left for a later run';
            END $$;
        """)


def downgrade() -> None:
    op.execute("CREATE INDEX IF NOT EXISTS idx_event_entities_event ON ocal.event_entities (event_id)")
    op.execute("CREATE INDEX IF NOT EXISTS diary_events_source_id_index ON ocal.diary_events (source_id)")
    op.execute("CREATE INDEX IF NOT EXISTS idx_events_source_date ON ocal.diary_events (source_id, event_date)")
