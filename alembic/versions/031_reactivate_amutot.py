"""Reactivate the 'עמותות' dataset (stranded inactive since the odata outage)

Dataset 73f3cd78 last polled 2026-06-18 and never again — through 20 days of
weekly-overdue reschedules — while status stayed 'active' and last_error NULL.
The only poll exit with that signature is the ``is_active`` skip at the top of
``poll_dataset`` (no timestamp write, no error), and ``init_scheduler`` filters
on ``is_active.is_(True)``, which also explains the missing auto-polls. Most
likely stranded by the 2026-06-23 odata-outage mass pause and never resumed.

Set it active again. Idempotent and harmless if it's already TRUE (or NULL —
the column is nullable, and NULL is falsy to the poll guard). On this deploy's
restart, init_scheduler sees an overdue active dataset and fires the poll,
which now routes to the NEON streaming path (see migration 030).

Revision ID: 031
Revises: 030
Create Date: 2026-07-08
"""
from typing import Sequence, Union

from alembic import op


revision: str = "031"
down_revision: Union[str, None] = "030"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DATASET_ID = "73f3cd78-ef41-4f2e-90c6-64ecbfc6e9a9"


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET is_active = TRUE
        WHERE id = '{_DATASET_ID}' AND is_active IS DISTINCT FROM TRUE
        """
    )


def downgrade() -> None:
    # No-op: we can't know the prior (paused) state was intentional; leaving
    # the dataset active on downgrade is the safe direction.
    pass
