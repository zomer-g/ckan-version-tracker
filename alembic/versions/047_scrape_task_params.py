"""Per-RUN parameters on a scrape task, so one dataset can be sampled several ways.

Until now everything the worker knew about a run came from the DATASET
(``scraper_config``), which makes every poll of a dataset identical by
construction. That is right for a source whose only question is "what does it
look like now", and wrong for a register the size of Jerusalem's building
licensing files, where a full pass is a ten-hour sweep and the useful questions
are much narrower: *what is new*, *what changed among the files at a given
status*, *this one file*.

``params`` is the missing axis. It is merged over ``scraper_config`` in the
/poll response, so a run can override any config key for that run only, and the
dataset's stored configuration is left alone. Unknown keys are ignored by every
existing scraper — a source that doesn't read them behaves exactly as before.

Nullable with no default: a task without params IS the routine full poll.

Revision ID: 047
Revises: 046
Create Date: 2026-07-31
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "047"
down_revision: Union[str, None] = "046"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "scrape_tasks",
        sa.Column("params", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("scrape_tasks", "params")
