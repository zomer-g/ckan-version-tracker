"""Track the last poll error per dataset

Adds `last_error` to tracked_datasets so we can show *why* a dataset has
no usable versions. Until now, when every resource download or upload
failed, the poller still created an empty version with no resource
mappings — the only signal of failure was a server log line that no one
ever sees. From now on, errors are persisted and the empty-version
shortcut is closed (see app/worker/poll_job.py and app/api/worker.py).

Revision ID: 014
Revises: 013
Create Date: 2026-04-30
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "014"
down_revision: Union[str, None] = "013"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("last_error", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "last_error")
