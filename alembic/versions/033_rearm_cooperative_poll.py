"""Re-arm the cooperative registry poll after the conditional-archiver fix

Migration 032 armed a self-firing repoll for dataset 59360419, but the poll it
triggered was intercepted by the conditional archiver: bytes unchanged since
v1 ⇒ it committed a metadata_only version (v2) and returned BEFORE poll_job's
neon_datastore routing — so NEON stayed empty, and the poll re-stamped
last_modified/last_polled_at, disarming the trigger. The code fix (this
deploy) exempts archive_neon datasets from the conditional archiver, mirroring
the append_only exemption; this migration just re-arms the same two NULLs so
the restart's scheduler fires the poll again:

  - last_polled_at NULL → init_scheduler fires immediately (never-polled);
  - last_modified NULL → has_metadata_changed(None,·)=True bypasses the
    unchanged-metadata skip, and forced_repoll bypasses the version-exists skip.

This time the poll reaches the routing (single datastore-active resource +
archive_neon) and streams the ~7.2k rows to NEON via delta_archiver.

Revision ID: 033
Revises: 032
Create Date: 2026-07-08
"""
from typing import Sequence, Union

from alembic import op


revision: str = "033"
down_revision: Union[str, None] = "032"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DATASET_ID = "59360419-13ac-4a8e-8dce-f6f6b89a3beb"


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET last_modified = NULL,
            last_polled_at = NULL
        WHERE id = '{_DATASET_ID}'
        """
    )


def downgrade() -> None:
    pass  # one-off poll re-arm; nothing meaningful to restore
