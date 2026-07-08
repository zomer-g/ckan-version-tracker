"""Pin the 'עמותות' dataset to its single datastore resource (forward NEON)

Dataset 73f3cd78 ('מאגר עמותות וחברות לתועלת הציבור — עמותות רשומות') carries the
r2+neon storage plan, but its source package (moj-amutot) grew to 5 resources
while the dataset tracked them all (resource_ids=NULL). The NEON row-streaming
path in ``poll_job._poll_large_dataset`` only engages for a SINGLE
datastore-active resource — so with 5 resources the poll fell back to the
multi-CSV in-memory snapshot path, which never wrote NEON and pushed the 512MB
web dyno into OOM.

Pin it to the main 'עמותות רשומות' CSV (be5b7935…) so forward polls stream that
one table's rows straight to the NEON append DB via delta_archiver (paged +
checkpointed, memory-bounded). No historical backfill — NEON fills from the
next poll onward. The r2+neon plan and storage_mode are left untouched;
``dataset_archives_neon`` already routes this dataset through the streaming path
once it tracks a single resource.

One-off data fix — single row, idempotent.

Revision ID: 030
Revises: 029
Create Date: 2026-07-08
"""
from typing import Sequence, Union

from alembic import op


revision: str = "030"
down_revision: Union[str, None] = "029"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DATASET_ID = "73f3cd78-ef41-4f2e-90c6-64ecbfc6e9a9"
_RESOURCE_ID = "be5b7935-3922-45d4-9638-08871b17ec95"  # 'עמותות רשומות' CSV, datastore-active


def upgrade() -> None:
    op.execute(
        f"""
        UPDATE tracked_datasets
        SET resource_ids = '["{_RESOURCE_ID}"]'::jsonb
        WHERE id = '{_DATASET_ID}'
        """
    )


def downgrade() -> None:
    op.execute(
        f"UPDATE tracked_datasets SET resource_ids = NULL WHERE id = '{_DATASET_ID}'"
    )
