"""Drive export: document-level resume cursor columns

The export now unpacks each ZIP and uploads the individual documents, so the
unit of work is documents, not source files. Add documents_uploaded (headline
count + fine resume cursor) and archive_base (documents_uploaded at the last
finished archive) so a resume skips exactly the already-uploaded members of the
in-flight archive without re-downloading finished ones or creating duplicates.

Revision ID: 020
Revises: 019
Create Date: 2026-06-26
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "020"
down_revision: Union[str, None] = "019"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "drive_export_jobs",
        sa.Column("documents_uploaded", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "drive_export_jobs",
        sa.Column("archive_base", sa.Integer(), nullable=False, server_default="0"),
    )


def downgrade() -> None:
    op.drop_column("drive_export_jobs", "archive_base")
    op.drop_column("drive_export_jobs", "documents_uploaded")
