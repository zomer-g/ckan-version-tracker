"""Add tags + dataset_tags for cross-org dataset categorization

Revision ID: 012
Revises: 011
Create Date: 2026-04-25
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "012"
down_revision: Union[str, None] = "011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tags",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.String(2000), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "created_by",
            sa.UUID(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    # Case-insensitive uniqueness on name (Hebrew is unaffected; matters for
    # latin-script tags, e.g. "Transport" vs "transport").
    op.create_index(
        "ix_tags_name_lower",
        "tags",
        [sa.text("lower(name)")],
        unique=True,
    )

    op.create_table(
        "dataset_tags",
        sa.Column(
            "dataset_id",
            sa.UUID(),
            sa.ForeignKey("tracked_datasets.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "tag_id",
            sa.UUID(),
            sa.ForeignKey("tags.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index("ix_dataset_tags_tag_id", "dataset_tags", ["tag_id"])


def downgrade() -> None:
    op.drop_index("ix_dataset_tags_tag_id", table_name="dataset_tags")
    op.drop_table("dataset_tags")
    op.drop_index("ix_tags_name_lower", table_name="tags")
    op.drop_table("tags")
