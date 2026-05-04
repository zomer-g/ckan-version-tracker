"""Persist conditional-archiver probe baselines on the tracked dataset

The conditional archiver does cheap "did this change?" probes
(``datastore_info`` row count + field shape; HTTP HEAD ``ETag`` /
``Last-Modified`` / ``Content-Length``) and short-circuits the legacy
download-and-hash pipeline when the probes confirm nothing changed.

The baseline it compares against has to live somewhere persistent. The
first place that suggests itself — the latest ``VersionIndex.resource_mappings``
— doesn't work in practice, because the legacy snapshot path creates a
fresh ``resource_mappings`` dict on every version it writes and never
inherits the probe fields. The result was that the conditional path
could probe, but never had a baseline to compare against, so it always
returned FALLBACK.

Putting the baseline on ``TrackedDataset`` instead decouples it from
version creation entirely. Any successful probe writes here, and the
next poll reads from here. Persistent across versions, no inheritance
dance.

Shape of ``resource_probes``:

    {
      "<ckan_resource_id>": {
        "datastore": {"total": int, "fields": [str, ...]} | null,
        "http":      {"etag": str|null, "last_modified": str|null, "content_length": str|null} | null,
        "observed_at": "<ISO8601>"
      },
      ...
    }

Default NULL — the conditional archiver bootstraps the entry on its
first poll for each resource and uses it from the second poll onward.

Revision ID: 017
Revises: 016
Create Date: 2026-05-04
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "017"
down_revision: Union[str, None] = "016"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "tracked_datasets",
        sa.Column("resource_probes", JSONB(), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("tracked_datasets", "resource_probes")
