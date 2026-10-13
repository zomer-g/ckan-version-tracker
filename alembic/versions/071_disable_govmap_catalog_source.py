"""Disable the govmap_catalog registry source.

Revision ID: 071
Revises: 070
Create Date: 2026-10-14

GOVSCRAPER registered a ``govmap_catalog`` manifest (39f5921) for the GovMap
layer index and then withdrew it (6598a9a): the index is built inside OVER as
``public.over_govmap_layers`` instead (app/services/govmap_layer_index.py).
Sync never removes a manifest a worker stops sending, so the row stayed
enabled and OVER would still accept https://www.govmap.gov.il/ and create a
dataset no worker can scrape. No dataset was created from it.

The downgrade re-enables it.
"""
from alembic import op

revision = "071"
down_revision = "070"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("UPDATE app.source_registry SET enabled = false WHERE id = 'govmap_catalog'")


def downgrade() -> None:
    op.execute("UPDATE app.source_registry SET enabled = true WHERE id = 'govmap_catalog'")
