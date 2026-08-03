"""Per-source cap on concurrent workers, so one upstream can't eat the fleet

The scrape queue could already answer "what goes next" (priority bands, 044) but
not "how much of the fleet may sit on one site at once". Nothing stopped every
worker from ending up on the same upstream: GovMap alone is 866 of ~1,100
tracked datasets, munidata another 38, and a coverage sweep queues them in bulk.
Concentrating the whole fleet on one server is how you get throttled or blocked
— and while it lasts, every other source waits behind work that is all aimed at
one place.

`source_limits` holds one admin decision per source: at most N workers on it
concurrently. Enforcement is on the claim path (app/api/worker.py) — a saturated
source is simply excluded from the query that hands out the next task, so the
cap can never abort a scrape already in flight; it takes effect by starving new
claims until the excess drains.

Empty by default, and an absent row means uncapped, so this migration changes
the fleet's behaviour for exactly nobody until someone throttles a source on
purpose. max_workers = 0 is allowed and means "stop giving this source work" —
the state you want the moment an upstream starts erroring.

`source_key` is derived (app/services/source_load.py), not an entity: a scraper
ckan_id prefix ("munidata", "jda") or a source_type ("govmap", "ckan"). Hence no
foreign key — the table of sources it would point at does not exist.

Revision ID: 055
Revises: 054
Create Date: 2026-08-03
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "055"
down_revision: Union[str, None] = "054"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "source_limits",
        sa.Column("source_key", sa.String(length=64), primary_key=True),
        sa.Column("max_workers", sa.SmallInteger(), nullable=False),
        sa.Column("updated_by", sa.String(length=255), nullable=True),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("max_workers >= 0", name="ck_source_limits_non_negative"),
    )


def downgrade() -> None:
    op.drop_table("source_limits")
