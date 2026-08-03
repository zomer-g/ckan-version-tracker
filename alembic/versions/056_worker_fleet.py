"""The fleet as a table, so one worker can be drained for a code update

A worker existed only as a string stamped on the tasks it had run
(scrape_tasks.worker_id / worker_ip). That cannot answer either question an
operator has when updating worker code: which machines are alive right now — an
idle worker has stamped nothing — and how do I take ONE machine out of rotation
without killing the scrape it is in the middle of, which for a heavy GovMap
layer can be over an hour of work.

`workers` is written by the poll endpoint, so it is the fleet as it reports
itself, throttled to one write per machine per 20s (a worker polls ~1/s, and on
a DB billed by compute a row update per poll buys precision nobody reads).

`paused` is the drain, and it is the gentlest mechanism available: a paused
worker is told "no task" (204) when it asks for its NEXT one. Nothing is killed
and nothing is rolled back. It needs no worker-side code — 204 is the response
the worker has always handled — which matters because the worker repo deploys
separately from this one.

Empty by default and populated on the first poll after deploy; no dataset,
task, or worker behaviour changes until an admin pauses something.

Revision ID: 056
Revises: 055
Create Date: 2026-08-03
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


revision: str = "056"
down_revision: Union[str, None] = "055"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "workers",
        sa.Column("worker_key", sa.String(length=80), primary_key=True),
        sa.Column("worker_id", sa.String(length=64), nullable=True),
        sa.Column("worker_ip", sa.String(length=64), nullable=True),
        sa.Column("worker_version", sa.String(length=64), nullable=True),
        sa.Column("worker_upstream", sa.String(length=16), nullable=True),
        sa.Column(
            "last_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "paused", sa.Boolean(), server_default=sa.text("false"), nullable=False
        ),
        sa.Column("paused_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("paused_by", sa.String(length=255), nullable=True),
    )


def downgrade() -> None:
    op.drop_table("workers")
