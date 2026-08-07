"""Key a worker row by MACHINE, not by process — the fleet list was filling with ghosts

The worker reports ``<hostname>#<short>`` where ``short`` is a random token
fixed at import time (govscraper _detect_worker_id), i.e. a PROCESS id.
Migration 056 keyed the fleet on that whole string, which was wrong twice:

  * every restart minted a new row. Within two days of shipping, one machine
    (GZ-14) had over twenty rows in the panel, all but one of them a dead
    process;
  * a pause was pinned to a process that no longer exists. Restarting to update
    code — the entire reason the pause exists — brought the machine back under
    a new key, unpaused. The switch could not survive the one event it was
    built for.

This collapses each hostname group to a single row, keeping the state of its
MOST RECENTLY SEEN instance: that is the live one, so a pause an admin set
minutes ago on the running instance carries over, while a pause left on an
instance that died days ago does not come back to haunt a machine that is
working fine now.

Rows keyed without a "#" (an OVER_WORKER_ID friendly name, or the "ip:<addr>"
fallback) already name a machine and pass through untouched.

Revision ID: 061
Revises: 060
Create Date: 2026-08-03
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "061"
down_revision: Union[str, None] = "060"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop every non-latest instance of each machine. The (last_seen_at,
    # worker_key) tuple breaks ties deterministically, so this leaves exactly
    # one row per hostname even if two instances share a timestamp.
    op.execute(sa.text("""
        DELETE FROM workers a
        USING workers b
        WHERE split_part(a.worker_key, '#', 1) = split_part(b.worker_key, '#', 1)
          AND (a.last_seen_at, a.worker_key) < (b.last_seen_at, b.worker_key)
    """))
    # Re-key the survivors to the machine. Safe as a bare UPDATE: after the
    # delete above, no two rows can collapse onto the same hostname.
    op.execute(sa.text("""
        UPDATE workers
        SET worker_key = split_part(worker_key, '#', 1)
        WHERE worker_key LIKE '%#%'
    """))


def downgrade() -> None:
    # The per-process rows are gone and cannot be reconstructed — they will
    # re-appear on their own as workers poll under the old keying scheme.
    pass
