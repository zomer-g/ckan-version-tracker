"""MCP service-gateway principal: seed api_users row + widen tier check

Supports the machine-to-machine service-token bypass (app/mcp/auth.py). A
request whose Bearer token equals ``MCP_SERVICE_TOKEN`` authenticates as a
fixed "service-gateway" principal, skipping OAuth. That principal's tool calls
still write to ``mcp_usage_events``, whose ``api_user_id`` is a NOT-NULL FK to
``api_users.id`` — so the row must actually exist, or every service call's
usage log would silently fail the FK. This migration:

  1. widens the ``ck_api_users_tier`` CHECK to allow the new ``'service'`` tier;
  2. seeds the service row with the fixed UUID that ``auth.SERVICE_USER_ID``
     (00000000-0000-4000-8000-000000000001) hardcodes — keep them in lockstep.

The seed is idempotent (ON CONFLICT DO NOTHING); safe to re-run.

Revision ID: 024
Revises: 023
Create Date: 2026-07-04
"""
from typing import Sequence, Union

from alembic import op


revision: str = "024"
down_revision: Union[str, None] = "023"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_SERVICE_ID = "00000000-0000-4000-8000-000000000001"


def upgrade() -> None:
    # Widen the tier check to admit the machine-to-machine 'service' tier.
    op.drop_constraint("ck_api_users_tier", "api_users", type_="check")
    op.create_check_constraint(
        "ck_api_users_tier", "api_users", "tier IN ('beta','free','pro','service')",
    )

    # Seed the fixed service-gateway principal (idempotent).
    op.execute(
        f"""
        INSERT INTO api_users (id, email, name, is_active, tier)
        VALUES ('{_SERVICE_ID}', 'service-gateway@over.org.il',
                'Discovery Gateway (service)', TRUE, 'service')
        ON CONFLICT (id) DO NOTHING
        """
    )


def downgrade() -> None:
    op.execute(f"DELETE FROM api_users WHERE id = '{_SERVICE_ID}'")
    op.drop_constraint("ck_api_users_tier", "api_users", type_="check")
    op.create_check_constraint(
        "ck_api_users_tier", "api_users", "tier IN ('beta','free','pro')",
    )
