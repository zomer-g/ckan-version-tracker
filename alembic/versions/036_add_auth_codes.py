"""Short-lived one-time auth codes (replace JWT-in-query-string).

The OAuth callbacks used to hand the admin JWT back through the URL query
string (``/admin/login?sso_token=<JWT>`` and, for Drive, the whole token
embedded in Google's ``state``). Tokens in URLs leak via Referer, browser
history, and proxy/Render/Cloudflare logs. This table holds random, single-use,
time-boxed codes that carry NO secret in the URL: the SPA / Google carries only
the opaque code, and it is swapped server-side for a fresh JWT. Only the
SHA-256 of the code is stored. See app/models/auth_code.py + app/services/auth_codes.py.

Revision ID: 036
Revises: 035
Create Date: 2026-07-17
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


revision: str = "036"
down_revision: Union[str, None] = "035"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "auth_codes",
        sa.Column("code_hash", sa.String(length=64), primary_key=True),
        sa.Column("user_id", UUID(as_uuid=True), nullable=False),
        sa.Column("purpose", sa.String(length=16), nullable=False),
        sa.Column("next_path", sa.String(length=512), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_auth_codes_user_id", "auth_codes", ["user_id"])
    # Cheap sweep of expired rows.
    op.create_index("ix_auth_codes_expires_at", "auth_codes", ["expires_at"])


def downgrade() -> None:
    op.drop_index("ix_auth_codes_expires_at", table_name="auth_codes")
    op.drop_index("ix_auth_codes_user_id", table_name="auth_codes")
    op.drop_table("auth_codes")
