"""Move the credential-bearing tables out of `public` into `auth`.

Revision ID: 065
Revises: 064
Create Date: 2026-09-10

WHY
---
Today the public SQL console and the application's own tables live in two
physically separate Postgres databases, and `app/main.py:_guard_db_separation`
refuses to boot if they ever become one. That is the strongest guarantee in the
system: a connection to one database *cannot* reach the other, because Postgres
has no cross-database queries. No configuration mistake can weaken it.

The move to xhostd ends that arrangement. xhostd gives an app one database, and
injects a read-only role (`DATABASE_URL_READONLY`) that automatically holds
SELECT on **everything in `public`, including tables created later**. So on the
far side, "which schema is this table in" becomes the whole boundary, and
`public` becomes a default-allow surface.

This migration draws that boundary before the move rather than during it. The
six tables below hold credentials or personal data and go to `auth`, which the
read-only role is never granted on. Everything else stays in `public` and stays
readable, which is correct — it is all published data.

WHAT WAS CONSIDERED AND LEFT IN `public`
----------------------------------------
`workers` holds `worker_key`, which reads like a credential and is not one: it
is a machine identity string ("hostname#short", or "ip:<addr>" for older
workers). The fleet authenticates with WORKER_API_KEY, an environment variable
that is in no table. `workers` does expose worker IP addresses, which is a mild
infrastructure disclosure rather than a secret, and it is read by the admin
dashboard. Left in place deliberately; revisit if the fleet ever moves off a
shared env-var key.

`sql_shares` holds saved public queries. Public by construction.

SAFETY
------
`ALTER TABLE ... SET SCHEMA` moves a table with its indexes, constraints and
foreign keys intact, including foreign keys pointing at it from tables that stay
in `public` (drive_export_jobs, tags, tracked_datasets and api_users all
reference users.id). Postgres resolves those across schemas without change.

No raw SQL in the application references these tables — every access goes
through the SQLAlchemy models, which now declare `schema="auth"` — so the
qualification is emitted automatically.
"""
from alembic import op

revision = "065"
down_revision = "064"
branch_labels = None
depends_on = None

# Order does not matter to Postgres here; kept grouped by what each one holds.
TABLES = (
    "users",               # google_refresh_token (cleartext), hashed_password
    "auth_codes",          # code_hash — the one-time SSO/Drive exchange codes
    "api_users",           # the API/MCP allow-list, and the emails in it
    "mcp_oauth_clients",   # client_secret_hash
    "mcp_oauth_codes",     # `code` is the authorization code itself, cleartext
    "mcp_usage_events",    # who called the API and when
)


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS auth")
    for table in TABLES:
        # to_regclass returns NULL rather than raising when the table is absent,
        # so a database that never had one of these (a fresh test database built
        # from metadata, say) migrates cleanly instead of failing here.
        op.execute(
            f"""
            DO $$
            BEGIN
              IF to_regclass('public.{table}') IS NOT NULL THEN
                EXECUTE 'ALTER TABLE public.{table} SET SCHEMA auth';
              END IF;
            END $$;
            """
        )

    # The read-only role must never hold anything on this schema. Said out loud
    # here so an audit of the migrations shows the intent, not just its absence.
    op.execute("REVOKE ALL ON SCHEMA auth FROM PUBLIC")


def downgrade() -> None:
    for table in TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF to_regclass('auth.{table}') IS NOT NULL THEN
                EXECUTE 'ALTER TABLE auth.{table} SET SCHEMA public';
              END IF;
            END $$;
            """
        )
    op.execute("DROP SCHEMA IF EXISTS auth")
