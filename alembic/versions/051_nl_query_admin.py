"""Free-text query: move the cache out of the public DB, add a log and a kill switch.

THREE CHANGES, ONE OF THEM A FIX.

1. THE FIX — the question cache must not live in the append DB.
   It was originally created there (public.over_nl_cache) next to
   over_table_profiles, which looked right: derived data about public tables.
   It is not. data_catalog._over_index_records() surfaces EVERY public.over_*
   table in the /data catalog, and /api/tables/sql will happily SELECT from it.
   That would have published every question anyone ever typed into the box —
   user-authored text, potentially naming people or businesses — as a queryable
   public dataset. The cache belongs in the app DB, which nothing public can
   read. Same for the log below, for the same reason and more so.

2. THE LOG — one row per question, recording which STAGE answered it (cache /
   template / deepseek / anthropic / refused), what it cost, and what came back.
   Without it the escalation rate and the refusal rate are invisible until they
   show up on an invoice, and "is the cheap model good enough" is unanswerable.

3. THE KILL SWITCH — a single-row config table read at request time, so a tier
   or the whole feature can be turned off from the admin UI without a deploy.
   Config in env vars is the wrong shape for a cost control: the moment you want
   it is the moment you do not want to wait for a redeploy.

The log stores the question text. That is the point — it is the only way to see
what people actually ask — but it makes this table personal-ish data: keep it
admin-only, and prune it (there is an index on created_at for exactly that).

Revision ID: 051
Revises: 050
Create Date: 2026-08-01
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "051"
down_revision: Union[str, None] = "050"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "nl_query_cache",
        sa.Column("fingerprint", sa.String(64), primary_key=True),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("entity", sa.String(255)),
        sa.Column("query_json", sa.JSON()),
        sa.Column("sql", sa.Text()),
        sa.Column("explanation", sa.Text()),
        sa.Column("source", sa.String(32)),
        sa.Column("model", sa.String(64)),
        sa.Column("hits", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("last_hit_at", sa.DateTime(timezone=True)),
    )

    op.create_table(
        "nl_query_log",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("question", sa.Text(), nullable=False),
        sa.Column("answered", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        # Which tier produced the answer: cache | template | deepseek |
        # anthropic | refused | error. This is the column the admin log is for.
        sa.Column("stage", sa.String(32), nullable=False),
        # Every tier that was ATTEMPTED, in order ("deepseek>anthropic"). The
        # stage says who won; this says what it cost to get there.
        sa.Column("attempts", sa.String(128)),
        sa.Column("model", sa.String(64)),
        sa.Column("escalated", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("entity", sa.String(255)),
        sa.Column("sql", sa.Text()),
        # The refusal reason, or the error message. Both are "why there is no
        # number", which is the same question from the admin's side.
        sa.Column("reason", sa.Text()),
        sa.Column("input_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("output_tokens", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_ms", sa.Integer()),
    )
    op.create_index("ix_nl_query_log_created_at", "nl_query_log", ["created_at"])
    # The two questions the admin page actually asks — "what did each stage cost
    # today" and "show me the refusals" — both filter on stage.
    op.create_index("ix_nl_query_log_stage", "nl_query_log", ["stage"])

    op.create_table(
        "nl_query_config",
        # Single row, id fixed at 1. A settings TABLE rather than a key/value
        # store because there are six knobs, they are typed, and they are read
        # on every request — a typo in a key name should be a schema error, not
        # a silently-ignored setting.
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("allow_deepseek", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("allow_anthropic", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("escalate_on_unanswerable", sa.Boolean(), nullable=False,
                  server_default=sa.text("true")),
        # NULL means "use the value from config.py". An admin override is an
        # exception, not the normal way to configure the app, so it must be
        # possible to clear one and go back to the deployed default.
        sa.Column("daily_call_budget", sa.Integer()),
        sa.Column("daily_output_token_budget", sa.BigInteger()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
    )
    op.execute("INSERT INTO nl_query_config (id) VALUES (1) ON CONFLICT DO NOTHING")


def downgrade() -> None:
    op.drop_table("nl_query_config")
    op.drop_index("ix_nl_query_log_stage", table_name="nl_query_log")
    op.drop_index("ix_nl_query_log_created_at", table_name="nl_query_log")
    op.drop_table("nl_query_log")
    op.drop_table("nl_query_cache")
