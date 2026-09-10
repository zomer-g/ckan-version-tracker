"""Move the application's own tables out of `public` into `app`.

Revision ID: 066
Revises: 065
Create Date: 2026-09-10

WHY
---
065 moved the credential tables into `auth` so that, on xhostd, the platform's
read-only role (which holds SELECT on everything in `public`, including tables
created later) could not reach them. That was not enough. The app database's
`public` also holds tables that were never meant to be queryable by the public:
the raw free-text questions people typed (nl_query_log, nl_query_cache,
nl_suggest_log), CBS feedback, unpublished admin drafts (decision_analysis),
Drive export targets, worker IP addresses (workers, scrape_tasks), and
tracked_datasets, which would reveal datasets whose status is 'hidden'.

On Neon none of that is reachable, because the console runs against a different
database. On xhostd there is one database, so this migration restores the
guarantee the two-database layout gave for free: `public` holds published data
and nothing else. Every table the application owns moves to `app`, which the
read-only role is never granted.

HOW THE APPLICATION STILL FINDS THEM
------------------------------------
No SQL in the application names a schema for these tables. The app engine sets
`search_path = app, public` on connect (app/database.py:install_app_search_path), so both the
ORM and the raw SQL resolve to `app`, and a table a future migration creates
without naming a schema lands in `app` too. The archive pools do not set it, so
append tables keep landing in `public`. Startup refuses a shared database where
the engine's current schema is not `app` (app/main.py).

`search_path = app, public` is backward compatible in both directions: before
this migration `app` is empty or absent and names fall through to `public`.

The list is written out rather than read from the models: seven of these tables
(activity_log, govmap_coverage and five nl_* tables) have no ORM model, and a
migration must not change meaning when the models do. tests/test_app_schema.py
pins it against app/database.py:APP_TABLES.
"""
from alembic import op

revision = "066"
down_revision = "065"
branch_labels = None
depends_on = None

TABLES = (
    "activity_log",
    "cbs_featured",
    "cbs_feedback",
    "cbs_gazetteer",
    "cbs_index",
    "dataset_tags",
    "datastore_push_jobs",
    "decision_analysis",
    "drive_export_jobs",
    "govmap_coverage",
    "llm_daily_usage",
    "nl_query_cache",
    "nl_query_config",
    "nl_query_log",
    "nl_suggest_log",
    "nl_synonyms",
    "organizations",
    "page_content",
    "scrape_tasks",
    "source_limits",
    "source_registry",
    "sql_shares",
    "tags",
    "tracked_datasets",
    "version_index",
    "workers",
)


def upgrade() -> None:
    op.execute("CREATE SCHEMA IF NOT EXISTS app")
    for table in TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF to_regclass('public.{table}') IS NOT NULL
                 AND to_regclass('app.{table}') IS NULL THEN
                EXECUTE 'ALTER TABLE public.{table} SET SCHEMA app';
              END IF;
            END $$;
            """
        )
    op.execute("REVOKE ALL ON SCHEMA app FROM PUBLIC")


def downgrade() -> None:
    for table in TABLES:
        op.execute(
            f"""
            DO $$
            BEGIN
              IF to_regclass('app.{table}') IS NOT NULL THEN
                EXECUTE 'ALTER TABLE app.{table} SET SCHEMA public';
              END IF;
            END $$;
            """
        )
    # Plain DROP, not CASCADE: if anything else was created in `app` meanwhile,
    # the downgrade stops here instead of destroying it.
    op.execute("DROP SCHEMA IF EXISTS app")
