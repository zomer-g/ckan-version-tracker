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

LOCKING: ONE TABLE PER TRANSACTION, PATIENT, AND SAYS WHO BLOCKED IT
--------------------------------------------------------------------
The first version moved all 26 tables in one transaction and deadlocked against
the live site on its first production run: it held ACCESS EXCLUSIVE on every
table it had moved while waiting for `organizations`, and a request holding
`organizations` asked for one of those. Postgres rolled it back whole.

So each move is its own transaction (autocommit) holding exactly one table's
lock, which cannot form a cycle. It waits at most 2 seconds for that lock, then
releases, sleeps a second and retries, so a long reader delays the move rather
than queueing every other reader behind it for longer than 2 seconds.

The second production run moved five tables and then spent all of 60 attempts
(three minutes) on dataset_tags, held by a transaction that had ended by the
time anyone looked. So the budget is now 300 attempts (about 15 minutes; waiting
costs the site nothing), and when it still runs out the error names every
session holding the table, with its state, transaction age and query, so the
build log says what to fix. OVER_066_LOCK_ATTEMPTS overrides the budget, for
tests.

A half-finished run is safe: `app, public` resolves both halves, and re-running
skips what already moved.
"""
import os

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

LOCK_TIMEOUT = "2s"


def _attempts() -> int:
    return max(1, int(os.environ.get("OVER_066_LOCK_ATTEMPTS", "300")))


def _move(table: str, src: str, dst: str) -> str:
    attempts = _attempts()
    return rf"""
    DO $$
    DECLARE
      attempt int := 0;
      holders text;
    BEGIN
      IF to_regclass('{src}.{table}') IS NULL OR to_regclass('{dst}.{table}') IS NOT NULL THEN
        RETURN;
      END IF;
      LOOP
        BEGIN
          SET LOCAL lock_timeout = '{LOCK_TIMEOUT}';
          EXECUTE 'ALTER TABLE {src}.{table} SET SCHEMA {dst}';
          RETURN;
        EXCEPTION WHEN lock_not_available OR deadlock_detected THEN
          attempt := attempt + 1;
          IF attempt >= {attempts} THEN
            SELECT string_agg(format('pid=%s app=%s state=%s xact_age=%s query=%s',
                                     a.pid, coalesce(nullif(a.application_name, ''), '-'), a.state,
                                     date_trunc('second', now() - a.xact_start),
                                     left(regexp_replace(a.query, '\s+', ' ', 'g'), 200)), ' | ')
              INTO holders
              FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
             WHERE l.relation = to_regclass('{src}.{table}') AND l.granted
               AND a.pid <> pg_backend_pid();
            RAISE EXCEPTION 'could not lock {src}.{table} after % attempts; held by: %',
              attempt, coalesce(holders, 'no session visible now')
              USING ERRCODE = 'lock_not_available';
          END IF;
          PERFORM pg_sleep(1);
        END;
      END LOOP;
    END $$;
    """


def upgrade() -> None:
    with op.get_context().autocommit_block():
        op.execute("CREATE SCHEMA IF NOT EXISTS app")
        op.execute("REVOKE ALL ON SCHEMA app FROM PUBLIC")
        for table in TABLES:
            op.execute(_move(table, "public", "app"))


def downgrade() -> None:
    with op.get_context().autocommit_block():
        for table in TABLES:
            op.execute(_move(table, "app", "public"))
        # Plain DROP, not CASCADE: if anything else was created in `app` meanwhile,
        # the downgrade stops here instead of destroying it.
        op.execute("DROP SCHEMA IF EXISTS app")
