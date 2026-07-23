-- ============================================================================
-- Least-privilege read-only role for the PUBLIC SQL consoles (over.org.il)
-- ============================================================================
--
-- WHY: The three public SQL consoles —
--   * append_store.run_readonly_sql   (/api/append/{id}/sql)
--   * knesset_db.run_sql              (/api/knesset-db/sql)
--   * knesset_db.iter_sql_csv         (CSV export of the above)
-- run arbitrary user-supplied SELECTs against the APPEND database. Until now
-- they used the SAME role the sync/poll pipeline uses to run CREATE SCHEMA /
-- ALTER TABLE / INSERT — a role with full write privileges. The only thing
-- stopping a console query from writing was the app's READ ONLY transaction +
-- keyword denylist: a single point of failure at the application layer.
--
-- This script provisions a dedicated role that Postgres itself will refuse to
-- let write (no INSERT/UPDATE/DELETE/DDL grants, not a superuser, not a member
-- of pg_read_server_files) and that can read ONLY the console-relevant schemas
-- (public + knesset + idx). Point APPEND_READONLY_DATABASE_URL at the append DB with
-- THIS role's credentials; the app routes the consoles to it (get_readonly_pool)
-- while the rest of the system stays on the read/write role.
--
-- IDEMPOTENT: safe to re-run. Re-running rotates the password and re-asserts the
-- grants (including for tables the sync created since the last run).
--
-- HOW TO RUN — as the SAME role the worker uses (i.e. with APPEND_DATABASE_URL),
-- because ALTER DEFAULT PRIVILEGES below only auto-grants future tables created
-- by the role that runs this script:
--
--   psql "$APPEND_DATABASE_URL" \
--     -v ro_password="'choose-a-strong-password'" \
--     -f scripts/create_append_readonly_role.sql
--
--   (note the DOUBLE quoting of the password value: the outer quotes are for the
--    shell, the inner single quotes make it a SQL string literal.)
--
--   Optional: -v ro_role="my_ro_role"  (default: over_readonly)
--
-- Then set, in the Render dashboard, APPEND_READONLY_DATABASE_URL to the append
-- DB URL but with user=<ro_role> and this password.
-- ============================================================================

\set ON_ERROR_STOP on

-- Require the password; default the role name to over_readonly.
\if :{?ro_password}
\else
  \echo '>>> ERROR: pass the role password, e.g.  -v ro_password="''s3cret''"'
  \quit
\endif
\if :{?ro_role}
\else
  \set ro_role over_readonly
\endif

\echo 'Provisioning read-only role:' :'ro_role'

-- 1) Role: create if missing, else re-assert attributes + rotate password.
--    NOSUPERUSER + NOBYPASSRLS + NOCREATEDB + NOCREATEROLE + NOINHERIT keep it
--    strictly least-privilege; it is deliberately NOT granted membership in any
--    predefined role (so it is not in pg_read_server_files, pg_read_all_data…).
SELECT NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'ro_role') AS need_create \gset
\if :need_create
  CREATE ROLE :"ro_role" LOGIN PASSWORD :'ro_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
\else
  ALTER ROLE :"ro_role" WITH LOGIN PASSWORD :'ro_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS;
\endif

-- 2) Lock down schema public: revoke the default PUBLIC grant (which lets ANY
--    role CREATE objects in public), then hand the read-only role USAGE only —
--    never CREATE. USAGE lets it resolve names; without CREATE it cannot make
--    tables/functions. (We do NOT touch TEMP on the database: the sync path uses
--    CREATE TEMP TABLE in append_diff, and a temp table is private + dropped on
--    disconnect, so it is not a containment concern.)
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO :"ro_role";

-- 3) Ensure the knesset schema exists (the /knesset feature may not have synced
--    yet on a fresh DB) and grant USAGE only.
CREATE SCHEMA IF NOT EXISTS knesset;
GRANT USAGE ON SCHEMA knesset TO :"ro_role";

-- 3b) Same for idx — the index-CSV mirror's schema, which the central /data
--     console reads (CONSOLE_SEARCH_PATH = "public, knesset, idx").
--     index_mirror.ensure_schema() re-asserts these same grants on every sync
--     tick, but ONLY when APPEND_READONLY_DATABASE_URL is already set AND the
--     mirror job is enabled. Granting here removes both preconditions: the
--     console can read idx from the moment the role is provisioned, even if
--     INDEX_MIRROR_ENABLED is false (as it was after the OOM of 2026-07-22).
CREATE SCHEMA IF NOT EXISTS idx;
GRANT USAGE ON SCHEMA idx TO :"ro_role";

-- 4) SELECT on every existing table/view in the three console schemas — and ONLY
--    those. No grants on any other schema ⇒ the role cannot read outside them.
GRANT SELECT ON ALL TABLES IN SCHEMA public  TO :"ro_role";
GRANT SELECT ON ALL TABLES IN SCHEMA knesset TO :"ro_role";
GRANT SELECT ON ALL TABLES IN SCHEMA idx     TO :"ro_role";

-- 5) Auto-grant SELECT on FUTURE tables the sync pipeline creates, so a newly
--    tracked dataset / new Knesset entity set is immediately queryable by the
--    console without re-running this script. Applies to tables created by the
--    role running this script (= the worker's role — see the run instructions).
--    This matters MOST for idx: every mirror sync builds a fresh staging table
--    and swaps it in (DROP + RENAME), so each synced dataset is a NEW table that
--    would otherwise lose the grant from step 4 on its next refresh.
ALTER DEFAULT PRIVILEGES IN SCHEMA public  GRANT SELECT ON TABLES TO :"ro_role";
ALTER DEFAULT PRIVILEGES IN SCHEMA knesset GRANT SELECT ON TABLES TO :"ro_role";
ALTER DEFAULT PRIVILEGES IN SCHEMA idx     GRANT SELECT ON TABLES TO :"ro_role";

-- 6) Belt-and-braces: make sure no stray write privileges linger on existing
--    objects (e.g. from a previous over-broad grant). SELECT stays (re-granted
--    above); INSERT/UPDATE/DELETE/TRUNCATE/REFERENCES/TRIGGER are stripped.
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA public  FROM :"ro_role";
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA knesset FROM :"ro_role";
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA idx     FROM :"ro_role";

-- ── Verification (printed; the script does not fail on these, they are FYI) ──
\echo ''
\echo 'Verification — expect: is_superuser=f, bypassrls=f, reads_server_files=f'
SELECT rolname,
       rolsuper                                                        AS is_superuser,
       rolbypassrls                                                    AS bypassrls,
       pg_has_role(rolname, 'pg_read_server_files', 'MEMBER')          AS reads_server_files,
       pg_has_role(rolname, 'pg_read_all_data', 'MEMBER')             AS reads_all_data
FROM pg_roles WHERE rolname = :'ro_role';

\echo 'Verification — schemas the role may USE (expect only public + knesset + idx):'
SELECT nspname AS schema
FROM pg_namespace
WHERE has_schema_privilege(:'ro_role', nspname, 'USAGE')
  AND nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname NOT LIKE 'pg_%'
ORDER BY nspname;

\echo 'Done. Set APPEND_READONLY_DATABASE_URL to this DB with user=' :'ro_role'
