-- ============================================================================
-- Read-only console role — PASTE-READY variant for the Neon SQL Editor
-- ============================================================================
--
-- Same effect as create_append_readonly_role.sql, minus the psql meta-commands
-- (\set, \if, \gset, \echo) which the Neon web editor cannot run. Use this when
-- psql is not installed. KEEP THE GRANTS IN SYNC with the psql script — that one
-- is canonical; this is a transcription of it.
--
-- HOW TO RUN
--   1. Open the Neon console → your project → the APPEND database → SQL Editor.
--   2. ⚠ CHECK WHICH ROLE YOU ARE. Run:  SELECT current_user;
--      It MUST be the same role as in APPEND_DATABASE_URL (the worker's role).
--      Why: ALTER DEFAULT PRIVILEGES below only auto-grants tables created by
--      the role that RUNS it. Run this as a different role and today's grants
--      work, but every table a FUTURE sync creates is invisible to the console —
--      a failure that shows up days later and looks like a mirror bug.
--   3. Replace the password on the marked line with a strong one you generated
--      locally (see below). Do not reuse any existing password.
--   4. Run the whole script. Then run the verification block at the bottom.
--   5. In Render, set APPEND_READONLY_DATABASE_URL to the SAME connection string
--      as APPEND_DATABASE_URL but with the user swapped to over_readonly and
--      this password. Same host, same database, different user. Copying
--      APPEND_DATABASE_URL verbatim silently defeats the entire purpose.
--
-- GENERATING THE PASSWORD (run locally, never paste it into a chat/issue):
--   PowerShell:
--     [Convert]::ToBase64String([Security.Cryptography.RandomNumberGenerator]::GetBytes(24)) -replace '[+/=]','x'
--   bash:
--     openssl rand -base64 24 | tr -d '+/='
--
-- IDEMPOTENT: safe to re-run. Re-running rotates the password (update Render
-- afterwards) and re-asserts every grant.
-- ============================================================================


-- 1) Role: create if missing, else re-assert attributes + rotate the password.
--    NOSUPERUSER + NOBYPASSRLS + NOCREATEDB + NOCREATEROLE + NOINHERIT keep it
--    strictly least-privilege. Deliberately NOT a member of any predefined role
--    (so not pg_read_server_files, not pg_read_all_data).
DO $do$
DECLARE
  -- ↓↓↓ THE ONLY LINE YOU EDIT ↓↓↓
  ro_pw text := 'CHANGE_ME_TO_A_GENERATED_PASSWORD';
  -- ↑↑↑ THE ONLY LINE YOU EDIT ↑↑↑
  attrs text := 'NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS';
BEGIN
  IF ro_pw = 'CHANGE_ME_TO_A_GENERATED_PASSWORD' THEN
    RAISE EXCEPTION 'Set a real password first — see the header of this script.';
  END IF;

  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'over_readonly') THEN
    EXECUTE format('ALTER ROLE over_readonly WITH LOGIN PASSWORD %L %s', ro_pw, attrs);
  ELSE
    EXECUTE format('CREATE ROLE over_readonly LOGIN PASSWORD %L %s', ro_pw, attrs);
  END IF;
END
$do$;


-- 2) Lock down schema public: revoke the blanket PUBLIC grant (which lets ANY
--    role CREATE objects there), then hand the read-only role USAGE only —
--    never CREATE. USAGE resolves names; without CREATE it cannot make objects.
--    (TEMP on the database is deliberately left alone: the sync path uses
--    CREATE TEMP TABLE in append_diff, and a temp table is private + dropped on
--    disconnect, so it is not a containment concern.)
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO over_readonly;

-- 3) The other two console schemas. CREATE IF NOT EXISTS because either may not
--    exist yet on a fresh DB — knesset until /knesset first syncs, idx until the
--    index-CSV mirror first runs.
CREATE SCHEMA IF NOT EXISTS knesset;
GRANT USAGE ON SCHEMA knesset TO over_readonly;

CREATE SCHEMA IF NOT EXISTS idx;
GRANT USAGE ON SCHEMA idx TO over_readonly;

-- 3c) `extensions` — where PostGIS is installed, kept out of `public` so its
--     ~1,000 functions and spatial_ref_sys do not flood the console's schema
--     reference. It is on CONSOLE_SEARCH_PATH, so without USAGE here every
--     ST_* call in the console fails to resolve. Deliberately NOT given the
--     default-privileges treatment of the data schemas below: nothing of ours
--     is ever created here, and future objects belong to the extension.
CREATE SCHEMA IF NOT EXISTS extensions;
GRANT USAGE ON SCHEMA extensions TO over_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA extensions TO over_readonly;

-- 4) SELECT on every existing table/view in the three DATA schemas — and ONLY
--    those. No grants on any other schema ⇒ the role cannot read outside them.
GRANT SELECT ON ALL TABLES IN SCHEMA public  TO over_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA knesset TO over_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA idx     TO over_readonly;

-- 5) Auto-grant SELECT on FUTURE tables, so a newly tracked dataset / Knesset
--    entity set / mirrored index is queryable without re-running this script.
--    Load-bearing for idx: every mirror sync builds a fresh staging table and
--    swaps it in (DROP + RENAME), so each refresh is a NEW table that would
--    otherwise lose the grant from step 4.
ALTER DEFAULT PRIVILEGES IN SCHEMA public  GRANT SELECT ON TABLES TO over_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA knesset GRANT SELECT ON TABLES TO over_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA idx     GRANT SELECT ON TABLES TO over_readonly;

-- 6) Belt-and-braces: strip any stray write privileges left by an earlier
--    over-broad grant. SELECT stays (re-granted in step 4).
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA public  FROM over_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA knesset FROM over_readonly;
REVOKE INSERT, UPDATE, DELETE, TRUNCATE, REFERENCES, TRIGGER
  ON ALL TABLES IN SCHEMA idx     FROM over_readonly;


-- ── VERIFICATION — run these separately and read the results ────────────────

-- (a) Did you run this as the worker's role? If this does NOT match the user in
--     APPEND_DATABASE_URL, re-run the whole script as that role (see header §2).
SELECT current_user AS ran_as;

-- (b) Least-privilege attributes. Expect f / f / f / f.
SELECT rolname,
       rolsuper                                               AS is_superuser,
       rolbypassrls                                           AS bypassrls,
       pg_has_role(rolname, 'pg_read_server_files', 'MEMBER') AS reads_server_files,
       pg_has_role(rolname, 'pg_read_all_data', 'MEMBER')     AS reads_all_data
FROM pg_roles WHERE rolname = 'over_readonly';

-- (c) Schemas the role may USE. Expect EXACTLY: idx, knesset, public.
SELECT nspname AS schema
FROM pg_namespace
WHERE has_schema_privilege('over_readonly', nspname, 'USAGE')
  AND nspname NOT IN ('pg_catalog', 'information_schema')
  AND nspname NOT LIKE 'pg_%'
ORDER BY nspname;

-- (d) Readable tables per console schema. All three should be > 0 once the
--     respective feature has synced (idx = 0 is fine if the mirror never ran).
SELECT table_schema, count(*) AS readable_tables
FROM information_schema.tables
WHERE table_schema IN ('public', 'knesset', 'idx')
  AND table_type = 'BASE TABLE'
  AND has_table_privilege('over_readonly',
        quote_ident(table_schema) || '.' || quote_ident(table_name), 'SELECT')
GROUP BY table_schema ORDER BY table_schema;
