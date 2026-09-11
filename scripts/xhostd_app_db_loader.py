"""Copy OVER's app database (schemas `app` and `auth`) from Neon into the channel database.

The archive is already here (scripts/xhostd_archive_loader.py). The app database
is small (app 465 MB, auth 1.4 MB) but keeps changing while the site serves from
Neon (sign-ins, question logs, feedback), so it is copied twice: once now, so the
xhostd app can be pointed at its own database and checked end to end on its
temporary host, and once more inside the switch window, with writes paused.

Every run is a full refresh, which is what makes a second run trivial to reason
about: drop both schemas, recreate them from pg_dump, copy every table with row
counts checked, set sequences, build indexes and foreign keys, revoke PUBLIC,
and record alembic_version. A run is tied to APP_DB_LOADER=<token> and recorded
as done, so it happens once; the switch window sets a new token.

It refuses to drop anything while the application itself is using the channel
database, unless MAINTENANCE_MODE=true has stopped the writes.

The only adaptation: pg_trgm already lives in schema `ocal` here (the archive
brought it), and a database holds an extension once. On Neon it is in `public`
and used by exactly one app index, app.ix_cbs_index_title_trgm, and by no query,
so the index is created with ocal.gin_trgm_ops.

Env: APP_DB_SOURCE_URL (Neon app database, owner role), XHOST_LOCAL_DATABASE_URL,
APP_DB_LOADER (the run token), DATABASE_URL (what the app uses after
xhostd-env.sh), MAINTENANCE_MODE.
"""
import asyncio
import os
import re
import sys
import time
from urllib.parse import urlsplit

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import xhostd_archive_loader as L  # noqa: E402

SCHEMAS = ("app", "auth")


def log(msg: str) -> None:
    print(f"[app-db] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


def adapt_post_data(sql: str) -> str:
    """Point trigram operator classes at the schema pg_trgm lives in on this database."""
    return re.sub(r"\bpublic\.(gin|gist)_trgm_ops\b", r"ocal.\1_trgm_ops", sql)


def clean_pre_data(sql: str) -> str:
    out = []
    for line in sql.splitlines():
        if re.match(r"^(CREATE EXTENSION|COMMENT ON EXTENSION)", line):
            continue  # extensions already exist here (pg_trgm in ocal)
        out.append(line)
    return "\n".join(out) + "\n"


def same_host(a: str, b: str) -> bool:
    ua, ub = urlsplit(L.libpq_url(a)), urlsplit(L.libpq_url(b))
    return (ua.hostname, ua.port or 5432, ua.path) == (ub.hostname, ub.port or 5432, ub.path)


async def main() -> None:
    token = (os.environ.get("APP_DB_LOADER") or "").strip()
    src_raw = os.environ.get("APP_DB_SOURCE_URL", "")
    dst_raw = os.environ.get("XHOST_LOCAL_DATABASE_URL", "")
    if not token or not src_raw or not dst_raw:
        log("APP_DB_LOADER, APP_DB_SOURCE_URL and XHOST_LOCAL_DATABASE_URL are required; not starting")
        return
    src_url = L.libpq_url(src_raw, direct=True)
    dst_url = L.libpq_url(dst_raw)
    phase = f"app-db:{token}"

    app_uses_local = same_host(os.environ.get("DATABASE_URL", ""), dst_raw)
    maintenance = (os.environ.get("MAINTENANCE_MODE") or "").strip().lower() in ("1", "true", "yes")

    dst = await L.connect(dst_url)
    try:
        await dst.execute("CREATE SCHEMA IF NOT EXISTS _loader")
        await dst.execute("CREATE TABLE IF NOT EXISTS _loader.phase (name text PRIMARY KEY, done_at timestamptz)")
        if await L.phase_done(dst, phase):
            log(f"token {token!r} already done; nothing to do")
            return
        if app_uses_local and not maintenance:
            log("REFUSED: the app is using this database and MAINTENANCE_MODE is off; "
                "set MAINTENANCE_MODE=true for a resync")
            return

        log(f"start token={token!r}: {urlsplit(src_url).hostname} -> {urlsplit(dst_url).hostname}"
            f" (app uses local: {app_uses_local}, maintenance: {maintenance})")
        t0 = time.monotonic()
        for s in SCHEMAS:
            await dst.execute(f'DROP SCHEMA IF EXISTS "{s}" CASCADE')

        schema_args = [f"--schema={s}" for s in SCHEMAS]
        rc, dump, err = await L.run([f"{L.BIN}/pg_dump", "--schema-only", "--section=pre-data",
                                     "--no-owner", "--no-privileges", *schema_args, src_url])
        if rc != 0:
            raise RuntimeError(f"pg_dump pre-data: {err[-600:]}")
        rc, _, err = await L.run([f"{L.BIN}/psql", dst_url, "-X", "-q", "-v", "ON_ERROR_STOP=1",
                                  "--single-transaction"], stdin_text=clean_pre_data(dump))
        if rc != 0:
            raise RuntimeError(f"psql pre-data: {err[-600:]}")
        log("pre-data applied")

        src = await L.connect(src_url)
        try:
            tables = await src.fetch(
                "SELECT n.nspname AS schema, c.relname AS name FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE c.relkind = 'r' AND n.nspname = ANY($1::text[]) ORDER BY 1, 2", list(SCHEMAS))
            mismatches = []
            for t in tables:
                await L.copy_table(src_url, dst_url, t["schema"], t["name"])
                q = L.qualified(t["schema"], t["name"])
                a = await src.fetchval(f"SELECT count(*) FROM {q}")
                b = await dst.fetchval(f"SELECT count(*) FROM {q}")
                if a != b:
                    mismatches.append(f"{q} {a} vs {b}")
            if mismatches:
                raise RuntimeError(f"row counts differ: {mismatches[:5]}")
            total = await dst.fetchval(
                "SELECT sum(n_live_tup) FROM pg_stat_user_tables WHERE schemaname = ANY($1::text[])", list(SCHEMAS))
            log(f"data: {len(tables)} tables copied, row counts equal")

            seqs = await src.fetch(
                "SELECT schemaname AS schema, sequencename AS name, last_value FROM pg_sequences "
                "WHERE schemaname = ANY($1::text[]) AND last_value IS NOT NULL", list(SCHEMAS))
            for r in seqs:
                await dst.execute("SELECT setval($1::regclass, $2, true)", L.qualified(r["schema"], r["name"]),
                                  r["last_value"])
            log(f"sequences: {len(seqs)} set")

            rc, dump, err = await L.run([f"{L.BIN}/pg_dump", "--schema-only", "--section=post-data",
                                         "--no-owner", "--no-privileges", *schema_args, src_url])
            if rc != 0:
                raise RuntimeError(f"pg_dump post-data: {err[-600:]}")
            items = L.split_post_data(adapt_post_data(dump))
            for _, stmt in items:
                await dst.execute(stmt)
            log(f"post-data: {len(items)} objects created")

            for s in SCHEMAS:
                await dst.execute(f'REVOKE ALL ON SCHEMA "{s}" FROM PUBLIC')

            version = await src.fetchval("SELECT version_num FROM public.alembic_version")
            await dst.execute("CREATE TABLE IF NOT EXISTS public.alembic_version ("
                              "version_num varchar(32) NOT NULL, "
                              "CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num))")
            async with dst.transaction():
                await dst.execute("DELETE FROM public.alembic_version")
                await dst.execute("INSERT INTO public.alembic_version (version_num) VALUES ($1)", version)
            log(f"alembic_version: {version}")

            for t in tables:
                await dst.execute(f"ANALYZE {L.qualified(t['schema'], t['name'])}")

            checks = {}
            for name, sql in {
                "fks": "SELECT count(*) FROM pg_constraint k JOIN pg_class c ON c.oid = k.conrelid "
                       "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE k.contype = 'f' AND n.nspname = ANY($1::text[])",
                "indexes": "SELECT count(*) FROM pg_indexes WHERE schemaname = ANY($1::text[])",
                "tables": "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                          "WHERE c.relkind = 'r' AND n.nspname = ANY($1::text[])",
            }.items():
                a = await src.fetchval(sql, list(SCHEMAS))
                b = await dst.fetchval(sql, list(SCHEMAS))
                checks[name] = (a, b)
            log("verify: " + ", ".join(f"{k} {a}/{b}" for k, (a, b) in checks.items()))
            if any(a != b for a, b in checks.values()):
                raise RuntimeError(f"structure differs: {checks}")
        finally:
            await src.close()

        await L.mark_phase(dst, phase)
        log(f"COMPLETE token={token!r} in {time.monotonic() - t0:.0f}s, ~{total} rows")
    except Exception as e:  # noqa: BLE001
        log(f"STOPPED: {str(e)[:800]}")
    finally:
        await dst.close()


if __name__ == "__main__":
    asyncio.run(main())
