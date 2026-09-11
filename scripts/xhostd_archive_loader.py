"""Copy OVER's archive database from Neon into this xhostd channel's own Postgres.

Runs in the background inside the `over` app on xhostd (launch.sh starts it
when ARCHIVE_LOADER=run), because the channel's database is reachable only from
inside the container. The site keeps serving from Neon the whole time.

Everything is resumable: a redeploy kills this process, and the next boot
carries on from the state it keeps in schema `_loader` (never granted to the
console role).

Phases, each recorded once done:
  1. pre-data   schema, types, functions, tables (no indexes yet), from pg_dump
                --section=pre-data. postgres_fdw is dropped: xhostd refuses it
                and nothing uses it (no foreign tables on Neon).
  2. data       each table as a psql | psql binary COPY pipe (copy_script),
                smallest first, PARALLEL at a time. Each is its own short read on
                Neon, so no transaction sits open for hours in front of the live
                site. TRUNCATE and COPY share one transaction on the target, so an
                interrupted table leaves nothing. Row counts must match.
     sequences  positions carried over, since COPY does not set them.
  3. post-data  indexes and constraints, then foreign keys, triggers and
                materialized view data. An object that already exists is taken
                as done, so an interrupted run resumes.
  4. analyze
  5. grants     the channel's read-only role gets USAGE and SELECT on the data
                schemas, and SELECT is revoked on every table the console role
                cannot read on Neon today (a hidden table must stay hidden; the
                platform grants all of `public` by default).

Not copied: pilot_idx and idx_backup (retired, ~5.2 GB). OVER is under
VERSION_FREEZE while this runs, so the archive does not change underneath it.

Env: APPEND_DATABASE_URL (Neon archive, owner role), XHOST_LOCAL_DATABASE_URL
(the channel's injected DATABASE_URL, saved by launch.sh before xhostd-env.sh
remaps it), DATABASE_URL_READONLY (the channel's read-only role).
"""
import asyncio
import os
import re
import sys
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

BIN = "/usr/lib/postgresql/18/bin"
EXCLUDE_SCHEMAS = ("pilot_idx", "idx_backup")
GRANT_SCHEMAS = ("public", "extensions", "idx", "knesset", "ocal", "ocoi", "odata")
BLOCKED_EXTENSIONS = ("postgres_fdw",)
PARALLEL = int(os.environ.get("ARCHIVE_LOADER_PARALLEL", "3"))
INDEX_PARALLEL = int(os.environ.get("ARCHIVE_LOADER_INDEX_PARALLEL", "3"))


def log(msg: str) -> None:
    print(f"[loader] {time.strftime('%Y-%m-%d %H:%M:%S')} {msg}", flush=True)


# ── pure helpers (tested in tests/test_xhostd_archive_loader.py) ─────────────

def libpq_url(url: str, *, direct: bool = False) -> str:
    """A URL pg_dump/psql accept. `direct` swaps a Neon pooler host for the
    direct endpoint: pg_dump needs a real session, not PgBouncer transaction mode."""
    u = urlsplit((url or "").strip())
    scheme = "postgresql"
    netloc = u.netloc
    if direct:
        netloc = netloc.replace("-pooler.", ".")
    return urlunsplit((scheme, netloc, u.path, u.query, ""))


def asyncpg_dsn(url: str, *, direct: bool = False) -> str:
    u = urlsplit(libpq_url(url, direct=direct))
    q = [(k, v) for k, v in parse_qsl(u.query) if k.lower() not in ("sslmode", "channel_binding", "options")]
    return urlunsplit((u.scheme, u.netloc, u.path, urlencode(q), ""))


def qualified(schema: str, name: str) -> str:
    return '"' + schema.replace('"', '""') + '"."' + name.replace('"', '""') + '"'


_TOC = re.compile(r"^-- Name: .*?; Type: (?P<type>[A-Z ]+?); Schema: .*$", re.M)
_TOC_BLOCK = re.compile(r"^--\n-- Name: (?P<name>.*?); Type: (?P<type>[A-Z ]+?); Schema: .*\n--\n", re.M)
# Foreign-data objects are dropped whole: xhostd refuses postgres_fdw, and on Neon
# the only ones are a leftover server `ocal_srv` and its user mapping, with no
# foreign table using them.
_DROP_TYPES = ("SERVER", "USER MAPPING", "FOREIGN TABLE", "FOREIGN DATA WRAPPER")


def clean_pre_data(sql: str) -> str:
    """Make pg_dump's pre-data section apply to a platform database we do not superuse."""
    kept = []
    blocks = list(_TOC_BLOCK.finditer(sql))
    kept.append(sql[:blocks[0].start()] if blocks else sql)
    for i, m in enumerate(blocks):
        end = blocks[i + 1].start() if i + 1 < len(blocks) else len(sql)
        kind, name = m.group("type").strip(), m.group("name")
        if kind in _DROP_TYPES:
            continue
        if kind in ("EXTENSION", "COMMENT") and any(ext in name for ext in BLOCKED_EXTENSIONS):
            continue
        kept.append(sql[m.start():end])
    out = []
    for line in "".join(kept).splitlines():
        if any(ext in line for ext in BLOCKED_EXTENSIONS) and re.match(
                r"^(CREATE EXTENSION|COMMENT ON EXTENSION)", line):
            continue
        if line.startswith("COMMENT ON SCHEMA public "):
            continue  # owned by the platform, not by us
        m = re.match(r"^CREATE SCHEMA (?!IF NOT EXISTS)(.+);$", line)
        if m:
            line = f"CREATE SCHEMA IF NOT EXISTS {m.group(1)};"
        out.append(line)
    return "\n".join(out) + "\n"


def split_post_data(sql: str) -> list[tuple[str, str]]:
    """pg_dump's post-data section as (type, statement) in dump order."""
    items = []
    matches = list(_TOC.finditer(sql))
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(sql)
        body = "\n".join(
            line for line in sql[m.end():end].splitlines()
            if line.strip() and not line.startswith("--") and not line.startswith("\\")
        ).strip()
        if body:
            items.append((m.group("type").strip(), body))
    return items


# ── database plumbing ───────────────────────────────────────────────────────

async def connect(url: str, *, direct: bool = False):
    import asyncpg
    from app.pg_ssl import asyncpg_ssl_for
    dsn = asyncpg_dsn(url, direct=direct)
    return await asyncpg.connect(dsn, ssl=asyncpg_ssl_for(dsn), statement_cache_size=0,
                                 command_timeout=None, timeout=60)


async def run(args: list[str], *, stdin_text: str | None = None, env: dict | None = None) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *args, stdin=asyncio.subprocess.PIPE if stdin_text is not None else None,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env)
    out, err = await proc.communicate(stdin_text.encode("utf-8") if stdin_text is not None else None)
    return proc.returncode, out.decode("utf-8", "replace"), err.decode("utf-8", "replace")


def copy_script() -> str:
    """One table, Neon to the channel database, as a psql | psql pipe in binary COPY.

    History, measured on the real run. pg_dump | psql per table spent ~10 s a
    table re-reading Neon's whole catalog on every start. Streaming through
    asyncpg on two open connections removed that but stalled on sub-megabyte
    tables for minutes while Neon sat idle in ClientRead, with the bytes stuck in
    the Python handoff. Plain psql on both ends skips the catalog and streams in C.

    The table name travels in an environment variable and is only ever expanded
    inside double quotes, so no name is re-parsed by the shell. TRUNCATE and COPY
    share one target transaction (--single-transaction, ON_ERROR_STOP). If the
    source dies midway, pipefail fails the table. A stream cut inside a row makes
    the target COPY error and roll back; one cut exactly between rows can commit,
    which is why the guarantee is not this pipe but the caller: a failed table is
    recorded failed and copied again from TRUNCATE on the next run, and a table
    is marked done only when its row counts match Neon's.
    """
    return (
        'set -o pipefail; '
        f'"{BIN}/psql" "$LOADER_SRC" -X -q -v ON_ERROR_STOP=1 '
        '-c "COPY $LOADER_TABLE TO STDOUT (FORMAT binary)" '
        f'| "{BIN}/psql" "$LOADER_DST" -X -q -v ON_ERROR_STOP=1 --single-transaction '
        '-c "TRUNCATE $LOADER_TABLE" -c "COPY $LOADER_TABLE FROM STDIN (FORMAT binary)"'
    )


async def copy_table(src_url: str, dst_url: str, schema: str, name: str) -> None:
    env = {**os.environ, "LOADER_TABLE": qualified(schema, name), "LOADER_SRC": src_url, "LOADER_DST": dst_url}
    rc, _, err = await run(["bash", "-c", copy_script()], env=env)
    if rc != 0:
        raise RuntimeError(err.strip()[-600:] or f"copy pipe exited {rc}")


async def sequences(src, dst) -> None:
    """Carry sequence positions over. pg_dump did this per table; binary COPY does not."""
    if await phase_done(dst, "sequences"):
        return
    seqs = await src.fetch(
        "SELECT schemaname AS schema, sequencename AS name, last_value FROM pg_sequences "
        "WHERE schemaname <> ALL($1::text[]) AND last_value IS NOT NULL", list(EXCLUDE_SCHEMAS))
    moved = 0
    for r in seqs:
        q = qualified(r["schema"], r["name"])
        if await dst.fetchval("SELECT to_regclass($1) IS NOT NULL", q):
            await dst.execute("SELECT setval($1::regclass, $2, true)", q, r["last_value"])
            moved += 1
    log(f"sequences: {moved} set")
    await mark_phase(dst, "sequences")


async def phase_done(dst, name: str) -> bool:
    return bool(await dst.fetchval("SELECT 1 FROM _loader.phase WHERE name = $1", name))


async def mark_phase(dst, name: str) -> None:
    await dst.execute("INSERT INTO _loader.phase (name, done_at) VALUES ($1, now()) "
                      "ON CONFLICT (name) DO UPDATE SET done_at = now()", name)
    log(f"phase {name}: done")


# ── phases ──────────────────────────────────────────────────────────────────

async def pre_data(src_url: str, dst_url: str, dst) -> None:
    if await phase_done(dst, "pre-data"):
        return
    excl = [f"--exclude-schema={s}" for s in EXCLUDE_SCHEMAS]
    rc, dump, err = await run([f"{BIN}/pg_dump", "--schema-only", "--section=pre-data",
                               "--no-owner", "--no-privileges", *excl, src_url])
    if rc != 0:
        raise RuntimeError(f"pg_dump pre-data failed: {err[-800:]}")
    rc, _, err = await run([f"{BIN}/psql", dst_url, "-v", "ON_ERROR_STOP=1", "-q", "--single-transaction"],
                           stdin_text=clean_pre_data(dump))
    if rc != 0:
        raise RuntimeError(f"psql pre-data failed: {err[-800:]}")
    await mark_phase(dst, "pre-data")


async def data(src_url: str, dst_url: str, src, dst) -> None:
    if await phase_done(dst, "data"):
        return
    rows = await src.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name, pg_total_relation_size(c.oid) AS bytes
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r'
          AND n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_%'
          AND n.nspname <> ALL($1::text[])
          AND NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'e')
        ORDER BY pg_total_relation_size(c.oid)
        """, list(EXCLUDE_SCHEMAS))
    await dst.executemany(
        "INSERT INTO _loader.tables (schema, name, src_bytes, status) VALUES ($1, $2, $3, 'pending') "
        "ON CONFLICT (schema, name) DO NOTHING",
        [(r["schema"], r["name"], r["bytes"]) for r in rows])
    todo = await dst.fetch("SELECT schema, name, src_bytes, status FROM _loader.tables "
                           "WHERE status <> 'done' ORDER BY src_bytes")
    total = len(rows)
    log(f"data: {total} tables, {total - len(todo)} already done, {len(todo)} to copy, "
        f"{sum(r['bytes'] for r in rows) / 1024**3:.1f} GiB at the source")

    queue: asyncio.Queue = asyncio.Queue()
    for r in todo:
        queue.put_nowait(r)
    progress = {"n": total - len(todo), "bytes": 0}
    started = time.monotonic()
    failures: list[str] = []

    async def worker(i: int) -> None:
        s = await connect(src_url)
        d = await connect(dst_url)
        try:
            while True:
                try:
                    t = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                table = qualified(t["schema"], t["name"])
                t0 = time.monotonic()
                try:
                    await d.execute("UPDATE _loader.tables SET status = 'running', updated_at = now() "
                                    "WHERE schema = $1 AND name = $2", t["schema"], t["name"])
                    await copy_table(src_url, dst_url, t["schema"], t["name"])
                    src_rows = await s.fetchval(f"SELECT count(*) FROM {table}")
                    dst_rows = await d.fetchval(f"SELECT count(*) FROM {table}")
                    status = "done" if src_rows == dst_rows else "mismatch"
                    secs = time.monotonic() - t0
                    await d.execute(
                        "UPDATE _loader.tables SET status = $3, src_rows = $4, dst_rows = $5, seconds = $6, "
                        "error = NULL, updated_at = now() WHERE schema = $1 AND name = $2",
                        t["schema"], t["name"], status, src_rows, dst_rows, round(secs, 1))
                    progress["n"] += 1
                    progress["bytes"] += t["src_bytes"] or 0
                    if status != "done":
                        failures.append(f"{table} rows {src_rows} vs {dst_rows}")
                    if status != "done" or (t["src_bytes"] or 0) > 200 * 1024**2 or progress["n"] % 100 == 0:
                        rate = progress["bytes"] / 1024**2 / max(time.monotonic() - started, 1)
                        log(f"data {progress['n']}/{total} {status} {table} rows={dst_rows} "
                            f"{(t['src_bytes'] or 0) / 1024**2:.0f} MB in {secs:.0f}s (avg {rate:.1f} MB/s)")
                except Exception as e:  # noqa: BLE001
                    failures.append(f"{table}: {e}")
                    log(f"data FAILED {table}: {str(e)[:400]}")
                    await d.execute("UPDATE _loader.tables SET status = 'failed', error = $3, updated_at = now() "
                                    "WHERE schema = $1 AND name = $2", t["schema"], t["name"], str(e)[:2000])
        finally:
            await s.close()
            await d.close()

    await asyncio.gather(*(worker(i) for i in range(PARALLEL)))
    if failures:
        raise RuntimeError(f"data: {len(failures)} table(s) not copied cleanly, first: {failures[0][:300]}")
    await mark_phase(dst, "data")


async def post_data(src_url: str, dst_url: str, dst) -> None:
    if await phase_done(dst, "post-data"):
        return
    excl = [f"--exclude-schema={s}" for s in EXCLUDE_SCHEMAS]
    rc, dump, err = await run([f"{BIN}/pg_dump", "--schema-only", "--section=post-data",
                               "--no-owner", "--no-privileges", *excl, src_url])
    if rc != 0:
        raise RuntimeError(f"pg_dump post-data failed: {err[-800:]}")
    items = split_post_data(dump)
    first = [(t, s) for t, s in items if t in ("INDEX", "CONSTRAINT")]
    rest = [(t, s) for t, s in items if t not in ("INDEX", "CONSTRAINT")]
    log(f"post-data: {len(first)} indexes/constraints, then {len(rest)} other objects")

    async def apply(conn, stmt: str) -> str:
        try:
            await conn.execute(stmt)
            return "ok"
        except Exception as e:  # noqa: BLE001
            text = str(e)
            if "already exists" in text or "multiple primary keys" in text:
                return "exists"
            raise

    queue: asyncio.Queue = asyncio.Queue()
    for it in first:
        queue.put_nowait(it)
    counts = {"ok": 0, "exists": 0}
    errors: list[str] = []

    async def index_worker() -> None:
        conn = await connect(dst_url)
        await conn.execute("SET maintenance_work_mem = '256MB'")
        try:
            while True:
                try:
                    _, stmt = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    counts[await apply(conn, stmt)] += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(f"{stmt[:160]} -> {str(e)[:200]}")
                done = counts["ok"] + counts["exists"]
                if done % 250 == 0:
                    log(f"post-data indexes {done}/{len(first)}")
        finally:
            await conn.close()

    await asyncio.gather(*(index_worker() for _ in range(INDEX_PARALLEL)))
    for _, stmt in rest:
        try:
            counts[await apply(dst, stmt)] += 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"{stmt[:160]} -> {str(e)[:200]}")
    log(f"post-data: {counts['ok']} created, {counts['exists']} already there, {len(errors)} failed")
    for e in errors[:15]:
        log(f"post-data FAILED {e}")
    if errors:
        raise RuntimeError(f"post-data: {len(errors)} statement(s) failed")
    await mark_phase(dst, "post-data")


async def analyze(dst) -> None:
    if await phase_done(dst, "analyze"):
        return
    t0 = time.monotonic()
    await dst.execute("ANALYZE")
    log(f"analyze: {time.monotonic() - t0:.0f}s")
    await mark_phase(dst, "analyze")


async def grants(src, dst, ro_role: str) -> None:
    if await phase_done(dst, "grants"):
        return
    if not ro_role:
        raise RuntimeError("DATABASE_URL_READONLY carries no role name")
    role = '"' + ro_role.replace('"', '""') + '"'
    present = {r["nspname"] for r in await dst.fetch("SELECT nspname FROM pg_namespace")}
    for schema in GRANT_SCHEMAS:
        if schema not in present:
            continue
        s = '"' + schema + '"'
        await dst.execute(f"GRANT USAGE ON SCHEMA {s} TO {role}")
        await dst.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {s} TO {role}")
        await dst.execute(f"ALTER DEFAULT PRIVILEGES IN SCHEMA {s} GRANT SELECT ON TABLES TO {role}")
    hidden = await src.fetch(
        """
        SELECT n.nspname AS schema, c.relname AS name
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind IN ('r', 'v', 'm', 'p') AND n.nspname = ANY($1::text[])
          AND NOT has_table_privilege('over_readonly', c.oid, 'SELECT')
        """, list(GRANT_SCHEMAS))
    for h in hidden:
        exists = await dst.fetchval("SELECT to_regclass($1) IS NOT NULL", qualified(h["schema"], h["name"]))
        if exists:
            await dst.execute(f"REVOKE ALL ON {qualified(h['schema'], h['name'])} FROM {role}")
    still = []
    for h in hidden:
        q = qualified(h["schema"], h["name"])
        if await dst.fetchval("SELECT to_regclass($1) IS NOT NULL AND has_table_privilege($2, $1, 'SELECT')", q, ro_role):
            still.append(q)
    if still:
        raise RuntimeError(f"grants: still readable by the console role: {still}")
    log(f"grants: {role} reads {', '.join(GRANT_SCHEMAS)}; kept hidden: "
        f"{', '.join(qualified(h['schema'], h['name']) for h in hidden) or 'none'}")
    await mark_phase(dst, "grants")


async def summary(dst) -> None:
    rows = await dst.fetch("SELECT status, count(*) AS n, coalesce(sum(dst_rows), 0) AS rows "
                           "FROM _loader.tables GROUP BY status ORDER BY status")
    size = await dst.fetchval("SELECT pg_size_pretty(pg_database_size(current_database()))")
    idx = await dst.fetchval("SELECT count(*) FROM pg_indexes WHERE schemaname NOT IN ('pg_catalog','information_schema')")
    log("summary: " + "; ".join(f"{r['status']}={r['n']} tables/{r['rows']} rows" for r in rows)
        + f"; database {size}; {idx} indexes")


async def _reconnect(src, dst, src_url: str, dst_url: str):
    for conn in (src, dst):
        try:
            await conn.close()
        except Exception:  # noqa: BLE001
            pass
    return await connect(src_url), await connect(dst_url)


async def main() -> None:
    src_raw = os.environ.get("APPEND_DATABASE_URL", "")
    dst_raw = os.environ.get("XHOST_LOCAL_DATABASE_URL", "")
    ro_role = urlsplit(os.environ.get("DATABASE_URL_READONLY", "")).username or ""
    if not src_raw or not dst_raw:
        log("APPEND_DATABASE_URL and XHOST_LOCAL_DATABASE_URL are both required; not starting")
        return
    src_url = libpq_url(src_raw, direct=True)
    dst_url = libpq_url(dst_raw)
    if urlsplit(src_url).hostname == urlsplit(dst_url).hostname:
        log("source and target are the same host; refusing")
        return

    log(f"start: {urlsplit(src_url).hostname} -> {urlsplit(dst_url).hostname}, parallel={PARALLEL}")
    src = await connect(src_url)
    dst = await connect(dst_url)
    try:
        await dst.execute("CREATE SCHEMA IF NOT EXISTS _loader")
        await dst.execute("REVOKE ALL ON SCHEMA _loader FROM PUBLIC")
        await dst.execute("CREATE TABLE IF NOT EXISTS _loader.phase (name text PRIMARY KEY, done_at timestamptz)")
        await dst.execute(
            "CREATE TABLE IF NOT EXISTS _loader.tables (schema text, name text, src_bytes bigint, "
            "src_rows bigint, dst_rows bigint, status text, seconds numeric, error text, "
            "updated_at timestamptz DEFAULT now(), PRIMARY KEY (schema, name))")
        await pre_data(src_url, dst_url, dst)
        await data(src_url, dst_url, src, dst)
        # Fresh connections for every later phase. The first complete run lost its
        # Neon connection while it sat idle through 40 minutes of index building,
        # and the grants phase, which asks Neon which tables are hidden, died on
        # "connection is closed" after everything else had finished.
        src, dst = await _reconnect(src, dst, src_url, dst_url)
        await sequences(src, dst)
        await post_data(src_url, dst_url, dst)
        src, dst = await _reconnect(src, dst, src_url, dst_url)
        await analyze(dst)
        src, dst = await _reconnect(src, dst, src_url, dst_url)
        await grants(src, dst, ro_role)
        await summary(dst)
        log("COMPLETE")
    except Exception as e:  # noqa: BLE001
        log(f"STOPPED: {str(e)[:800]}")
        try:
            await summary(dst)
        except Exception:  # noqa: BLE001
            pass
    finally:
        await src.close()
        await dst.close()


if __name__ == "__main__":
    asyncio.run(main())
