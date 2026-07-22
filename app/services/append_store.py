"""Row-level APPEND archive in a dedicated Postgres (Neon) database.

Why this exists:
    data.gov.il datastore-backed datasets tracked as append_only need a place
    to accumulate rows over time — the source is a live window (flights board)
    or a huge slowly-growing registry (vehicles), and the history must be kept
    and queried by OVER. The old design wrote the accumulation into ODATA's
    CKAN datastore; ODATA's write path is down (500/502) and is being abandoned
    for R2. R2 is object storage (no upsert/queries), so it can't host a
    queryable growing table. This module is the answer: a dedicated Postgres
    (configured via APPEND_DATABASE_URL) holding one table per dataset.

What the DB gives us that the seen-set workaround didn't:
    - Dedup is the DB's job: a UNIQUE index + ``INSERT … ON CONFLICT DO NOTHING``
      replaces the ever-growing ``_appendonly_seen`` list carried in JSONB and
      re-serialized every poll (the write-amplification that forced the windowed
      seen-set for 15-min boards). No seen-set, no windowing needed.
    - ``first_seen`` is a column with ``DEFAULT now()`` — only newly-inserted
      rows get stamped; conflicting (already-seen) rows keep their original
      first_seen. Exactly the "timestamp of each addition" semantics.
    - Keyed datasets (vehicle registry) dedup on the key column (mispar_rechev);
      keyless datasets (flights) dedup on a row_hash of the whole row, so every
      distinct row STATE is captured once.

Connection: a lazily-created asyncpg pool with min_size=0 so the Neon compute
can scale to zero between polls (keeps cost down). All identifiers are quoted,
so Hebrew column names (e.g. החלטות ממשלה fields) work. Every value is stored as
text — archival robustness over typing; callers cast on query.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import ssl
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

import asyncpg

from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None
_pool_lock = asyncio.Lock()

# Second, least-privilege pool for the PUBLIC SQL consoles only (see
# get_readonly_pool). None until first use; falls back to _pool when the
# read-only role isn't configured.
_ro_pool: asyncpg.Pool | None = None
_ro_pool_lock = asyncio.Lock()
_ro_fallback_warned = False

# Postgres bind-parameter ceiling is 32767; stay well under it.
_MAX_PARAMS = 30000

# Postgres identifier limit: NAMEDATALEN (64) - 1, counted in BYTES.
_MAX_IDENT_BYTES = 63

# Backstop statement timeout for the PUBLIC console role (see get_readonly_pool).
_READONLY_STATEMENT_TIMEOUT = "30s"


def is_configured() -> bool:
    return bool(settings.append_database_url)


def _dsn_from(raw: str) -> str:
    """Normalize a Postgres URL into a DSN asyncpg accepts.

    Neon hands out ``postgresql://…?sslmode=require&channel_binding=require``
    (and the SQLAlchemy ``+asyncpg`` suffix may be present). asyncpg takes the
    plain ``postgresql://`` scheme and gets SSL via the ``ssl`` kwarg, not query
    params — so strip the libpq-only params and the dialect suffix."""
    u = urlsplit((raw or "").strip())
    scheme = u.scheme.split("+", 1)[0] or "postgresql"
    q = [
        (k, v) for k, v in parse_qsl(u.query)
        if k.lower() not in ("sslmode", "channel_binding", "options")
    ]
    return urlunsplit((scheme, u.netloc, u.path, urlencode(q), ""))


def _dsn() -> str:
    """DSN for the read/write append pool (the sync/poll pipeline)."""
    return _dsn_from(settings.append_database_url)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        async with _pool_lock:
            if _pool is None:
                ctx = ssl.create_default_context()
                _pool = await asyncpg.create_pool(
                    dsn=_dsn(),
                    ssl=ctx,
                    min_size=0,      # let Neon scale to zero between polls
                    max_size=5,
                    command_timeout=180,
                )
                logger.info("append_store: connection pool created")
    return _pool


async def get_readonly_pool() -> asyncpg.Pool:
    """Pool for the PUBLIC SQL consoles ONLY (append run_readonly_sql, knesset
    run_sql / iter_sql_csv).

    Connects to the SAME append DB as get_pool() but authenticates as a dedicated
    least-privilege role (APPEND_READONLY_DATABASE_URL) that has been GRANTed
    SELECT only — so a write attempted through a console is refused by Postgres
    itself (permission denied), not merely by the app's READ ONLY transaction and
    keyword denylist. That DB-level denial is the point: it removes the single
    point of failure where the app-layer guard was the only thing between an
    arbitrary user SELECT and a full-privilege role.

    Falls back to the read/write pool (with a ONE-TIME warning) when the env var
    isn't set, so dev/prod keep working until the role is provisioned. The
    app-layer guards (single statement, SELECT/WITH only, READ ONLY tx,
    statement_timeout, row caps) still apply in both cases — this is
    defense-in-depth, not a replacement for them.

    The pool also carries a CONNECTION-LEVEL statement_timeout backstop. Every
    console path today sets its own ``SET LOCAL statement_timeout`` (10s for
    run_readonly_sql, 20s for knesset run_sql, 8s for sample_rows, 60s for the
    CSV exports) and those still win inside their transaction — but a path that
    forgets to set one would otherwise inherit "no limit". That matters now that
    index tables hold TOASTed geometry: a query touching a large geometry column
    was measured at 46 SECONDS of compute, and an unbounded one pins the Neon
    endpoint for as long as it runs. The backstop applies to the console role
    only — never to get_pool(), whose COPY/backfill work legitimately runs long.

    Caveat: the fallback above returns the read/write pool, which has NO such
    backstop. That path is already a warned, degraded mode."""
    global _ro_pool, _ro_fallback_warned
    raw = (settings.append_readonly_database_url or "").strip()
    if not raw:
        if not _ro_fallback_warned:
            logger.warning(
                "append_store: APPEND_READONLY_DATABASE_URL not set — the public "
                "SQL consoles fall back to the read/write append pool, so the READ "
                "ONLY transaction is the only write guard. Provision the read-only "
                "role (scripts/create_append_readonly_role.sql) and set the env var."
            )
            _ro_fallback_warned = True
        return await get_pool()
    if _ro_pool is None:
        async with _ro_pool_lock:
            if _ro_pool is None:
                ctx = ssl.create_default_context()
                _ro_pool = await asyncpg.create_pool(
                    dsn=_dsn_from(raw),
                    ssl=ctx,
                    min_size=0,      # let Neon scale to zero between queries
                    max_size=5,
                    command_timeout=180,
                    server_settings={
                        "statement_timeout": _READONLY_STATEMENT_TIMEOUT,
                    },
                )
                logger.info("append_store: read-only connection pool created")
    return _ro_pool


def table_name(ds) -> str:
    """Stable, readable, unique table name for a tracked dataset.

    ``append_<sanitized ckan_name>_<id8>`` — the id suffix guarantees no
    collision between two datasets that share a ckan_name. Clamped to Postgres'
    63-char identifier limit."""
    base = re.sub(r"[^a-z0-9_]+", "_", (ds.ckan_name or "").lower()).strip("_") or "ds"
    sid = str(ds.id).replace("-", "")[:8]
    return f"append_{base}"[:54] + f"_{sid}"


def table_name_for_resource(ds, resource_id: str) -> str:
    """Per-RESOURCE NEON table for a multi-resource dataset archived to NEON
    (e.g. one CSV per year → one queryable table per year).

    ``append_<base>_<dsid8>_<rid8>`` — dsid8 keeps two datasets that share a
    ckan_name apart, rid8 keeps the dataset's resources apart. Clamped to the
    63-char identifier limit (reserve 18 = ``_<8>_<8>`` for the two suffixes)."""
    base = re.sub(r"[^a-z0-9_]+", "_", (ds.ckan_name or "").lower()).strip("_") or "ds"
    dsid = str(ds.id).replace("-", "")[:8]
    rid = str(resource_id).replace("-", "")[:8]
    return f"append_{base}"[:63 - 18].rstrip("_") + f"_{dsid}_{rid}"


def _qi(name: str) -> str:
    """Quote a SQL identifier (supports Hebrew/Unicode column names)."""
    return '"' + str(name).replace('"', '""') + '"'


def clip_ident_bytes(name: str, limit: int = _MAX_IDENT_BYTES) -> str:
    """Truncate an identifier to ``limit`` UTF-8 BYTES on a character boundary.

    Postgres caps identifiers at NAMEDATALEN-1 = 63 **bytes**, not characters,
    and truncates anything longer SILENTLY. A Hebrew letter is 2 bytes, so a
    55-character Hebrew header is 94 bytes and the server stores it as ~38
    characters. Callers must therefore reason about the CLIPPED name, never the
    raw one (see safe_column_names)."""
    raw = str(name).encode("utf-8")
    if len(raw) <= limit:
        return str(name)
    # errors="ignore" drops a partial trailing character rather than raising.
    return raw[:limit].decode("utf-8", "ignore")


def safe_column_names(headers: list[str]) -> list[str]:
    """CSV headers → column names that are unique, non-empty and ≤63 bytes.

    Returned in the SAME order as ``headers`` so callers can zip them back to
    the CSV's positional values.

    Why this is not just ``name[:63]``: the 63-byte clip is what Postgres itself
    applies, so two *distinct* long Hebrew headers sharing a prefix collapse to
    the SAME identifier — e.g. "…שעבדו בשנת 2008 בעלי השכלה על תיכונית" and
    "…שעבדו בשנת 2008 בעלי תואר אקדמי" both clip to "…בעלי", and the CREATE TABLE
    fails with DuplicateColumnError. Dedup therefore has to run on the clipped
    names, and the "_2"/"_3" disambiguating suffix has to fit INSIDE the same
    63-byte budget (mirrors the reasoning in _index_name)."""
    out: list[str] = []
    seen: dict[str, int] = {}
    for i, raw in enumerate(headers):
        name = clip_ident_bytes((str(raw) if raw is not None else "").strip()
                                .replace("\x00", "")) or f"col_{i + 1}"
        key = name.lower()
        if key in seen:
            seen[key] += 1
            suffix = f"_{seen[key]}"
            name = clip_ident_bytes(
                name, _MAX_IDENT_BYTES - len(suffix.encode("utf-8"))) + suffix
        else:
            seen[key] = 0
        out.append(name)
    return out


def _index_name(table: str, suffix: str) -> str:
    """Collision-safe index name ≤63 chars (Postgres identifier limit).

    Naively ``f"{table}_{suffix}"[:63]`` BREAKS when ``table`` is already ~63
    chars: the truncation drops the suffix and the index name ends up EQUAL to
    the table name, so ``CREATE UNIQUE INDEX IF NOT EXISTS`` sees the name as
    already taken (by the table) and silently creates nothing — leaving the
    table with no unique index, which then makes every ``ON CONFLICT`` insert
    fail. When the plain name fits and differs from the table, use it; otherwise
    fall back to a hashed name that's guaranteed distinct and within the limit."""
    name = f"{table}_{suffix}"
    if len(name) <= 63 and name != table:
        return name
    h = hashlib.md5(table.encode("utf-8")).hexdigest()[:8]
    return f"{table[:48]}_{suffix}_{h}"[:63]


async def drop_table(table: str) -> None:
    """Drop a dataset's append table (used to reset a mis-created table before a
    clean re-seed). Idempotent."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f'DROP TABLE IF EXISTS {_qi(table)}')


def row_hash(row: dict, cols: list[str]) -> str:
    """SHA-256 over the row's column values (str-coerced, None→''), used as the
    dedup identity for keyless datasets. Matches version_detector._row_identity
    semantics so behavior is consistent."""
    canonical = json.dumps(
        {c: ("" if row.get(c) is None else str(row.get(c))) for c in cols},
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def build_insert(
    table: str,
    cols: list[str],
    chunk: list[dict],
    *,
    key_col: str | None,
    keyless: bool,
    first_seen=None,  # datetime | None — explicit backfill timestamp, else now()
) -> tuple[str, list]:
    """Build one multi-row ``INSERT … ON CONFLICT DO NOTHING`` for a chunk.

    Pure (no DB) so it's unit-testable. Dedups within the chunk by the conflict
    identity (so a single statement can't carry the same key twice). ``first_seen``
    defaults to ``now()`` inline ("first seen this poll"); pass an explicit
    timestamp (ISO string) to backfill historical dates (e.g. a retroactive seed
    from per-version snapshots — each row stamped with the version's date, oldest
    first, so ON CONFLICT keeps the earliest). Keyless rows also carry the
    computed ``row_hash``. Returns (sql, params); sql is "" when the chunk has
    nothing to insert."""
    insert_cols = list(cols) + ["first_seen"] + (["row_hash"] if keyless else [])
    conflict_target = "row_hash" if keyless else key_col
    values_sql: list[str] = []
    params: list = []
    seen_local: set[str] = set()
    p = 1
    for r in chunk:
        if keyless:
            ident = row_hash(r, cols)
        else:
            kv = r.get(key_col)
            ident = "" if kv is None else str(kv)
        if ident in seen_local:
            continue
        seen_local.add(ident)
        placeholders: list[str] = []
        for c in cols:
            v = r.get(c)
            params.append(None if v is None else str(v))
            placeholders.append(f"${p}")
            p += 1
        if first_seen is not None:
            # asyncpg binds this against the timestamptz column, so it must be a
            # datetime instance (a str raises DataError). Callers pass a
            # tz-aware datetime for retroactive backfill.
            params.append(first_seen)
            placeholders.append(f"${p}")
            p += 1
        else:
            placeholders.append("now()")
        if keyless:
            params.append(ident)
            placeholders.append(f"${p}")
            p += 1
        values_sql.append("(" + ",".join(placeholders) + ")")
    if not values_sql:
        return "", []
    cols_sql = ",".join(_qi(c) for c in insert_cols)
    sql = (
        f"INSERT INTO {_qi(table)} ({cols_sql}) VALUES {','.join(values_sql)} "
        f"ON CONFLICT ({_qi(conflict_target)}) DO NOTHING"
    )
    return sql, params


def chunk_size_for(num_cols: int, keyless: bool) -> int:
    """Rows per INSERT so total bind params stay under the Postgres ceiling."""
    per_row = num_cols + (1 if keyless else 0)  # first_seen uses now(), no param
    return max(1, _MAX_PARAMS // max(1, per_row))


async def ensure_table(table: str, source_cols: list[str], *, key_col: str | None, keyless: bool) -> None:
    """Create the dataset's table if absent; otherwise add any newly-appeared
    source columns (schema drift). Then ensure the dedup UNIQUE index exists."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1",
            table,
        )
        existing = {r["column_name"] for r in rows}
        if not existing:
            cols_sql = ", ".join(f"{_qi(c)} text" for c in source_cols)
            hashcol = ', "row_hash" text' if keyless else ""
            try:
                await conn.execute(
                    f'CREATE TABLE IF NOT EXISTS {_qi(table)} '
                    f'({cols_sql}, "first_seen" timestamptz NOT NULL DEFAULT now(){hashcol})'
                )
            except (asyncpg.DuplicateTableError, asyncpg.UniqueViolationError):
                # CREATE TABLE IF NOT EXISTS isn't atomic against the pg_type
                # catalog: two concurrent polls of the same dataset can race and
                # one loses with a pg_type_typname duplicate. Benign — the table
                # now exists; fall through to the column/index reconciliation.
                pass
            existing = source_cols  # treat as present for the drift pass below
        else:
            for c in source_cols:
                if c not in existing:
                    await conn.execute(
                        f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS {_qi(c)} text'
                    )
            if keyless and "row_hash" not in existing:
                await conn.execute(
                    f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS "row_hash" text'
                )
        target = "row_hash" if keyless else key_col
        idx = _index_name(table, "uq")
        await conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {_qi(idx)} ON {_qi(table)} ({_qi(target)})'
        )


async def append_rows(
    table: str,
    source_cols: list[str],
    rows: list[dict],
    *,
    key_col: str | None,
    keyless: bool,
    first_seen: str | None = None,
) -> int:
    """Insert ``rows`` into ``table``, skipping ones already present (by key or
    row_hash). Returns the number actually inserted (parsed from the INSERT
    command tags). Assumes ensure_table has run for this (table, cols, mode).
    ``first_seen`` (ISO timestamp) backfills historical dates; default ``now()``."""
    if not rows:
        return 0
    pool = await get_pool()
    size = chunk_size_for(len(source_cols), keyless)
    inserted = 0
    async with pool.acquire() as conn:
        for i in range(0, len(rows), size):
            sql, params = build_insert(
                table, source_cols, rows[i:i + size], key_col=key_col, keyless=keyless,
                first_seen=first_seen,
            )
            if not sql:
                continue
            status = await conn.execute(sql, *params)  # e.g. "INSERT 0 42"
            try:
                inserted += int(status.split()[-1])
            except (ValueError, IndexError):
                pass
    return inserted


# ── Content-diff mode (capture new AND changed rows, efficiently) ────────────
#
# For heavy registries (vehicles) where we want to record CHANGES to existing
# rows — not just new keys — the dedup identity is a hash of the WHOLE row, and
# inserts go through a COPY-staged set-based diff instead of millions of
# per-row ON CONFLICT probes. The hash is computed in SQL (md5 over the source
# columns) so the one-time backfill of existing rows and every ongoing poll use
# the exact same value — an unchanged row hashes identically and is skipped.

_HASH_SEP = "chr(31)"  # unit separator — won't appear in the text values


def _content_hash_expr(cols: list[str], alias: str = "") -> str:
    """SQL expression hashing the row's source columns (NULL→'') in a fixed
    order. Identical for the backfill (no alias) and the staging diff (alias)."""
    pfx = (alias + ".") if alias else ""
    parts = ",".join(f"coalesce({pfx}{_qi(c)}::text,'')" for c in cols)
    return f"md5(concat_ws({_HASH_SEP},{parts}))"


async def ensure_content_diff(table: str, source_cols: list[str], key_col: str | None) -> None:
    """One-time, idempotent, resumable migration of a keyed append table to
    content-diff mode: add a ``row_hash`` column, backfill it in committed
    batches (so a kill resumes), drop the old single-key unique index (which
    would block a changed row that reuses the same key), and add a UNIQUE index
    on ``row_hash``. After the migration every call is a few cheap no-ops."""
    pool = await get_pool()
    expr = _content_hash_expr(source_cols)
    async with pool.acquire() as conn:
        await conn.execute(f'ALTER TABLE {_qi(table)} ADD COLUMN IF NOT EXISTS "row_hash" text')
    # Batched backfill — each batch is its own committed statement so an
    # interrupted migration resumes from WHERE row_hash IS NULL.
    while True:
        async with pool.acquire() as conn:
            tag = await conn.execute(
                f'UPDATE {_qi(table)} SET "row_hash" = {expr} '
                f'WHERE ctid IN (SELECT ctid FROM {_qi(table)} '
                f'WHERE "row_hash" IS NULL LIMIT 50000)'
            )
        try:
            done = int(tag.split()[-1])
        except (ValueError, IndexError):
            done = 0
        if done == 0:
            break
    async with pool.acquire() as conn:
        await conn.execute(f'DROP INDEX IF EXISTS {_qi(_index_name(table, "uq"))}')
        await conn.execute(
            f'CREATE UNIQUE INDEX IF NOT EXISTS {_qi(_index_name(table, "hash_uq"))} '
            f'ON {_qi(table)} ("row_hash")'
        )


async def append_diff(table: str, source_cols: list[str], rows: list[dict]) -> int:
    """Insert only NEW or CHANGED rows (by full-row hash) via a COPY-staged,
    set-based diff — one COPY + one INSERT…SELECT…ON CONFLICT per batch instead
    of thousands of per-row probes. Returns the number inserted. Requires
    ensure_content_diff to have run for this table."""
    if not rows:
        return 0
    pool = await get_pool()
    expr_s = _content_hash_expr(source_cols, alias="s")
    cols_q = ",".join(_qi(c) for c in source_cols)
    sel_q = ",".join("s." + _qi(c) for c in source_cols)
    records = [
        tuple(None if r.get(c) is None else str(r.get(c)) for c in source_cols)
        for r in rows
    ]
    stg_def = ", ".join(f"{_qi(c)} text" for c in source_cols)
    async with pool.acquire() as conn:
        async with conn.transaction():
            # Staging holds ONLY the source columns (all text). Using LIKE the
            # target would inherit first_seen's NOT NULL without its DEFAULT, and
            # COPY (source cols only) would leave it NULL → violation.
            await conn.execute(f"CREATE TEMP TABLE _stg ({stg_def}) ON COMMIT DROP")
            await conn.copy_records_to_table("_stg", records=records, columns=source_cols)
            tag = await conn.execute(
                f'INSERT INTO {_qi(table)} ({cols_q}, "first_seen", "row_hash") '
                f'SELECT {sel_q}, now(), {expr_s} FROM _stg s '
                f'ON CONFLICT ("row_hash") DO NOTHING'
            )
    try:
        return int(tag.split()[-1])
    except (ValueError, IndexError):
        return 0


import difflib as _difflib
import re as _re

# Only single SELECT/WITH queries reach the DB; the READ ONLY transaction is the
# real guard (Postgres rejects any write), this denylist just fails obvious
# write/DDL attempts early with a clear message.
_SQL_STARTS_OK = _re.compile(r"^\s*(with|select)\b", _re.IGNORECASE)
_SQL_DENY = _re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|copy|merge|call|do|vacuum|reindex)\b",
    _re.IGNORECASE,
)
# Leading SQL comments — a commented query is still a SELECT, and the consoles
# lead with explanatory "-- ..." lines (the examples, the page placeholder), so
# the START check must look past them. Only the check skips comments; the
# statement is executed verbatim and the denylist still scans the whole text.
_SQL_LEADING_COMMENT = _re.compile(r"^\s*(--[^\n]*\n|/\*.*?\*/)", _re.DOTALL)


def _strip_leading_comments(s: str) -> str:
    """Drop leading line/block comments so SELECT/WITH detection sees the SQL."""
    prev = None
    while prev != s:
        prev = s
        s = _SQL_LEADING_COMMENT.sub("", s, count=1)
    return s


# ── Case-insensitive identifier help (shared by every Neon-backed SQL console) ─
# A frequent complaint on all the SQL consoles is casing: a DOUBLE-QUOTED
# identifier is case-sensitive in Postgres, so `"Desc"` / `"DecisionNum"` fail
# even when the real column exists under a different case. (Unquoted names are
# folded to lowercase by Postgres, so they only work when the real column IS
# lowercase.) These helpers rewrite a quoted identifier to the schema's actual
# casing and turn a raw "does not exist" error into an actionable hint. Callers
# pass a `canonical` map: lower(real_name) -> real_name (see _canonical_idents).


def normalize_quoted_case(sql: str, canonical: dict[str, str]) -> str:
    """Rewrite each double-quoted identifier to the real stored casing when it
    (case-insensitively) names a table/column but was written in the wrong case.
    Single-quoted string literals, ``--`` / ``/* */`` comments and unknown quoted
    names (genuine aliases) are copied verbatim. Best-effort: any surprise means
    the input is returned unchanged — normalization must never break a valid
    query. `canonical` maps lower(name) -> actual name."""
    if not canonical:
        return sql
    out: list[str] = []
    i, n = 0, len(sql)
    while i < n:
        ch = sql[i]
        if ch == "'":                                   # string literal
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            out.append(sql[i:j])
            i = j
        elif ch == '"':                                 # quoted identifier
            j, buf, closed = i + 1, [], False
            while j < n:
                if sql[j] == '"':
                    if j + 1 < n and sql[j + 1] == '"':
                        buf.append('"')
                        j += 2
                        continue
                    j += 1
                    closed = True
                    break
                buf.append(sql[j])
                j += 1
            inner = "".join(buf)
            real = canonical.get(inner.lower())
            if closed and real is not None and real != inner:
                out.append(_qi(real))
            else:
                out.append(sql[i:j])
            i = j
        elif ch == "-" and i + 1 < n and sql[i + 1] == "-":   # line comment
            j = sql.find("\n", i)
            j = n if j == -1 else j
            out.append(sql[i:j])
            i = j
        elif ch == "/" and i + 1 < n and sql[i + 1] == "*":   # block comment
            j = sql.find("*/", i + 2)
            j = n if j == -1 else j + 2
            out.append(sql[i:j])
            i = j
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def sql_error_hint(exc: Exception, canonical: dict[str, str]) -> str:
    """Append a Hebrew nudge to a Postgres 'does not exist' error — the common
    cause is a wrong-case or reserved-word column name."""
    msg = (getattr(exc, "message", None) or str(exc)).strip()
    m = _re.search(r'(column|relation)\s+"([^"]+)"\s+does not exist', msg)
    if not m:
        return msg
    raw = m.group(2).split(".")[-1]
    names = list(dict.fromkeys(canonical.values()))
    parts = ["שמות עמודות/טבלאות רגישים לאותיות גדולות/קטנות בתוך מרכאות כפולות."]
    sugg = _difflib.get_close_matches(raw.lower(), [c.lower() for c in names], n=3, cutoff=0.5)
    if sugg:
        low = {c.lower(): c for c in names}
        parts.append("האם התכוונת ל־" + ", ".join(low[s] for s in sugg) + "?")
    parts.append('מילה שמורה או שם עם אות גדולה/עברית כשם עמודה דורשת מרכאות כפולות, למשל "desc".')
    return msg + "\n" + "  ".join(parts)


async def _canonical_idents(table: str) -> dict[str, str]:
    """lower(name) -> actual name for a public-schema table and its columns.
    Used to normalize wrong-case identifiers in that dataset's SQL console."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1", table)
    canon = {r["column_name"].lower(): r["column_name"] for r in rows}
    canon[table.lower()] = table
    return canon


# ── Schema-as-text (for feeding an LLM / copy-to-AI button) ──────────────────
# A DESCRIBE-style dump — CREATE TABLE DDL, the form LLMs are most fluent in —
# so a model can generate correct SQL (right names, right case, right quoting)
# instead of guessing. Generated live from the real schema, never hand-kept.

_PG_RESERVED = {
    "select", "from", "where", "group", "order", "by", "having", "limit",
    "offset", "join", "on", "as", "and", "or", "not", "null", "is", "in",
    "like", "asc", "desc", "distinct", "union", "all", "case", "when", "then",
    "else", "end", "date", "time", "timestamp", "user", "table", "column",
    "with", "values", "default", "primary", "key", "using", "into", "over",
    "window", "returning", "and", "any", "check", "unique",
}


def _ident_ref(name: str) -> str:
    """How a column/table must be written in SQL: bare if a plain lowercase
    identifier, otherwise double-quoted (mixed-case, Hebrew, reserved word)."""
    if _re.fullmatch(r"[a-z_][a-z0-9_]*", name or "") and name.lower() not in _PG_RESERVED:
        return name
    return _qi(name)


def format_schema_ddl(tables: list[dict], notes: str = "") -> str:
    """Render tables as CREATE TABLE DDL text. Each table dict:
    ``{"table": str, "description": str|None, "columns": [{"name","type"}]}``.
    Column names are shown in the exact form SQL requires (quoted when needed)."""
    out: list[str] = []
    if notes:
        out.append(notes.rstrip() + "\n")
    for t in tables:
        desc = (t.get("description") or "").strip()
        head = f"CREATE TABLE {_ident_ref(t['table'])} ("
        out.append(f"{head}  -- {desc}" if desc else head)
        cols = t.get("columns") or []
        for i, c in enumerate(cols):
            tail = "," if i < len(cols) - 1 else ""
            out.append(f"  {_ident_ref(c['name'])} {c.get('type') or 'text'}{tail}")
        out.append(");")
        out.append("")
    return "\n".join(out).rstrip() + "\n"


async def schema_text(table: str, *, title: str | None = None) -> str:
    """DESCRIBE-style DDL text for one append table (for the copy-to-AI button
    and the MCP). Hidden bookkeeping columns (row_hash) are omitted."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = $1 "
            "ORDER BY ordinal_position", table)
    cols = [{"name": r["column_name"], "type": r["data_type"]}
            for r in rows if r["column_name"] not in _HIDDEN_COLS]
    notes = (
        "-- ארכיון מצטבר ב-OVER (over.org.il) — סכימה לכתיבת SQL\n"
        "-- קריאה בלבד: SELECT / WITH יחיד. שמות עמודות נשמרים כפי שהם במקור;\n"
        "-- עמודה עם אות גדולה / עברית / מילה שמורה חייבת מרכאות כפולות (כפי שמופיע למטה).\n"
        "-- first_seen = הזמן שבו השורה נוספה לארכיון."
    )
    return format_schema_ddl([{"table": table, "description": title, "columns": cols}], notes)


def validate_readonly_sql(sql: str) -> str:
    """Normalize + validate a user SQL string for the read-only consoles.

    Returns the cleaned single statement, or raises ValueError with a Hebrew
    message. Shared by run_readonly_sql and iter_sql_csv so both apply the exact
    same guards (single statement, SELECT/WITH only, no write/DDL keywords)."""
    s = (sql or "").strip().rstrip(";").strip()
    if not s:
        raise ValueError("השאילתה ריקה")
    if ";" in s:
        raise ValueError("רק משפט יחיד מותר (ללא ';')")
    if not _SQL_STARTS_OK.match(_strip_leading_comments(s)):
        raise ValueError("רק שאילתות SELECT / WITH מותרות")
    if _SQL_DENY.search(s):
        raise ValueError("רק קריאה (SELECT) מותרת — אסורות פעולות כתיבה/שינוי")
    return s


async def run_readonly_sql(sql: str, *, table: str | None = None,
                           search_path: str | None = None,
                           max_rows: int = 1000, timeout_ms: int = 10000) -> dict:
    """Run a user-supplied read-only SELECT against the append DB and return
    {columns, rows, truncated}. Defense in depth: single statement, must start
    with SELECT/WITH, write/DDL keywords rejected, executed inside a Postgres
    READ ONLY transaction with a statement_timeout, result hard-capped at
    max_rows. The append DB holds only public data and no app secrets. When
    ``table`` is given, wrong-case quoted identifiers are corrected to that
    table's real column casing and errors get a helpful hint. ``search_path``
    (e.g. "public, knesset") makes tables of extra schemas resolvable unqualified
    — used by the central /data console that spans both schemas."""
    s = validate_readonly_sql(sql)
    canonical: dict[str, str] = {}
    if table:
        try:
            canonical = await _canonical_idents(table)
            s = normalize_quoted_case(s, canonical)
        except Exception:  # noqa: BLE001 — normalization is best-effort
            logger.debug("append_store: identifier normalization skipped", exc_info=True)
    wrapped = f"SELECT * FROM (\n{s}\n) _q LIMIT {int(max_rows) + 1}"
    pool = await get_readonly_pool()  # least-privilege role: writes denied by the DB
    try:
        async with pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
                if search_path:
                    await conn.execute(f"SET LOCAL search_path = {_safe_search_path(search_path)}")
                stmt = await conn.prepare(wrapped)
                attrs = stmt.get_attributes()
                cols = [a.name for a in attrs]
                fields = [{"id": a.name, "type": _ckan_type(getattr(a.type, "name", None))} for a in attrs]
                recs = await stmt.fetch()
    except asyncpg.PostgresError as e:
        raise ValueError(sql_error_hint(e, canonical)) from e
    truncated = len(recs) > max_rows
    rows = [
        {k: (v if (v is None or isinstance(v, (str, int, float, bool))) else str(v))
         for k, v in dict(r).items()}
        for r in recs[:max_rows]
    ]
    return {"columns": cols, "fields": fields, "rows": rows,
            "truncated": truncated, "row_count": len(rows)}


async def table_count(table: str) -> int:
    """Total rows currently in the dataset's archive table (0 if absent)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            return int(await conn.fetchval(f'SELECT count(*) FROM {_qi(table)}'))
        except asyncpg.UndefinedTableError:
            return 0


def _safe_search_path(search_path: str) -> str:
    """Whitelist a comma-separated schema list before interpolating it into
    ``SET LOCAL search_path``. The value is always a server-side constant (never
    user input), but validate anyway so a future caller can't smuggle SQL in.
    Each name must be a plain identifier; the list is re-emitted quoted."""
    parts = [p.strip() for p in str(search_path or "").split(",") if p.strip()]
    for p in parts:
        if not re.fullmatch(r"[a-z_][a-z0-9_]*", p):
            raise ValueError(f"invalid schema name in search_path: {p!r}")
    if not parts:
        raise ValueError("empty search_path")
    return ", ".join(_qi(p) for p in parts)


# ── Central /data catalog helpers (the whole-site SQL console) ────────────────
# These enumerate every queryable NEON dataset table (public schema, ``append_*``)
# cheaply — one round-trip each, estimates over exact COUNTs — so the /data page
# can list ~hundreds of tables without scanning the giant ones (e.g. the 4.1M-row
# vehicle registry). Public data only; served through the read/write pool because
# they read information_schema/pg_catalog, which the least-privilege console role
# is intentionally not granted broad access to.

_APPEND_TABLE_LIKE = "append\\_%"  # ESCAPE '\' — literal underscore, not wildcard


async def list_public_tables() -> dict[str, int]:
    """{table: estimated_row_count} for every ``append_*`` table in ``public``.

    Uses ``pg_class.reltuples`` (planner estimate, refreshed by ANALYZE/autovacuum)
    so listing the whole catalog is one cheap query rather than N ``COUNT(*)``
    scans. A freshly-created table reports -1/0 until first analyzed; clamp to 0."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT c.relname AS table, c.reltuples::bigint AS est "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = 'public' AND c.relkind = 'r' "
            "AND c.relname LIKE $1 ESCAPE '\\'",
            _APPEND_TABLE_LIKE,
        )
    return {r["table"]: max(0, int(r["est"] or 0)) for r in rows}


async def public_table_columns() -> dict[str, list[dict]]:
    """{table: [{name,type}, …]} for every ``append_*`` table in ``public``, in
    one information_schema query. Bookkeeping columns (row_hash) are hidden.
    Powers the /data console autocomplete + schema reference."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT table_name, column_name, data_type "
            "FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name LIKE $1 ESCAPE '\\' "
            "ORDER BY table_name, ordinal_position",
            _APPEND_TABLE_LIKE,
        )
    out: dict[str, list[dict]] = {}
    for r in rows:
        if r["column_name"] in _HIDDEN_COLS:
            continue
        out.setdefault(r["table_name"], []).append(
            {"name": r["column_name"], "type": _ckan_type(r["data_type"])}
        )
    return out


async def sample_rows(table: str, *, schema: str = "public", limit: int = 20) -> dict:
    """{columns, rows} — the first ``limit`` rows of a table, for the /data detail
    cube. Read through the least-privilege console role (SELECT-only). ``schema``
    is a plain identifier chosen by our code (public|knesset), never user input."""
    if not re.fullmatch(r"[a-z_][a-z0-9_]*", schema or ""):
        raise ValueError(f"invalid schema: {schema!r}")
    pool = await get_readonly_pool()
    ref = f"{_qi(schema)}.{_qi(table)}"
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute("SET LOCAL statement_timeout = 8000")
            try:
                stmt = await conn.prepare(f"SELECT * FROM {ref} LIMIT {int(limit)}")
            except asyncpg.UndefinedTableError:
                return {"columns": [], "rows": []}
            cols = [a.name for a in stmt.get_attributes() if a.name not in _HIDDEN_COLS]
            recs = await stmt.fetch()
    rows = [
        {k: (v if (v is None or isinstance(v, (str, int, float, bool))) else str(v))
         for k, v in dict(r).items() if k not in _HIDDEN_COLS}
        for r in recs
    ]
    return {"columns": cols, "rows": rows}


async def iter_sql_csv(sql: str, *, search_path: str | None = None,
                       max_rows: int = 200_000, timeout_ms: int = 60_000):
    """Async generator streaming a user SELECT's full result as CSV (utf-8-sig)
    over the least-privilege read-only role, server-side cursor so large exports
    stay memory-bounded. Same validation guards as run_readonly_sql; row-capped at
    ``max_rows``. ``search_path`` spans extra schemas for the /data console."""
    import csv as _csv
    import io as _io

    s = validate_readonly_sql(sql)
    wrapped = f"SELECT * FROM (\n{s}\n) _q LIMIT {int(max_rows)}"
    pool = await get_readonly_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            if search_path:
                await conn.execute(f"SET LOCAL search_path = {_safe_search_path(search_path)}")
            cur = await conn.cursor(wrapped)
            first = await cur.fetch(1)
            if not first:
                yield "﻿".encode("utf-8")
                return
            cols = list(first[0].keys())

            def _row_to_csv(rec) -> bytes:
                buf = _io.StringIO()
                _csv.writer(buf).writerow(
                    ["" if rec[c] is None else str(rec[c]) for c in cols]
                )
                return buf.getvalue().encode("utf-8")

            head = _io.StringIO()
            _csv.writer(head).writerow(cols)
            yield ("﻿" + head.getvalue()).encode("utf-8")
            yield _row_to_csv(first[0])
            while True:
                batch = await cur.fetch(500)
                if not batch:
                    break
                for rec in batch:
                    yield _row_to_csv(rec)


# ── Read side: powering the OVER "view & pull the archive" UI ────────────────
#
# All of the below validate every column name against the table's REAL columns
# (so a caller can't inject an identifier), parameterize every value, and quote
# identifiers — the table/column names are operator-controlled but the filter
# VALUES come from the public UI.

# Internal columns hidden from the UI (dedup bookkeeping, not data).
_HIDDEN_COLS = {"row_hash"}

# Hard ceiling on a single rows page, regardless of what the client asks.
MAX_PAGE = 500


async def user_columns(table: str) -> list[str]:
    """Ordered list of the table's user-facing columns (source columns +
    first_seen, with internal bookkeeping hidden). Empty if the table is gone."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
            table,
        )
    return [r["column_name"] for r in rows if r["column_name"] not in _HIDDEN_COLS]


def _build_where(
    cols: list[str], q: str | None, filters: dict[str, str], start_param: int,
) -> tuple[str, list]:
    """Build a WHERE clause: optional free-text ``q`` (ILIKE across every
    column) AND per-column ILIKE ``filters``. Only known columns are honored.
    Returns (clause_without_WHERE_or_empty, params)."""
    conds: list[str] = []
    params: list = []
    p = start_param
    colset = set(cols)
    if q:
        ph = f"${p}"  # one param, referenced by every column's ILIKE
        clause = " OR ".join(f"{_qi(c)}::text ILIKE {ph}" for c in cols)
        conds.append("(" + clause + ")")
        params.append(f"%{q}%")
        p += 1
    for col, val in filters.items():
        if col not in colset or val is None or val == "":
            continue
        conds.append(f"{_qi(col)}::text ILIKE ${p}")
        params.append(f"%{val}%")
        p += 1
    return (" WHERE " + " AND ".join(conds)) if conds else "", params


async def query(
    table: str,
    *,
    limit: int = 50,
    offset: int = 0,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    filters: dict[str, str] | None = None,
) -> dict:
    """Paginated, filtered read for the UI. Returns
    {columns, rows, total, limit, offset}. Validates sort/filter columns
    against the live schema; caps the page at MAX_PAGE."""
    cols = await user_columns(table)
    if not cols:
        return {"columns": [], "rows": [], "total": 0, "limit": limit, "offset": offset}
    filters = {k: v for k, v in (filters or {}).items() if k in cols}
    limit = max(1, min(int(limit or 50), MAX_PAGE))
    offset = max(0, int(offset or 0))
    sort_col = sort if (sort in cols) else ("first_seen" if "first_seen" in cols else cols[0])
    direction = "ASC" if str(order).lower() == "asc" else "DESC"

    where, params = _build_where(cols, q, filters, start_param=1)
    select_cols = ", ".join(_qi(c) for c in cols)
    pool = await get_pool()
    async with pool.acquire() as conn:
        total = int(await conn.fetchval(f'SELECT count(*) FROM {_qi(table)}{where}', *params))
        rows = await conn.fetch(
            f'SELECT {select_cols} FROM {_qi(table)}{where} '
            f'ORDER BY {_qi(sort_col)} {direction} NULLS LAST '
            f'LIMIT ${len(params)+1} OFFSET ${len(params)+2}',
            *params, limit, offset,
        )
    return {
        "columns": cols,
        "rows": [dict(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
        "sort": sort_col,
        "order": direction.lower(),
    }


def _ckan_type(pg_type: str | None) -> str:
    """Map a Postgres type (information_schema data_type OR asyncpg type name)
    to a CKAN-datastore-ish field type name."""
    t = (pg_type or "").lower()
    if "timestamp" in t or t == "date":
        return "timestamp"
    if t in ("integer", "bigint", "smallint", "int2", "int4", "int8"):
        return "int"
    if t in ("numeric", "real", "double precision", "decimal", "float4", "float8"):
        return "numeric"
    if t in ("boolean", "bool"):
        return "bool"
    return "text"


async def column_meta(table: str) -> list[dict]:
    """[{id, type}, …] for the table's user columns — CKAN ``fields`` shape."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
            table,
        )
    return [
        {"id": r["column_name"], "type": _ckan_type(r["data_type"])}
        for r in rows if r["column_name"] not in _HIDDEN_COLS
    ]


def _parse_sort(sort: str | None, cols: set[str]) -> str:
    """CKAN-style ``sort`` ("col, col2 desc") → ORDER BY clause over known
    columns only. Empty when nothing valid is given (caller may default)."""
    out: list[str] = []
    for seg in str(sort or "").split(","):
        seg = seg.strip()
        if not seg:
            continue
        toks = seg.split()
        if toks[0] not in cols:
            continue
        direction = "DESC" if (len(toks) > 1 and toks[1].lower() == "desc") else "ASC"
        out.append(f"{_qi(toks[0])} {direction}")
    return " ORDER BY " + ", ".join(out) if out else ""


async def datastore_search(
    table: str,
    *,
    fields: list[str] | None = None,
    filters: dict | None = None,
    q: str | None = None,
    sort: str | None = None,
    limit: int = 100,
    offset: int = 0,
    distinct: bool = False,
    include_total: bool = True,
) -> dict | None:
    """CKAN ``datastore_search``-style query over an append table. ``filters``
    are exact-match per column (scalar or list → IN); ``q`` is a substring match
    across all columns; ``fields`` projects the output columns. Returns
    {fields:[{id,type}], records, total, limit, offset} or None if the table is
    gone. All column names are validated against the live schema; values are
    parameterized."""
    all_cols = await user_columns(table)
    if not all_cols:
        return None
    colset = set(all_cols)
    sel = [c for c in (fields or all_cols) if c in colset] or all_cols
    limit = max(0, min(int(limit if limit is not None else 100), MAX_PAGE))
    offset = max(0, int(offset or 0))

    conds: list[str] = []
    params: list = []
    p = 1
    if q:
        ph = f"${p}"
        conds.append("(" + " OR ".join(f"{_qi(c)}::text ILIKE {ph}" for c in all_cols) + ")")
        params.append(f"%{q}%")
        p += 1
    for col, val in (filters or {}).items():
        if col not in colset:
            continue
        if isinstance(val, list):
            phs = []
            for v in val:
                params.append(None if v is None else str(v))
                phs.append(f"${p}")
                p += 1
            conds.append(f"{_qi(col)}::text = ANY(ARRAY[{','.join(phs)}]::text[])")
        else:
            params.append(None if val is None else str(val))
            conds.append(f"{_qi(col)}::text = ${p}")
            p += 1
    where = " WHERE " + " AND ".join(conds) if conds else ""
    order = _parse_sort(sort, colset) or (
        ' ORDER BY "first_seen" DESC' if "first_seen" in colset else ""
    )
    select = ("SELECT DISTINCT " if distinct else "SELECT ") + ", ".join(_qi(c) for c in sel)

    pool = await get_pool()
    async with pool.acquire() as conn:
        total = None
        if include_total:
            total = int(await conn.fetchval(f"SELECT count(*) FROM {_qi(table)}{where}", *params))
        recs = await conn.fetch(
            f"{select} FROM {_qi(table)}{where}{order} "
            f"LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
            *params, limit, offset,
        )
    meta = [f for f in await column_meta(table) if f["id"] in sel]
    return {
        "fields": meta,
        "records": [dict(r) for r in recs],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


async def iter_csv(
    table: str,
    *,
    sort: str | None = None,
    order: str = "desc",
    q: str | None = None,
    filters: dict[str, str] | None = None,
):
    """Async generator yielding the filtered table as CSV (utf-8-sig) chunks,
    server-side cursor so even multi-million-row exports stay memory-bounded."""
    import csv as _csv
    import io as _io

    cols = await user_columns(table)
    if not cols:
        yield "﻿".encode("utf-8")
        return
    filters = {k: v for k, v in (filters or {}).items() if k in cols}
    sort_col = sort if (sort in cols) else ("first_seen" if "first_seen" in cols else cols[0])
    direction = "ASC" if str(order).lower() == "asc" else "DESC"
    where, params = _build_where(cols, q, filters, start_param=1)
    select_cols = ", ".join(_qi(c) for c in cols)
    sql = (
        f'SELECT {select_cols} FROM {_qi(table)}{where} '
        f'ORDER BY {_qi(sort_col)} {direction} NULLS LAST'
    )

    def _row_to_csv(values) -> bytes:
        buf = _io.StringIO()
        _csv.writer(buf).writerow(["" if v is None else str(v) for v in values])
        return buf.getvalue().encode("utf-8")

    # Header with BOM so Excel reads Hebrew correctly.
    head = _io.StringIO()
    _csv.writer(head).writerow(cols)
    yield ("﻿" + head.getvalue()).encode("utf-8")

    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            async for rec in conn.cursor(sql, *params):
                yield _row_to_csv([rec[c] for c in cols])
