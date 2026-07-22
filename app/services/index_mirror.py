"""Index-CSV → NEON mirror: the memory-bounded loader primitive.

Every scraper/govmap version carries one "נתוני הסורק" CSV on R2 — the index of
that version (GovMap feature tables including ``geometry_wkt``; FOI responses
including the ``attachment_filename``/``attachment_url`` file index). This module
loads ONE such CSV into ONE queryable table in the ``idx`` schema of the append
DB, so the /data SQL console can search *inside* the collections.

Implements stages 0–1 of docs/neon-index-pilot/README.md §10: the loader, the
eligibility rule, the version-landed trigger and the sync driver. The staged
rollout itself (§10.4) is driven from the admin endpoint, not from here.

Design decisions, all settled in the pilot (§10.1):

* **Latest version only.** The table is REPLACED on each sync, never appended
  to; version history stays in R2. That removes ``_row_hash`` and the ON CONFLICT
  dedup entirely — which is also why this path exists instead of reusing
  ``append_store.append_rows`` (measured 4.8–5.5x slower, plus a 1.04–1.12x
  storage premium for the unique index).
* **Every column is ``text``.** These CSVs have no reliable declared types, and
  the console lets users cast. Postgres TOAST-compresses the big geometry cells
  well enough that a table typically lands SMALLER than its source CSV (0.70x
  measured across 310 datasets).
* **Constant memory.** The object streams to a temp file, the CSV is parsed
  incrementally, and rows go out in fixed batches via ``COPY``. Nothing here ever
  holds the whole file — the largest index CSV in the corpus is 3.58 GB and the
  dyno has 512 MB.
* **Atomic swap.** Rows land in a staging table; a single transaction drops the
  old table and renames staging into place, so readers see either the previous
  version or the new one, never a partial load.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import tempfile

from urllib.parse import urlsplit

from app.config import settings
from app.services import append_store
from app.services.storage_client import is_storage_value, storage_client

logger = logging.getLogger(__name__)

SCHEMA = "idx"

# The one resource every scraper/govmap version carries: its index CSV.
CSV_RESOURCE_KEY = "נתוני הסורק"

# Source types whose versions carry an index CSV worth mirroring.
ELIGIBLE_SOURCE_TYPES = ("scraper", "govmap")

STATE_TABLE = "_sync_state"

# How many times a dataset may be attempted before it is deferred instead of
# retried. The point is not transient-error tolerance — it is that an OOM kills
# the process before any error can be recorded, so without a counter claimed
# BEFORE the load the same dataset is picked again every tick, forever. That is
# precisely the crash loop of §10.9.
MAX_ATTEMPTS = 3

# A COPY batch is bounded by BOTH row count and payload bytes, whichever trips
# first. Rows alone are not a memory bound: the pilot used 20k rows/batch on an
# unconstrained machine, but the largest layer in the corpus averages ~11 KB per
# row (3.58 GB over 329,182 rows), so 20k of those rows is ~220 MB in flight —
# and the dyno has 512 MB. Wide geometry rows therefore flush on the byte limit
# and narrow rows on the row limit.
COPY_BATCH_ROWS = 20_000
# 16 MB, not 32: measured on the production dyno, a mirror tick pushed RSS to
# 427 MB against a 512 MB limit and a <400 MB acceptance target. Python str
# overhead plus asyncpg's encoding buffer make the real cost several times the
# nominal text size, so the batch budget has to be well under the headroom.
COPY_BATCH_BYTES = 16 * 1024 * 1024

# A single CSV cell can be enormous: one real dataset ("אינטרסים של מקורות") is
# 4 rows / 34 MB because each cell is a whole polygon. Python's default cap is
# 128 KB. Matches app/api/worker.py.
csv.field_size_limit(10**8)


def table_name(ds) -> str:
    """Stable, unique, ASCII table name for a dataset's index table.

    ``<sanitized ckan_name>_<id8>`` — the id suffix keeps two datasets that share
    a ckan_name apart. Clipped to Postgres' 63-byte identifier limit."""
    base = re.sub(r"[^a-z0-9_]+", "_", (ds.ckan_name or "").lower()).strip("_") or "ds"
    sid = str(ds.id).replace("-", "")[:8]
    return f"{base}"[:54].rstrip("_") + f"_{sid}"


def _qi(name: str) -> str:
    return append_store._qi(name)


def _qt(table: str, schema: str = SCHEMA) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def _staging_name(table: str) -> str:
    """Staging table name that stays inside the 63-byte identifier budget."""
    return append_store.clip_ident_bytes(table, 63 - len("__stg")) + "__stg"


def _readonly_role() -> str | None:
    """Username of the public consoles' least-privilege role, from
    APPEND_READONLY_DATABASE_URL. The console can only read ``idx`` if that role
    is granted USAGE + SELECT on it, and the schema is created at RUNTIME (so
    scripts/create_append_readonly_role.sql can't have covered it). Returns None
    when the role isn't configured — then the consoles are on the read/write
    role anyway and no grant is needed."""
    raw = (settings.append_readonly_database_url or "").strip()
    if not raw:
        return None
    user = urlsplit(raw).username
    return user or None


async def ensure_schema(conn) -> None:
    """Create the ``idx`` schema and make it readable by the console role.

    Idempotent. The GRANTs matter as much as the CREATE: without them the /data
    console authenticates as the read-only role and would see the schema as
    non-existent. ALTER DEFAULT PRIVILEGES covers every table a later sync
    creates, so a newly mirrored dataset is queryable immediately."""
    await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(SCHEMA)}")
    role = _readonly_role()
    if not role:
        return
    r = _qi(role)
    try:
        await conn.execute(f"GRANT USAGE ON SCHEMA {_qi(SCHEMA)} TO {r}")
        await conn.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA {_qi(SCHEMA)} TO {r}")
        await conn.execute(
            f"ALTER DEFAULT PRIVILEGES IN SCHEMA {_qi(SCHEMA)} "
            f"GRANT SELECT ON TABLES TO {r}")
    except Exception:  # noqa: BLE001 — a missing role must not break the sync
        logger.warning("idx: could not grant read access to %r (console may not "
                       "see idx tables)", role, exc_info=True)


async def _ensure_state_table(conn) -> None:
    """Checkpoint table: which dataset is mirrored at which version.

    This is what makes the trigger cheap and the backfill resumable — the
    scheduler compares ``version_number`` here against version_index and does
    nothing at all when they match (the common case: GovMap polls every 90 days
    and only ~916 new versions have ever landed corpus-wide)."""
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_qt(STATE_TABLE)} (
            dataset_id      uuid PRIMARY KEY,
            table_name      text NOT NULL,
            version_number  integer NOT NULL,
            rows            bigint,
            synced_at       timestamptz NOT NULL DEFAULT now(),
            error           text
        )
    """)
    # Added after the first deploy: `deferred` marks a dataset we deliberately
    # are NOT mirroring here (too large for this dyno, or it failed repeatedly),
    # and `attempts` is what stops a crash loop. Both are treated as "settled"
    # by loaded_versions so the driver stops re-offering them.
    for ddl in (
        f"ALTER TABLE {_qt(STATE_TABLE)} ADD COLUMN IF NOT EXISTS deferred text",
        f"ALTER TABLE {_qt(STATE_TABLE)} ADD COLUMN IF NOT EXISTS csv_bytes bigint",
        f"ALTER TABLE {_qt(STATE_TABLE)} ADD COLUMN IF NOT EXISTS attempts integer NOT NULL DEFAULT 0",
    ):
        await conn.execute(ddl)


def index_csv_value(mappings: dict | None) -> str | None:
    """The ``r2:``-marked index CSV of a version, or None if it has none."""
    v = (mappings or {}).get(CSV_RESOURCE_KEY)
    return v if (v and is_storage_value(v)) else None


def dataset_is_index_mirror_eligible(ds) -> bool:
    """Whether this dataset's index CSV should be mirrored into ``idx``.

    Scraper/govmap sources only: they are the ones whose versions carry a
    "נתוני הסורק" CSV. CKAN datasets already stream their rows into the append
    tables in ``public`` via archive_neon, so mirroring them here would just
    duplicate data. Whether a given VERSION actually has the CSV is checked
    separately (index_csv_value) — a dataset can be eligible but not yet have a
    version to mirror."""
    if (getattr(ds, "status", None) or "") not in ("active", "pending"):
        return False
    return (getattr(ds, "source_type", None) or "") in ELIGIBLE_SOURCE_TYPES


def _iter_batches(path: str, columns: list[str], keep: list[int]):
    """Yield lists of positional row tuples, bounded by rows AND bytes.

    Flushes at ``COPY_BATCH_ROWS`` rows or ``COPY_BATCH_BYTES`` of accumulated
    cell text, whichever comes first — so peak memory stays flat whether the
    table is 40 narrow columns or one 10 KB geometry blob per row.

    Rows are normalised to exactly ``len(columns)`` values: short rows (a ragged
    CSV) are padded with None, long ones truncated, so COPY never rejects a row
    for arity. Each batch is dropped by the caller before the next is built."""
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        reader = csv.reader(fh)
        next(reader, None)  # header already consumed by the caller
        batch: list[tuple] = []
        nbytes = 0
        for row in reader:
            rec = tuple(row[i] if i < len(row) else None for i in keep)
            batch.append(rec)
            # len() on str is character count, close enough as a size proxy and
            # far cheaper than encoding every cell just to measure it.
            nbytes += sum(len(v) for v in rec if v is not None)
            if len(batch) >= COPY_BATCH_ROWS or nbytes >= COPY_BATCH_BYTES:
                yield batch
                batch = []
                nbytes = 0
        if batch:
            yield batch


async def load_index_csv(r2_value: str, table: str) -> dict:
    """Load one index CSV from R2 into ``idx.<table>``, replacing it atomically.

    ``r2_value`` is the ``r2:``-marked value straight out of
    ``version_index.resource_mappings["נתוני הסורק"]``.

    Returns ``{"table", "rows", "columns"}``. Raises on failure — the caller owns
    the retry/checkpoint policy (stage 1); a failure here leaves the PREVIOUS
    table untouched, because nothing is swapped until the load has finished.
    """
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured (APPEND_DATABASE_URL missing)")

    tmp: str | None = None
    try:
        fd, tmp = tempfile.mkstemp(suffix=".csv", prefix="idx-load-")
        os.close(fd)
        if not await storage_client.download_to_file(r2_value, tmp):
            raise RuntimeError(f"could not download {r2_value} from object storage")

        with open(tmp, "r", encoding="utf-8-sig", newline="") as fh:
            header = next(csv.reader(fh), None)
        if not header:
            raise ValueError("index CSV is empty (no header row)")

        # 63-BYTE clip + dedup. Postgres truncates identifiers by bytes, and a
        # Hebrew letter is 2 of them, so distinct long headers sharing a prefix
        # would otherwise collapse onto each other.
        safe = append_store.safe_column_names(header)
        keep = [i for i, c in enumerate(safe) if c and c != "_id"]
        columns = [safe[i] for i in keep]
        if not columns:
            raise ValueError("index CSV has no usable columns")

        staging = _staging_name(table)
        defs = ", ".join(f"{_qi(c)} text" for c in columns)
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            await ensure_schema(conn)
            await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
            await conn.execute(f"CREATE TABLE {_qt(staging)} ({defs})")

            rows = 0
            try:
                for batch in _iter_batches(tmp, columns, keep):
                    await conn.copy_records_to_table(
                        staging, schema_name=SCHEMA, columns=columns, records=batch,
                    )
                    rows += len(batch)

                # Atomic cutover: readers see the old table or the new one.
                async with conn.transaction():
                    await conn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                    await conn.execute(
                        f"ALTER TABLE {_qt(staging)} RENAME TO {_qi(table)}")
            except BaseException:
                # Never leave a half-filled staging table behind to confuse the
                # next run or the catalog.
                try:
                    await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
                except Exception:  # noqa: BLE001 — cleanup is best-effort
                    logger.debug("idx: staging cleanup failed for %s", staging,
                                 exc_info=True)
                raise

            # Planner stats for the fresh table (cheap, and the /data console's
            # row estimates read reltuples).
            await conn.execute(f"ANALYZE {_qt(table)}")

        logger.info("idx mirror: loaded %s — %d rows, %d columns",
                    table, rows, len(columns))
        return {"table": table, "rows": rows, "columns": len(columns)}
    finally:
        if tmp:
            try:
                os.remove(tmp)
            except OSError:
                pass


# ── sync driver ──────────────────────────────────────────────────────────────

async def loaded_versions() -> dict[str, int]:
    """{dataset_id: settled version_number} — one cheap read of the checkpoint.

    "Settled" means the driver should not offer it again: either it mirrored
    cleanly, or it is DEFERRED (too large for this environment / too many failed
    attempts). A transient error reports -1 so it gets retried — but only until
    ``MAX_ATTEMPTS``, after which it is deferred instead. That cap is what makes
    a crash loop impossible: a dataset that kills the process mid-load leaves an
    incremented attempt counter behind, so it cannot be retried forever."""
    if not append_store.is_configured():
        return {}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_schema(conn)
        await _ensure_state_table(conn)
        rows = await conn.fetch(
            f"SELECT dataset_id, version_number, error, deferred, attempts "
            f"FROM {_qt(STATE_TABLE)}")
    out: dict[str, int] = {}
    for r in rows:
        settled = (r["deferred"] is not None
                   or (not r["error"] and r["attempts"] is not None)
                   or (r["attempts"] or 0) >= MAX_ATTEMPTS)
        out[str(r["dataset_id"])] = int(r["version_number"]) if settled else -1
        if r["error"] and (r["attempts"] or 0) < MAX_ATTEMPTS:
            out[str(r["dataset_id"])] = -1
    return out


async def _record(dataset_id, table: str, version_number: int,
                  rows: int | None, error: str | None, *,
                  deferred: str | None = None, csv_bytes: int | None = None,
                  bump_attempt: bool = False) -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        await conn.execute(f"""
            INSERT INTO {_qt(STATE_TABLE)}
                (dataset_id, table_name, version_number, rows, synced_at,
                 error, deferred, csv_bytes, attempts)
            VALUES ($1, $2, $3, $4, now(), $5, $6, $7, $8)
            ON CONFLICT (dataset_id) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                version_number = EXCLUDED.version_number,
                rows = EXCLUDED.rows,
                synced_at = now(),
                error = EXCLUDED.error,
                deferred = EXCLUDED.deferred,
                csv_bytes = COALESCE(EXCLUDED.csv_bytes, {_qt(STATE_TABLE)}.csv_bytes),
                attempts = CASE WHEN $9 THEN {_qt(STATE_TABLE)}.attempts + 1 ELSE 0 END
        """, dataset_id, table, int(version_number), rows, error, deferred,
             csv_bytes, 1 if bump_attempt else 0, bump_attempt)


async def pending(db, *, limit: int | None = None,
                  dataset_id=None) -> list[dict]:
    """Datasets whose latest version is newer than what ``idx`` holds.

    One query for the datasets, one for their latest versions, one for the
    checkpoint — then a pure in-memory diff. That is the "cheap SELECT when
    nothing changed" property the plan asks for: no object storage is touched
    and no table is written until something actually moved.

    Ordered by title — deliberately NOT by CSV size, which would cost a HEAD
    request per dataset and defeat the "cheap when nothing changed" property.
    Size is handled where it actually matters instead: load_index_csv streams
    and batches by bytes, so a multi-GB layer costs the same peak memory as a
    small one."""
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex

    # COLUMNS, not ORM entities. Materialising ~2,900 TrackedDataset objects
    # (each with its scraper_config JSONB) cost ~40MB per tick on a dyno that
    # only has ~200MB of headroom — for five fields we actually use.
    q = select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.ckan_name,
               TrackedDataset.source_type, TrackedDataset.status).where(
        TrackedDataset.source_type.in_(ELIGIBLE_SOURCE_TYPES),
        TrackedDataset.status.in_(["active", "pending"]),
    )
    if dataset_id is not None:
        q = q.where(TrackedDataset.id == dataset_id)
    datasets = [r for r in (await db.execute(q)).all()
                if dataset_is_index_mirror_eligible(r)]
    if not datasets:
        return []

    done = await loaded_versions()
    # Only ask the DB for versions of datasets that are not already settled —
    # in steady state that is a handful of ids instead of ~2,900 rows of JSONB.
    candidates = [r.id for r in datasets if done.get(str(r.id), -1) < 0
                  or str(r.id) not in done]
    if not candidates:
        candidates = [r.id for r in datasets]

    latest: dict = {}
    CHUNK = 500
    for i in range(0, len(candidates), CHUNK):
        rows = (await db.execute(
            select(VersionIndex.tracked_dataset_id, VersionIndex.version_number,
                   VersionIndex.resource_mappings)
            .where(VersionIndex.tracked_dataset_id.in_(candidates[i:i + CHUNK]))
            .distinct(VersionIndex.tracked_dataset_id)
            .order_by(VersionIndex.tracked_dataset_id,
                      VersionIndex.version_number.desc())
        )).all()
        for r in rows:
            v = index_csv_value(r[2] or {})
            if v:                          # keep only the value, drop the JSONB
                latest[r[0]] = (int(r[1]), v)
        if limit and len(latest) >= limit * 4:
            break                          # enough candidates for this chunk

    out: list[dict] = []
    for ds in datasets:
        got = latest.get(ds.id)
        if not got:
            continue          # no version yet, or it carries no index CSV
        vnum, value = got
        if done.get(str(ds.id), -1) >= vnum:
            continue                      # already mirrored at this version
        out.append({
            "dataset_id": ds.id,
            "title": ds.title or ds.ckan_name,
            "table": table_name(ds),
            "version_number": vnum,
            "r2_value": value,
        })
    out.sort(key=lambda x: x["title"] or "")
    return out[:limit] if limit else out


async def sync_one(item: dict, *, max_bytes: int | None = None) -> dict:
    """Mirror one pending dataset. Never raises.

    Two guards stand in front of the load, both learned from OOM-killing the web
    dyno (§10.9):

    * **Size gate** — a HEAD before a single byte is downloaded. Anything over
      ``max_bytes`` is recorded as DEFERRED and skipped. Peak memory tracks CSV
      size, and size in this corpus is wildly skewed, so a modest cap keeps
      98% of the datasets while excluding every one that could threaten the dyno.
    * **Attempt counter** — bumped BEFORE the load, so a dataset that kills the
      process mid-load still leaves evidence behind. After ``MAX_ATTEMPTS`` it is
      deferred rather than retried, which is what turns a crash *loop* into a
      single crash at worst.
    """
    if max_bytes:
        try:
            size = await storage_client.object_size(item["r2_value"])
        except Exception:  # noqa: BLE001 — unknown size ⇒ treat as too big
            size = None
        if size is None or size > max_bytes:
            reason = (f"csv {size/2**20:.1f} MB > {max_bytes/2**20:.0f} MB cap"
                      if size else "csv size unknown")
            await _record(item["dataset_id"], item["table"],
                          item["version_number"], None, None,
                          deferred=reason, csv_bytes=size)
            logger.info("idx: deferring %s — %s", item.get("title"), reason)
            return {**item, "ok": False, "deferred": reason}

    # Claim the attempt first: if the load takes the process down with it, the
    # counter survives and the next tick will not repeat it indefinitely.
    await _record(item["dataset_id"], item["table"], item["version_number"],
                  None, "in progress", bump_attempt=True)
    try:
        res = await load_index_csv(item["r2_value"], item["table"])
        await _record(item["dataset_id"], item["table"], item["version_number"],
                      res["rows"], None)
        return {**item, "rows": res["rows"], "columns": res["columns"], "ok": True}
    except Exception as e:  # noqa: BLE001 — one bad dataset must not stop a run
        logger.warning("idx: sync failed for %s (%s): %s",
                       item.get("title"), item.get("table"), e)
        await _record(item["dataset_id"], item["table"], item["version_number"],
                      None, str(e)[:500], bump_attempt=True)
        return {**item, "ok": False, "error": str(e)[:300]}


async def sync_due(db, *, limit: int = 20, dataset_id=None,
                   max_csv_mb: int | None = None) -> dict:
    """Mirror up to ``limit`` datasets whose index CSV moved. Sequential on
    purpose: each load already streams a whole CSV through the dyno, and running
    several at once is what would put memory back at risk.

    Returns a summary; the caller decides whether to keep going (the backfill
    driver just calls this repeatedly until ``pending`` is empty)."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    todo = await pending(db, limit=limit, dataset_id=dataset_id)
    if not todo:
        return {"pending": 0, "synced": 0, "failed": 0, "results": []}

    cap_mb = settings.index_mirror_max_csv_mb if max_csv_mb is None else max_csv_mb
    max_bytes = int(cap_mb) * 1024 * 1024 if cap_mb else None
    results = [await sync_one(item, max_bytes=max_bytes) for item in todo]
    ok = [r for r in results if r.get("ok")]
    deferred = [r for r in results if r.get("deferred")]
    bad = [r for r in results if not r.get("ok") and not r.get("deferred")]
    if ok:
        # New/replaced tables ⇒ the /data catalog must not serve a stale list.
        from app.services.data_catalog import invalidate_catalog_cache
        invalidate_catalog_cache()
    logger.info("idx sync: %d ok, %d deferred, %d failed, %d rows",
                len(ok), len(deferred), len(bad),
                sum(r.get("rows") or 0 for r in ok))
    return {
        "pending": len(todo), "synced": len(ok), "deferred": len(deferred),
        "failed": len(bad), "rows": sum(r.get("rows") or 0 for r in ok),
        "results": [{k: (str(v) if k == "dataset_id" else v)
                     for k, v in r.items() if k != "r2_value"} for r in results],
    }


async def list_tables() -> list[dict]:
    """Catalog rows for the mirrored tables: {dataset_id, table, version_number,
    rows, synced_at}. Empty (not an error) before the first sync."""
    if not append_store.is_configured():
        return []
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(f"""
                SELECT s.dataset_id, s.table_name, s.version_number, s.rows,
                       s.synced_at
                FROM {_qt(STATE_TABLE)} s
                JOIN pg_class c ON c.relname = s.table_name
                JOIN pg_namespace n ON n.oid = c.relnamespace
                                   AND n.nspname = '{SCHEMA}'
                WHERE s.error IS NULL
            """)
    except Exception:  # noqa: BLE001 — schema not created yet
        logger.debug("idx: list_tables before first sync", exc_info=True)
        return []
    return [{"dataset_id": str(r["dataset_id"]), "table": r["table_name"],
             "version_number": r["version_number"], "rows": r["rows"],
             "synced_at": r["synced_at"]} for r in rows]


async def list_deferred() -> list[dict]:
    """Datasets deliberately skipped here — too large for this dyno, or failed
    repeatedly. They are not lost: a run with real memory headroom (the worker
    service or an out-of-Render backfill) picks them up via ``retry_deferred``."""
    if not append_store.is_configured():
        return []
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT dataset_id, table_name, deferred, csv_bytes, attempts "
                f"FROM {_qt(STATE_TABLE)} WHERE deferred IS NOT NULL "
                f"ORDER BY csv_bytes DESC NULLS LAST")
    except Exception:  # noqa: BLE001
        return []
    return [{"dataset_id": str(r["dataset_id"]), "table": r["table_name"],
             "reason": r["deferred"], "csv_bytes": r["csv_bytes"],
             "attempts": r["attempts"]} for r in rows]


async def retry_deferred() -> int:
    """Clear the deferred marks so the next run re-offers them. Use after moving
    the backfill somewhere with more memory (or raising the cap)."""
    if not append_store.is_configured():
        return 0
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        res = await conn.execute(
            f"DELETE FROM {_qt(STATE_TABLE)} "
            f"WHERE deferred IS NOT NULL OR attempts >= {MAX_ATTEMPTS}")
    n = int(str(res).rsplit(" ", 1)[-1] or 0)
    logger.info("idx: cleared %d deferred checkpoint rows", n)
    return n
