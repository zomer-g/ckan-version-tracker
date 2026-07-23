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

# Scraper kinds excluded because their index CSV DUPLICATES a table we already
# mirror properly elsewhere — mirroring it again would put two copies of the
# same facts in the /data console and make them disagree as one goes stale.
#
#   "knesset" = the per-committee protocol datasets (knesset-committee-single-*,
#   1,921 of them). Their index is protocol metadata, which the `knesset` schema
#   already holds in its 48 ODATA tables (KNS_CommitteeSession & friends) —
#   synced directly from the Knesset's own API, so that copy is both richer and
#   fresher. NOTE the MMM datasets (kind "knesset_mmm") are a separate source and
#   stay eligible.
EXCLUDED_SCRAPER_KINDS = frozenset({"knesset"})

STATE_TABLE = "_sync_state"

# Process-local cache of the checkpoint read — see loaded_versions(). None =
# not read yet / invalidated. Every writer of STATE_TABLE that can change what
# loaded_versions() returns clears it (_record, purge_ineligible,
# retry_deferred).
_loaded_versions_cache: dict[str, int] | None = None

# ── PostGIS geometry column ──────────────────────────────────────────────────
# Optional, behind settings.index_mirror_postgis_enabled. GovMap index CSVs carry
# the feature geometry as WKT text in `geometry_wkt`; with the extension present
# we also materialise it as a real geometry plus a GiST index, which is what lets
# /data run ST_Intersects / ST_DWithin instead of only matching the text.
#
# The extension lives in its OWN schema so its ~1,000 functions and
# spatial_ref_sys stay out of the console's autocomplete. That isolation is
# exactly why every reference below is schema-qualified: the worker's connection
# carries no search_path at all (it addresses everything as idx."table"), so a
# bare `geometry` or `ST_GeomFromText` raises 42704 — measured, not assumed.
PG_EXT_SCHEMA = "extensions"
WKT_COLUMN = "geometry_wkt"
GEOM_COLUMN = "geom"
# Everything the scraper has written since 2026-07-08 is WGS84 lon/lat. Layers
# last scraped before that still hold ITM 6991 metres, and converting those as
# 4326 would produce geometry that is wrong but looks valid — hence the sniff.
GEOM_SRID = 4326

_WKT_FIRST_NUMBER = re.compile(r"-?\d+(?:\.\d+)?")


def classify_wkt_crs(sample: str | None) -> str:
    """Classify one WKT sample as ``"degrees"``, ``"itm"`` or ``"unknown"``.

    Reads the first coordinate — for ``POLYGON((34.78 32.08, …))`` that is the
    X, i.e. the longitude — and decides by magnitude. Israel spans roughly
    34.2–35.9°E in WGS84 and 120,000–320,000 m easting in ITM, so the two ranges
    are three orders of magnitude apart and cannot be confused.

    Deliberately conservative: anything that is neither is ``"unknown"``, and
    the caller skips the geometry column rather than guessing. A census of all
    235 mirrored geometry tables on 2026-07-23 found 232 degrees, 3 itm, 0
    unknown (docs/neon-postgis/README.md §5 stage 0).
    """
    if not sample:
        return "unknown"
    m = _WKT_FIRST_NUMBER.search(sample)
    if not m:
        return "unknown"
    try:
        x = float(m.group(0))
    except ValueError:  # pragma: no cover — the regex cannot produce this
        return "unknown"
    if 33.0 <= x <= 37.0:
        return "degrees"
    if 100_000.0 <= x <= 400_000.0:
        return "itm"
    return "unknown"

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
    # Row-tolerant WKT parser. ST_GeomFromText aborts the whole statement on the
    # first unparseable value, which cost a 12,309-feature layer its entire
    # geometry because GovMap emitted a handful of degenerate rings (three
    # points, first == last — a line wearing a polygon's name). Source data
    # quality is not something we can fix, and losing a whole layer to it is the
    # wrong trade. Bad rows become NULL and are COUNTED by the caller, so the
    # gap is reported rather than silent.
    try:
        await conn.execute(f"""
            CREATE OR REPLACE FUNCTION {_qt('try_geom')}(w text, srid int)
            RETURNS {_qi(PG_EXT_SCHEMA)}.geometry
            LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE AS $fn$
            BEGIN
                RETURN {_qi(PG_EXT_SCHEMA)}.ST_GeomFromText(w, srid);
            EXCEPTION WHEN others THEN
                RETURN NULL;
            END $fn$
        """)
    except Exception:  # noqa: BLE001 — PostGIS absent ⇒ the geom step is off anyway
        logger.debug("idx: try_geom not created (PostGIS missing?)", exc_info=True)
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
        # Geometry coverage. Without these two, a corpus where PostGIS was on
        # for some syncs and off for others looks identical to one where it is
        # on everywhere — every sync rebuilds its table, so the column comes and
        # goes silently. `postgis_rows` is how many geometries the LAST sync
        # converted (NULL = none built); `postgis_note` is why, when it did not.
        f"ALTER TABLE {_qt(STATE_TABLE)} ADD COLUMN IF NOT EXISTS postgis_rows bigint",
        f"ALTER TABLE {_qt(STATE_TABLE)} ADD COLUMN IF NOT EXISTS postgis_note text",
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
    duplicate data — and so would the kinds in EXCLUDED_SCRAPER_KINDS, which
    have a better copy in another schema.

    Whether a given VERSION actually has the CSV is checked separately
    (index_csv_value) — a dataset can be eligible but not yet have a version to
    mirror."""
    if (getattr(ds, "status", None) or "") not in ("active", "pending"):
        return False
    if (getattr(ds, "source_type", None) or "") not in ELIGIBLE_SOURCE_TYPES:
        return False
    return (getattr(ds, "kind", None) or "") not in EXCLUDED_SCRAPER_KINDS


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


def _geom_index_name(table: str) -> str:
    """GiST index name for ``table``, inside the 63-byte identifier budget."""
    suffix = "_geom_gix"
    return append_store.clip_ident_bytes(table, 63 - len(suffix)) + suffix


async def _add_geometry(conn, staging: str, columns: list[str]) -> dict:
    """Materialise ``geom`` + a GiST index on ``staging``, before the swap.

    Building it on staging rather than on the live table is not a preference:
    every sync rebuilds the table and swaps it in, so a column added to the live
    table would be destroyed by the next sync.

    (``backfill_geometry`` passes a LIVE table on purpose — see its docstring.
    The index name is derived from whatever is passed, so the sync path gets a
    ``…__stg_geom_gix`` that the swap renames, and the backfill gets the final
    name directly. Same function, no special-casing.)

    **Never raises.** Geometry is an enhancement; a layer whose WKT we cannot
    convert must still get its content refreshed. All three statements run in one
    transaction, and DDL is transactional in Postgres, so a failure takes the
    column with it and leaves staging exactly as the COPY left it — the caller
    then swaps in a geom-less table, with the reason recorded so the admin
    coverage view shows the gap instead of the corpus silently drifting.

    Returns ``{"rows": n}`` on success, ``{"skipped": why}`` when there is
    nothing to do, or ``{"error": msg}`` when the conversion itself failed.
    """
    if not settings.index_mirror_postgis_enabled:
        return {"skipped": "postgis disabled"}
    if WKT_COLUMN not in columns:
        return {"skipped": "no geometry column"}

    # One truncated row: enough to classify the CRS, and it never expands the
    # whole TOASTed geometry (which is what made a naive scan cost 46 seconds).
    sample = await conn.fetchval(
        f"SELECT substring({_qi(WKT_COLUMN)} from 1 for 120) FROM {_qt(staging)} "
        f"WHERE {_qi(WKT_COLUMN)} IS NOT NULL AND {_qi(WKT_COLUMN)} <> '' LIMIT 1")
    if sample is None:
        return {"skipped": "no geometry rows"}
    crs = classify_wkt_crs(sample)
    if crs != "degrees":
        # ITM layers are not transformed here on purpose: re-scraping the source
        # rewrites the CSV as 4326, which keeps "every geom is 4326" true
        # everywhere instead of introducing a second, silent conversion path.
        return {"skipped": f"wkt looks like {crs}, expected degrees "
                           f"(EPSG:{GEOM_SRID}) — re-scrape the dataset"}

    geom, wkt = _qi(GEOM_COLUMN), _qi(WKT_COLUMN)
    try:
        async with conn.transaction():  # all-or-nothing: no half-built column
            await conn.execute(
                f"ALTER TABLE {_qt(staging)} ADD COLUMN {geom} "
                f"{_qi(PG_EXT_SCHEMA)}.geometry(Geometry, {GEOM_SRID})")
            status = await conn.execute(
                f"UPDATE {_qt(staging)} SET {geom} = "
                f"{_qt('try_geom')}({wkt}, {GEOM_SRID}) "
                f"WHERE {wkt} IS NOT NULL AND {wkt} <> ''")
            # Rows whose WKT the parser refused. Counted INSIDE the transaction,
            # while the numbers are still true, so a layer that half-converted
            # reports the gap instead of looking complete.
            bad = await conn.fetchval(
                f"SELECT count(*) FROM {_qt(staging)} "
                f"WHERE {wkt} IS NOT NULL AND {wkt} <> '' AND {geom} IS NULL")
            await conn.execute(
                f"CREATE INDEX {_qi(_geom_index_name(staging))} ON {_qt(staging)} "
                f"USING GIST ({geom})")
    except Exception as exc:  # noqa: BLE001 — the load must survive this
        logger.warning("idx mirror: geometry step failed for %s — loading "
                       "without it", staging, exc_info=True)
        return {"error": f"{type(exc).__name__}: {exc}"[:500]}

    # asyncpg returns the command tag, e.g. "UPDATE 5840".
    try:
        attempted = int(str(status).rsplit(" ", 1)[-1])
    except (ValueError, AttributeError):
        attempted = 0
    bad = int(bad or 0)
    out: dict = {"rows": attempted - bad}
    if bad:
        out["skipped"] = (f"{bad} of {attempted} rows had WKT PostGIS could not "
                          f"parse (kept as NULL geom)")
        logger.warning("idx mirror: %s — %d/%d rows had unparseable WKT",
                       staging, bad, attempted)
    return out


async def geometry_backfill_candidates(conn, limit: int) -> list[str]:
    """Mirrored layers that carry ``geometry_wkt`` but not yet ``geom``.

    Smallest first, so a run that is interrupted has still converted the most
    layers it could in the time it had."""
    rows = await conn.fetch(f"""
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = $1
        WHERE c.relkind = 'r'
          AND EXISTS (SELECT 1 FROM information_schema.columns col
                      WHERE col.table_schema = $1 AND col.table_name = c.relname
                        AND col.column_name = $2)
          AND NOT EXISTS (SELECT 1 FROM information_schema.columns col
                          WHERE col.table_schema = $1 AND col.table_name = c.relname
                            AND col.column_name = $3)
        ORDER BY pg_total_relation_size(c.oid) ASC
        LIMIT $4
    """, SCHEMA, WKT_COLUMN, GEOM_COLUMN, int(limit))
    return [r["relname"] for r in rows]


async def backfill_geometry(limit: int = 25) -> dict:
    """Add ``geom`` to already-mirrored layers IN PLACE, without reloading them.

    The obvious way to backfill would be to clear every checkpoint and let the
    sync engine rebuild each table — but that re-downloads the whole 710 MB of
    index CSVs from object storage and re-COPYs every row, hours of work to
    produce tables whose content is already correct. The only thing missing is a
    derived column, and deriving it is a second of database-side work per layer.

    So this adds the column to the live table directly. That is normally the
    wrong move here (the next sync rebuilds the table and would drop it), but it
    is exactly right for a backfill: the flag is on, so that rebuild will
    construct ``geom`` itself. Either path converges on the same table; this one
    just gets there now instead of whenever a new version happens to land.

    Chunked and idempotent — candidates are chosen by the ABSENCE of the column,
    so a re-run picks up where the last one stopped and converting twice is
    impossible. Per-layer failures are recorded, never raised, so one bad layer
    cannot stall the rest.
    """
    if not settings.index_mirror_postgis_enabled:
        return {"skipped": "postgis disabled"}
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured")

    pool = await append_store.get_pool()
    done, failed, skipped = [], [], []
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        tables = await geometry_backfill_candidates(conn, limit)
        for table in tables:
            cols = [r["column_name"] for r in await conn.fetch(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = $1 AND table_name = $2", SCHEMA, table)]
            res = await _add_geometry(conn, table, cols)
            note = res.get("error") or res.get("skipped")
            await conn.execute(
                f"UPDATE {_qt(STATE_TABLE)} SET postgis_rows = $1, postgis_note = $2 "
                f"WHERE table_name = $3", res.get("rows"), note, table)
            if res.get("rows") is not None:
                done.append({"table": table, "rows": res["rows"]})
            elif res.get("error"):
                failed.append({"table": table, "error": res["error"]})
            else:
                skipped.append({"table": table, "reason": res.get("skipped")})
        remaining = len(await geometry_backfill_candidates(conn, 10_000))

    logger.info("idx backfill: converted=%d failed=%d skipped=%d remaining=%d",
                len(done), len(failed), len(skipped), remaining)
    return {"converted": len(done), "failed": len(failed),
            "skipped": len(skipped), "remaining": remaining,
            "results": done, "failures": failed, "skips": skipped}


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

                geom = await _add_geometry(conn, staging, columns)

                # Atomic cutover: readers see the old table or the new one.
                async with conn.transaction():
                    await conn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                    await conn.execute(
                        f"ALTER TABLE {_qt(staging)} RENAME TO {_qi(table)}")
                    if geom.get("rows") is not None:
                        # Only now is the name free: an index shares the relation
                        # namespace with tables, so the previous version's index
                        # had to be dropped (with its table, a line above) before
                        # this one can take the final name.
                        await conn.execute(
                            f"ALTER INDEX {_qt(_geom_index_name(staging))} "
                            f"RENAME TO {_qi(_geom_index_name(table))}")
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

        logger.info("idx mirror: loaded %s — %d rows, %d columns%s",
                    table, rows, len(columns),
                    f", geom {geom['rows']}" if geom.get("rows") is not None
                    else f" (geom: {geom.get('skipped') or geom.get('error')})")
        return {"table": table, "rows": rows, "columns": len(columns),
                "geom_rows": geom.get("rows"),
                "geom_error": geom.get("error"),
                "geom_skipped": geom.get("skipped")}
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
    incremented attempt counter behind, so it cannot be retried forever.

    Cached in process, because "one cheap read" was neither. This is the only
    thing the 10-minute scheduler tick does when nothing has changed — the
    common case, since GovMap polls every 90 days — and each call opened a
    connection to the append DB for ~12 statements, not one: ensure_schema
    alone is CREATE SCHEMA + CREATE OR REPLACE FUNCTION + two GRANTs + ALTER
    DEFAULT PRIVILEGES, and ``GRANT SELECT ON ALL TABLES`` re-grants across all
    ~250 mirrored tables. Several of those are catalog WRITES, so an idle tick
    was generating WAL every 10 minutes forever.

    On Neon that is also the last scheduler job holding the append compute
    awake: it scales to zero after 5 idle minutes, and a 10-minute tick caps
    the achievable sleep at ~50% even after the knesset tick was gated (see
    knesset_db._next_due_at). With the cache, a tick where nothing moved costs
    ZERO append-DB statements and the compute can stay down.

    Non-durable on purpose: a restart re-reads once, which also re-runs the
    idempotent DDL/GRANTs — so the schema still converges on every deploy,
    just not 144 times a day."""
    global _loaded_versions_cache
    if not append_store.is_configured():
        return {}
    if _loaded_versions_cache is not None:
        return dict(_loaded_versions_cache)   # copy: callers must not mutate it
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
    _loaded_versions_cache = out
    return dict(out)


async def _record(dataset_id, table: str, version_number: int,
                  rows: int | None, error: str | None, *,
                  deferred: str | None = None, csv_bytes: int | None = None,
                  bump_attempt: bool = False,
                  postgis_rows: int | None = None,
                  postgis_note: str | None = None) -> None:
    global _loaded_versions_cache
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        await conn.execute(f"""
            INSERT INTO {_qt(STATE_TABLE)}
                (dataset_id, table_name, version_number, rows, synced_at,
                 error, deferred, csv_bytes, attempts, postgis_rows, postgis_note)
            VALUES ($1, $2, $3, $4, now(), $5, $6, $7, $8, $10, $11)
            ON CONFLICT (dataset_id) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                version_number = EXCLUDED.version_number,
                rows = EXCLUDED.rows,
                synced_at = now(),
                error = EXCLUDED.error,
                deferred = EXCLUDED.deferred,
                csv_bytes = COALESCE(EXCLUDED.csv_bytes, {_qt(STATE_TABLE)}.csv_bytes),
                attempts = CASE WHEN $9 THEN {_qt(STATE_TABLE)}.attempts + 1 ELSE 0 END,
                -- Overwritten unconditionally, including back to NULL: the
                -- geometry column really is gone when a sync runs with the flag
                -- off, so a stale "yes it has geom" here would be a lie.
                postgis_rows = EXCLUDED.postgis_rows,
                postgis_note = EXCLUDED.postgis_note
        """, dataset_id, table, int(version_number), rows, error, deferred,
             csv_bytes, 1 if bump_attempt else 0, bump_attempt,
             postgis_rows, postgis_note)
    # The checkpoint just moved — the next pending() must re-read it, or a
    # just-synced dataset would be offered again for the life of the process.
    _loaded_versions_cache = None


async def pending(db, *, limit: int | None = None,
                  dataset_id=None) -> list[dict]:
    """Datasets whose latest version is newer than what ``idx`` holds.

    One query for the datasets, one for their latest versions, one for the
    checkpoint — then a pure in-memory diff. That is the "cheap SELECT when
    nothing changed" property the plan asks for: no object storage is touched
    and no table is written until something actually moved.

    REFRESHES COME FIRST. A dataset that already has a table in ``idx`` and
    whose version moved is offered ahead of one that was never mirrored, and
    only then by title. Without that, "a new version reaches SQL automatically"
    is true but unbounded in time: the driver takes 3 per tick, so a fresh
    version could sit behind hundreds of first-time loads purely because its
    title sorts late — over a day of staleness on a table users can already see.
    A first-time load is invisible until it lands; a stale visible table is
    actively misleading, so freshness wins.

    Size is deliberately NOT part of the order: it would cost a HEAD request per
    dataset and defeat the "cheap when nothing changed" property. It is handled
    where it actually matters instead — load_index_csv streams and batches by
    bytes, so a multi-GB layer costs the same peak memory as a small one."""
    from sqlalchemy import or_ as sa_or, select

    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex

    # COLUMNS, not ORM entities. Materialising ~2,900 TrackedDataset objects
    # (each with its scraper_config JSONB) cost ~40MB per tick on a dyno that
    # only has ~200MB of headroom — for five fields we actually use.
    # `kind` is pulled as a single text field, not the whole scraper_config
    # JSONB — the filter below needs it, the memory budget does not need the rest.
    kind_col = TrackedDataset.scraper_config["kind"].astext.label("kind")
    q = select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.ckan_name,
               TrackedDataset.source_type, TrackedDataset.status, kind_col).where(
        TrackedDataset.source_type.in_(ELIGIBLE_SOURCE_TYPES),
        TrackedDataset.status.in_(["active", "pending"]),
        sa_or(kind_col.is_(None),
              kind_col.notin_(tuple(EXCLUDED_SCRAPER_KINDS))),
    )
    if dataset_id is not None:
        q = q.where(TrackedDataset.id == dataset_id)
    datasets = [r for r in (await db.execute(q)).all()
                if dataset_is_index_mirror_eligible(r)]
    if not datasets:
        return []

    done = await loaded_versions()
    ids = [r.id for r in datasets]

    # PASS 1 — version numbers only, for EVERY eligible dataset. No JSONB.
    #
    # This used to ask only about datasets that were NOT already settled, to
    # keep an idle tick cheap. That optimisation had a correctness cost that
    # only shows up while a backfill is running: with even one never-mirrored
    # dataset left, every already-mirrored one was excluded from the query, so
    # a new version landing on a live table was INVISIBLE to the driver until
    # the whole backlog drained. Measured 2026-07-23: 611 of 1,013 eligible
    # datasets unmirrored, i.e. ~34 hours during which no refresh could happen.
    #
    # Asking for two plain columns is cheap enough to do unconditionally
    # (~1,000 rows of uuid+int, no JSONB, released per chunk), and the JSONB is
    # still fetched only for datasets that actually moved — which in steady
    # state is none.
    CHUNK = 500
    moved: list = []              # (dataset_id, version_number)
    for i in range(0, len(ids), CHUNK):
        rows = (await db.execute(
            select(VersionIndex.tracked_dataset_id, VersionIndex.version_number)
            .where(VersionIndex.tracked_dataset_id.in_(ids[i:i + CHUNK]))
            .distinct(VersionIndex.tracked_dataset_id)
            .order_by(VersionIndex.tracked_dataset_id,
                      VersionIndex.version_number.desc())
        )).all()
        moved += [(r[0], int(r[1])) for r in rows
                  if done.get(str(r[0]), -1) < int(r[1])]
    if not moved:
        return []                 # the normal case: one cheap query, no JSONB

    # PASS 2 — resource_mappings for the movers only, to find the index CSV.
    latest: dict = {}
    moved_ids = [d for d, _ in moved]
    for i in range(0, len(moved_ids), CHUNK):
        rows = (await db.execute(
            select(VersionIndex.tracked_dataset_id, VersionIndex.version_number,
                   VersionIndex.resource_mappings)
            .where(VersionIndex.tracked_dataset_id.in_(moved_ids[i:i + CHUNK]))
            .distinct(VersionIndex.tracked_dataset_id)
            .order_by(VersionIndex.tracked_dataset_id,
                      VersionIndex.version_number.desc())
        )).all()
        for r in rows:
            v = index_csv_value(r[2] or {})
            if v:                          # keep only the value, drop the JSONB
                latest[r[0]] = (int(r[1]), v)

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
            # Rank 0 = this dataset has been through the driver before, so a
            # table (or a recorded deferral) already exists for it and what is
            # pending is a REFRESH. Rank 1 = never loaded. A previously deferred
            # dataset ranks 0 too and will jump the queue when its version moves
            # — that costs one HEAD request and it is deferred again without a
            # download, which is cheaper than letting a live table go stale.
            "refresh": str(ds.id) in done and done[str(ds.id)] >= 0,
        })
    out.sort(key=lambda x: (0 if x["refresh"] else 1, x["title"] or ""))
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
                      res["rows"], None,
                      postgis_rows=res.get("geom_rows"),
                      postgis_note=res.get("geom_error") or res.get("geom_skipped"))
        return {**item, "rows": res["rows"], "columns": res["columns"], "ok": True,
                "geom_rows": res.get("geom_rows"),
                "geom_note": res.get("geom_error") or res.get("geom_skipped")}
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


async def purge_ineligible(db, *, apply: bool = False) -> dict:
    """Drop ``idx`` tables belonging to datasets that are no longer eligible.

    Needed because eligibility can TIGHTEN after rows are already mirrored — the
    per-committee Knesset protocol datasets were mirrored before we recognised
    their index duplicates the `knesset` ODATA schema. Without this the stale
    copies would sit in /data forever, drifting out of date and contradicting the
    authoritative tables.

    ``apply=False`` reports what it would drop."""
    global _loaded_versions_cache
    from sqlalchemy import select

    from app.models.tracked_dataset import TrackedDataset

    if not append_store.is_configured():
        return {"error": "append DB not configured"}

    kind_col = TrackedDataset.scraper_config["kind"].astext.label("kind")
    rows = (await db.execute(
        select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.source_type,
               TrackedDataset.status, kind_col)
    )).all()
    eligible = {str(r.id) for r in rows if dataset_is_index_mirror_eligible(r)}

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        state = await conn.fetch(
            f"SELECT dataset_id, table_name FROM {_qt(STATE_TABLE)}")
        victims = [(r["dataset_id"], r["table_name"]) for r in state
                   if str(r["dataset_id"]) not in eligible]
        if apply and victims:
            for dsid, table in victims:
                await conn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                await conn.execute(
                    f"DELETE FROM {_qt(STATE_TABLE)} WHERE dataset_id = $1", dsid)
    if apply and victims:
        _loaded_versions_cache = None      # checkpoint rows deleted
        from app.services.data_catalog import invalidate_catalog_cache
        invalidate_catalog_cache()
        logger.info("idx: purged %d ineligible mirrored tables", len(victims))
    return {"apply": apply, "purged": len(victims),
            "tables": [t for _, t in victims[:50]]}


async def retry_deferred() -> int:
    """Clear the deferred marks so the next run re-offers them. Use after moving
    the backfill somewhere with more memory (or raising the cap)."""
    global _loaded_versions_cache
    if not append_store.is_configured():
        return 0
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await _ensure_state_table(conn)
        res = await conn.execute(
            f"DELETE FROM {_qt(STATE_TABLE)} "
            f"WHERE deferred IS NOT NULL OR attempts >= {MAX_ATTEMPTS}")
    n = int(str(res).rsplit(" ", 1)[-1] or 0)
    # "Re-offer them" is read through loaded_versions — a stale cache would
    # make this admin action look like it did nothing until the next deploy.
    _loaded_versions_cache = None
    logger.info("idx: cleared %d deferred checkpoint rows", n)
    return n
