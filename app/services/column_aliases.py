"""Column aliases — the Hebrew caption that belongs to a machine-named column.

Since 2026-07-30 the GovMap scraper publishes each layer's MACHINE field names
(``shem_yishuv``, ``pop_total``, ``ata_shem``) instead of the Hebrew captions
GovMap shows on its own map: a caption is not a joinable identifier, not a legal
SQL column, and not what the layer's SLD filters on. That is the right column
name — but on its own it left the /data console unreadable to anyone who does
not already know the source, and unsearchable in the language the data is in.

The caption did not disappear. Every GovMap version scraped since that change
carries a documentation bundle (``_symbology``) whose ``<layer>_fields.csv``
maps ``machine_name → hebrew_alias``. This service lifts that mapping out of the
archive OVER ALREADY HOLDS — no call to GovMap, no re-scrape, no worker change —
and stores one row per (schema, table, column) in the append DB:

    public.over_column_aliases  (table_schema, table_name, column_name, alias)

Two consumers:
  * the /data catalog labels every column with its alias, so the browser, the
    autocomplete, the schema reference and the copy-to-AI DDL all read in
    Hebrew alongside the name you must actually type;
  * the table is queryable from the console itself, so "which layers have a
    field captioned אוכלוסייה" is a SQL question like any other.

Layers whose last scrape predates the contract change have no bundle — and also
still carry Hebrew column names, so there is nothing to translate. That is why
this is a labelling service and never a rename: the column name in the database
is the only thing that answers to SQL.
"""
from __future__ import annotations

import csv
import io
import logging
import zipfile

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import append_store
from app.services.append_store import _qi
from app.services.storage_client import is_storage_value, storage_client

logger = logging.getLogger(__name__)

ALIASES_TABLE = "over_column_aliases"
STATE_TABLE = "over_column_alias_state"

# The version resource that carries a GovMap layer's documentation bundle
# (SLD + icons + the field dictionary). See app/api/worker.py, which maps the
# worker's symbology_resource_ids onto this key.
DOC_RESOURCE_KEY = "_symbology"

# Guard against pulling a huge object into a 512MB dyno: the bundles are a few
# KB to a couple of MB (icons dominate), so anything far past that is not a
# dictionary and is skipped rather than loaded.
MAX_BUNDLE_BYTES = 32 * 1024 * 1024


# ── DDL ──────────────────────────────────────────────────────────────────────
async def ensure_tables() -> None:
    """Create the alias + checkpoint tables and grant the read-only console role
    SELECT on them. Idempotent; safe to call on every boot."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(ALIASES_TABLE)} (
                table_schema text NOT NULL,
                table_name   text NOT NULL,
                column_name  text NOT NULL,
                alias        text NOT NULL,
                source       text,
                updated_at   timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (table_schema, table_name, column_name)
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS public.{_qi(STATE_TABLE)} (
                dataset_id     uuid PRIMARY KEY,
                version_number integer NOT NULL,
                fields         integer NOT NULL DEFAULT 0,
                note           text,
                updated_at     timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        if role:
            for t in (ALIASES_TABLE, STATE_TABLE):
                await conn.execute(
                    f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")


# ── the dictionary file ──────────────────────────────────────────────────────
def parse_fields_csv(text: str) -> dict[str, str]:
    """``{machine_name: hebrew_alias}`` from a ``<layer>_fields.csv``.

    Rows the dictionary documents but the layer never serves are kept: they cost
    nothing and are dropped later anyway, because a name that is not a live
    column of the table is never written. An alias equal to the machine name
    carries no information (GovMap echoes the name when it has no caption) and
    is treated as no alias at all."""
    out: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(text)):
        name = (row.get("machine_name") or "").strip()
        alias = (row.get("hebrew_alias") or "").strip()
        if not name or not alias or alias.lower() == name.lower():
            continue
        out[name] = alias
    return out


def dictionary_from_bundle(blob: bytes) -> dict[str, str]:
    """The field dictionary inside a documentation ZIP, or {} if it has none.

    The CSV is named after the layer's Hebrew caption (``אתרי_עתיקות_182_fields.csv``),
    so it is found by suffix, not by name. utf-8-sig: the sidecar ships with a
    BOM so Excel opens it correctly."""
    with zipfile.ZipFile(io.BytesIO(blob)) as z:
        names = [n for n in z.namelist() if n.lower().endswith("_fields.csv")]
        if not names:
            return {}
        merged: dict[str, str] = {}
        for n in names:
            merged.update(parse_fields_csv(z.read(n).decode("utf-8-sig")))
        return merged


def _doc_values(mappings: dict | None) -> list[str]:
    """Every storage value under the documentation key. The mapping is
    list-valued (a version can carry more than one bundle)."""
    v = (mappings or {}).get(DOC_RESOURCE_KEY)
    if not v:
        return []
    vals = v if isinstance(v, list) else [v]
    return [x for x in vals if x and is_storage_value(x)]


async def _bundle_dictionary(values: list[str]) -> dict[str, str]:
    """Merge the dictionaries of a version's documentation bundles."""
    merged: dict[str, str] = {}
    for val in values:
        # HEAD first: the whole object is read into memory, and this runs on a
        # 512MB dyno with a documented OOM history.
        size = await storage_client.object_size(val)
        if size is not None and size > MAX_BUNDLE_BYTES:
            logger.info("column_aliases: %s is %d bytes — skipped", val, size)
            continue
        blob = await storage_client.get_object_bytes(val)
        if not blob:
            logger.info("column_aliases: %s not readable from storage", val)
            continue
        try:
            merged.update(dictionary_from_bundle(blob))
        except (zipfile.BadZipFile, UnicodeDecodeError, csv.Error) as e:
            logger.info("column_aliases: unreadable bundle %s: %s", val, e)
    return merged


# ── refresh ──────────────────────────────────────────────────────────────────
async def _latest_versions(db: AsyncSession, dataset_ids: list) -> dict:
    """{dataset_id: (version_number, resource_mappings)} for the newest version
    of each dataset — one DISTINCT ON query."""
    if not dataset_ids:
        return {}
    rows = (await db.execute(
        select(VersionIndex.tracked_dataset_id, VersionIndex.version_number,
               VersionIndex.resource_mappings)
        .where(VersionIndex.tracked_dataset_id.in_(dataset_ids))
        .distinct(VersionIndex.tracked_dataset_id)
        .order_by(VersionIndex.tracked_dataset_id, VersionIndex.version_number.desc())
    )).all()
    return {r[0]: (r[1], r[2] or {}) for r in rows}


async def _checkpoints() -> dict[str, int]:
    """{dataset_id: version_number} already ingested."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            f"SELECT dataset_id, version_number FROM public.{_qi(STATE_TABLE)}")
    return {str(r["dataset_id"]): r["version_number"] for r in rows}


async def _write(conn, targets: list[tuple[str, str]], dictionary: dict[str, str],
                 source: str) -> int:
    """Write the dictionary onto every (schema, table) it applies to, matching
    machine names against the columns the table ACTUALLY has.

    The match is case-insensitive because the physical column is whatever the
    CSV header became when the mirror loaded it (clipped to 63 bytes, deduped) —
    the live column list is the authority, never the dictionary. A table is
    rewritten wholesale so a field that lost its caption upstream loses it here
    too, instead of keeping a stale one forever."""
    written = 0
    lowered = {k.lower(): v for k, v in dictionary.items()}
    for schema, table in targets:
        live = await conn.fetch(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
            """,
            schema, table,
        )
        pairs = [(r["column_name"], lowered[r["column_name"].lower()])
                 for r in live if r["column_name"].lower() in lowered]
        await conn.execute(
            f"DELETE FROM public.{_qi(ALIASES_TABLE)} "
            f"WHERE table_schema = $1 AND table_name = $2", schema, table)
        if not pairs:
            continue
        await conn.executemany(
            f"""
            INSERT INTO public.{_qi(ALIASES_TABLE)}
                (table_schema, table_name, column_name, alias, source, updated_at)
            VALUES ($1, $2, $3, $4, $5, now())
            ON CONFLICT (table_schema, table_name, column_name)
            DO UPDATE SET alias = EXCLUDED.alias, source = EXCLUDED.source,
                          updated_at = now()
            """,
            [(schema, table, col, alias, source) for col, alias in pairs],
        )
        written += len(pairs)
    return written


async def refresh(db: AsyncSession, *, limit: int | None = None,
                  force: bool = False) -> dict:
    """Ingest field dictionaries for GovMap datasets whose latest version has one.

    Version-gated like the index mirror: a dataset already ingested at its
    current version is skipped, so a tick where nothing new landed is two
    queries. ``force`` re-reads everything; ``limit`` bounds one tick's work.
    """
    if not append_store.is_configured():
        return {"ok": False, "reason": "append DB not configured"}
    await ensure_tables()

    from app.services import index_mirror
    datasets = (await db.execute(
        select(TrackedDataset)
        .where(TrackedDataset.source_type == "govmap",
               TrackedDataset.status.in_(["active", "pending"]))
    )).scalars().all()
    if not datasets:
        return {"ok": True, "datasets": 0, "updated": 0, "columns": 0, "skipped": 0}

    latest = await _latest_versions(db, [d.id for d in datasets])
    done = {} if force else await _checkpoints()
    try:
        idx_tables = {m["dataset_id"]: m["table"] for m in await index_mirror.list_tables()}
    except Exception:  # noqa: BLE001 — the mirror may not exist yet
        idx_tables = {}

    updated = columns = skipped = missing = 0
    pool = await append_store.get_pool()
    for ds in datasets:
        version_number, maps = latest.get(ds.id, (None, {}))
        if version_number is None:
            continue
        if done.get(str(ds.id)) == version_number:
            skipped += 1
            continue
        values = _doc_values(maps)
        if not values:
            # Layer last scraped before the contract change: it has no bundle,
            # and its columns are still the Hebrew captions themselves.
            missing += 1
            continue

        dictionary = await _bundle_dictionary(values)
        targets: list[tuple[str, str]] = []
        if idx_tables.get(str(ds.id)):
            targets.append((index_mirror.SCHEMA, idx_tables[str(ds.id)]))
        for t in append_store.tables_from_mappings(ds, maps):
            targets.append(("public", t["table"]))

        async with pool.acquire() as conn:
            wrote = await _write(conn, targets, dictionary, "govmap") if targets else 0
            await conn.execute(
                f"""
                INSERT INTO public.{_qi(STATE_TABLE)}
                    (dataset_id, version_number, fields, note, updated_at)
                VALUES ($1, $2, $3, $4, now())
                ON CONFLICT (dataset_id) DO UPDATE
                SET version_number = EXCLUDED.version_number,
                    fields = EXCLUDED.fields, note = EXCLUDED.note,
                    updated_at = now()
                """,
                ds.id, version_number, wrote,
                None if dictionary else "bundle carried no field dictionary",
            )
        updated += 1
        columns += wrote
        if limit and updated >= limit:
            break

    if columns:
        from app.services import data_catalog
        data_catalog.invalidate_catalog_cache()
        invalidate_cache()
    return {"ok": True, "datasets": len(datasets), "updated": updated,
            "columns": columns, "skipped": skipped, "no_bundle": missing}


# ── read side ────────────────────────────────────────────────────────────────
# The catalog build asks for the whole map on every rebuild (every 5 minutes at
# most — it is behind the catalog's own cache). One query, a few thousand rows.
_cache: dict[tuple[str, str], dict[str, str]] | None = None


def invalidate_cache() -> None:
    global _cache
    _cache = None


async def load_map(*, use_cache: bool = True) -> dict[tuple[str, str], dict[str, str]]:
    """``{(schema, table): {column: alias}}`` for every labelled table.

    Never raises: a console that cannot read the alias table must still list its
    tables — the labels are an enrichment, not the data."""
    global _cache
    if use_cache and _cache is not None:
        return _cache
    if not append_store.is_configured():
        return {}
    try:
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT table_schema, table_name, column_name, alias "
                f"FROM public.{_qi(ALIASES_TABLE)}")
    except Exception:  # noqa: BLE001 — table not created yet, or DB hiccup
        logger.debug("column_aliases: load_map failed", exc_info=True)
        return {}
    out: dict[tuple[str, str], dict[str, str]] = {}
    for r in rows:
        out.setdefault((r["table_schema"], r["table_name"]), {})[r["column_name"]] = r["alias"]
    _cache = out
    return out


def apply(columns: list[dict], aliases: dict[str, str] | None) -> list[dict]:
    """Columns with an ``alias`` attached where one is known.

    Copies rather than mutates: the column lists come from append_store's
    catalog helpers and are shared across catalog records."""
    if not aliases:
        return columns
    return [({**c, "alias": aliases[c["name"]]} if c.get("name") in aliases else c)
            for c in columns]


async def stats() -> dict:
    """Coverage summary for the admin screen."""
    if not append_store.is_configured():
        return {"tables": 0, "columns": 0, "datasets": 0}
    try:
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                f"""
                SELECT count(*) AS columns,
                       count(DISTINCT (table_schema, table_name)) AS tables
                FROM public.{_qi(ALIASES_TABLE)}
                """)
            ds = await conn.fetchrow(
                f"SELECT count(*) AS n, sum(fields) AS fields "
                f"FROM public.{_qi(STATE_TABLE)}")
    except Exception:  # noqa: BLE001
        return {"tables": 0, "columns": 0, "datasets": 0}
    return {"tables": row["tables"], "columns": row["columns"],
            "datasets": ds["n"], "fields": ds["fields"] or 0}
