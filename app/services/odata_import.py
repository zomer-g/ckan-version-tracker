"""Admin-curated import of מידע לעם (odata) resources into queryable NEON tables.

odata (odata.org.il) is an EXTERNAL CKAN of PROCESSED data. An admin picks
specific datastore resources and pushes each into a real table in the ``odata``
schema of the append DB, so the /data console can query and JOIN them like any
other source — clearly badged "מידע לעם" (processed, not an original public
source).

This is the deliberate, MANUAL counterpart to ``index_mirror``: the same atomic
CSV→NEON load (streaming, all-text, staging-then-swap), but sourced from odata's
datastore CSV dump over HTTP and triggered by an admin action rather than a
version landing. There is no auto-sync — a re-import replaces the table.
"""
from __future__ import annotations

import csv
import logging
import os
import re
import tempfile
from urllib.parse import urlsplit

import httpx

from app.config import settings
from app.services import append_store
from app.services.index_mirror import _iter_batches  # schema-agnostic CSV batcher

logger = logging.getLogger(__name__)

SCHEMA = "odata"
REGISTRY = "_imports"
ODATA_BASE = "https://www.odata.org.il"
_UA = "Mozilla/5.0 (over.org.il odata import)"

# A single datastore cell can be large; match the worker / index_mirror cap.
csv.field_size_limit(10**8)


def _qi(name: str) -> str:
    return append_store._qi(name)


def _qt(table: str, schema: str = SCHEMA) -> str:
    return f"{_qi(schema)}.{_qi(table)}"


def _readonly_role() -> str | None:
    """Username of the public console's least-privilege role (see index_mirror).
    None when unset — then the console runs on the read/write role and needs no
    grant."""
    raw = (settings.append_readonly_database_url or "").strip()
    if not raw:
        return None
    return urlsplit(raw).username or None


def table_name_for(resource_id: str, dataset_name: str | None) -> str:
    """Stable, unique, ASCII table name: ``<dataset-slug>_<rid8>``.

    The odata dataset ``name`` is already an ascii slug (e.g. ``amidar-shivook``);
    the resource-id suffix keeps two resources of the same dataset apart. Clipped
    to Postgres' 63-byte identifier budget."""
    base = re.sub(r"[^a-z0-9_]+", "_", (dataset_name or "").lower()).strip("_")
    rid8 = resource_id.replace("-", "")[:8]
    prefix = (base[:44].rstrip("_") + "_") if base else "odata_"
    return f"{prefix}{rid8}"


async def ensure_schema(conn) -> None:
    """Create the ``odata`` schema and make it readable by the console role.
    Idempotent; the GRANTs are what let the read-only console see the tables."""
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
    except Exception:  # noqa: BLE001 — a missing role must not break the import
        logger.warning("odata: could not grant read access to %r", role, exc_info=True)


async def ensure_registry(conn) -> None:
    """Registry of imported resources: which odata resource → which Neon table."""
    await ensure_schema(conn)
    await conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {_qt(REGISTRY)} (
            resource_id  text PRIMARY KEY,
            table_name   text NOT NULL,
            package_id   text,
            dataset_name text,
            title        text,
            organization text,
            format       text,
            source_url   text,
            rows         bigint,
            columns      integer,
            imported_at  timestamptz NOT NULL DEFAULT now()
        )
    """)


async def _fetch_json(client: httpx.AsyncClient, action: str, **params) -> dict:
    r = await client.get(
        f"{ODATA_BASE}/api/3/action/{action}",
        params=params,
        headers={"User-Agent": _UA},
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("success"):
        raise ValueError(f"odata {action} failed: {data.get('error')}")
    return data["result"]


async def _download_dump(resource_id: str, path: str) -> None:
    """Stream the resource's whole datastore table as CSV to ``path``."""
    url = f"{ODATA_BASE}/datastore/dump/{resource_id}"
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(600.0), follow_redirects=True
    ) as client:
        async with client.stream(
            "GET", url, params={"format": "csv"},
            headers={"User-Agent": _UA},
        ) as resp:
            resp.raise_for_status()
            with open(path, "wb") as fh:
                async for chunk in resp.aiter_bytes(64 * 1024):
                    fh.write(chunk)


async def _load_csv_into(path: str, table: str) -> dict:
    """Load a local CSV into ``odata.<table>``, replacing it atomically.

    Same shape as index_mirror.load_index_csv: every column ``text``, ``_id``
    dropped, rows streamed to a staging table via COPY, then a single-transaction
    drop+rename so readers see the old table or the new one, never a partial."""
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh), None)
    if not header:
        raise ValueError("dump CSV is empty (no header row)")
    safe = append_store.safe_column_names(header)
    # Drop CKAN datastore bookkeeping columns.
    keep = [i for i, c in enumerate(safe) if c and c not in ("_id", "_full_text")]
    columns = [safe[i] for i in keep]
    if not columns:
        raise ValueError("dump CSV has no usable columns")

    staging = append_store.clip_ident_bytes(table, 63 - len("__stg")) + "__stg"
    defs = ", ".join(f"{_qi(c)} text" for c in columns)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_schema(conn)
        await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
        await conn.execute(f"CREATE TABLE {_qt(staging)} ({defs})")
        rows = 0
        try:
            for batch in _iter_batches(path, columns, keep):
                await conn.copy_records_to_table(
                    staging, schema_name=SCHEMA, columns=columns, records=batch,
                )
                rows += len(batch)
            async with conn.transaction():
                await conn.execute(f"DROP TABLE IF EXISTS {_qt(table)}")
                await conn.execute(
                    f"ALTER TABLE {_qt(staging)} RENAME TO {_qi(table)}")
        except BaseException:
            try:
                await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
            except Exception:  # noqa: BLE001 — cleanup is best-effort
                logger.debug("odata: staging cleanup failed for %s", staging,
                             exc_info=True)
            raise
        await conn.execute(f"ANALYZE {_qt(table)}")
    return {"rows": rows, "columns": len(columns)}


async def import_resource(resource_id: str) -> dict:
    """Import one odata datastore resource into a queryable ``odata`` table.

    Raises ValueError if the resource has no datastore (nothing to query)."""
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured (APPEND_DATABASE_URL missing)")

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0), follow_redirects=True
    ) as client:
        res = await _fetch_json(client, "resource_show", id=resource_id)
        if not res.get("datastore_active"):
            raise ValueError(
                "המשאב אינו ניתן לתשאול (אין לו datastore). ניתן לייבא רק "
                "קבצים שנטענו ל-datastore של מידע לעם.")
        pkg: dict = {}
        if res.get("package_id"):
            try:
                pkg = await _fetch_json(client, "package_show", id=res["package_id"])
            except Exception:  # noqa: BLE001 — metadata is nice-to-have
                logger.debug("odata: package_show failed for %s", res.get("package_id"),
                             exc_info=True)

    dataset_name = pkg.get("name")
    title = (pkg.get("title") or "").strip() or (res.get("name") or "").strip() \
        or dataset_name or resource_id
    org = ((pkg.get("organization") or {}) or {}).get("title")
    fmt = (res.get("format") or "").upper()
    table = table_name_for(resource_id, dataset_name)
    source_url = (
        f"{ODATA_BASE}/dataset/{dataset_name}/resource/{resource_id}"
        if dataset_name else f"{ODATA_BASE}/dataset"
    )

    fd, tmp = tempfile.mkstemp(suffix=".csv", prefix="odata-import-")
    os.close(fd)
    try:
        await _download_dump(resource_id, tmp)
        loaded = await _load_csv_into(tmp, table)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_registry(conn)
        await conn.execute(f"""
            INSERT INTO {_qt(REGISTRY)}
                (resource_id, table_name, package_id, dataset_name, title,
                 organization, format, source_url, rows, columns, imported_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10, now())
            ON CONFLICT (resource_id) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                package_id = EXCLUDED.package_id,
                dataset_name = EXCLUDED.dataset_name,
                title = EXCLUDED.title,
                organization = EXCLUDED.organization,
                format = EXCLUDED.format,
                source_url = EXCLUDED.source_url,
                rows = EXCLUDED.rows,
                columns = EXCLUDED.columns,
                imported_at = now()
        """, resource_id, table, res.get("package_id"), dataset_name, title,
             org, fmt, source_url, loaded["rows"], loaded["columns"])
        role = _readonly_role()
        if role:
            try:
                await conn.execute(f"GRANT SELECT ON {_qt(table)} TO {_qi(role)}")
            except Exception:  # noqa: BLE001
                logger.warning("odata: grant on %s failed", table, exc_info=True)

    from app.services.data_catalog import invalidate_catalog_cache
    invalidate_catalog_cache()
    logger.info("odata import: %s -> odata.%s (%d rows, %d cols)",
                resource_id, table, loaded["rows"], loaded["columns"])
    return {"resource_id": resource_id, "table": table, "title": title,
            "organization": org, "rows": loaded["rows"],
            "columns": loaded["columns"], "source_url": source_url}


async def list_imports() -> list[dict]:
    """Registry rows whose table physically exists, newest first."""
    if not append_store.is_configured():
        return []
    pool = await append_store.get_pool()
    try:
        async with pool.acquire() as conn:
            await ensure_registry(conn)
            rows = await conn.fetch(f"""
                SELECT r.resource_id, r.table_name, r.dataset_name, r.title,
                       r.organization, r.format, r.source_url, r.rows,
                       r.columns, r.imported_at
                FROM {_qt(REGISTRY)} r
                JOIN pg_class c ON c.relname = r.table_name
                JOIN pg_namespace n ON n.oid = c.relnamespace
                                   AND n.nspname = '{SCHEMA}'
                ORDER BY r.imported_at DESC
            """)
    except Exception:  # noqa: BLE001 — schema not created yet
        logger.debug("odata: list_imports before first import", exc_info=True)
        return []
    return [{
        "resource_id": r["resource_id"], "table": r["table_name"],
        "dataset_name": r["dataset_name"], "title": r["title"],
        "organization": r["organization"], "format": r["format"],
        "source_url": r["source_url"], "rows": r["rows"],
        "columns": r["columns"], "imported_at": r["imported_at"],
    } for r in rows]


async def delete_import(resource_id: str) -> bool:
    """Drop an imported table and forget it. False if it wasn't imported."""
    if not append_store.is_configured():
        return False
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_registry(conn)
        row = await conn.fetchrow(
            f"SELECT table_name FROM {_qt(REGISTRY)} WHERE resource_id = $1",
            resource_id)
        if not row:
            return False
        await conn.execute(f"DROP TABLE IF EXISTS {_qt(row['table_name'])}")
        await conn.execute(
            f"DELETE FROM {_qt(REGISTRY)} WHERE resource_id = $1", resource_id)
    from app.services.data_catalog import invalidate_catalog_cache
    invalidate_catalog_cache()
    logger.info("odata import: dropped %s (%s)", row["table_name"], resource_id)
    return True
