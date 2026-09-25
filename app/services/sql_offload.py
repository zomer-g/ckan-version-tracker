"""Move a dataset's rows out of the SQL archive into CSV files on R2.

For a dataset whose rows are worth keeping but not worth querying: a static
bulk upload like the 62M-row rain forecast (15.5 GB as tables, a few hundred
MB as files). The rows stay downloadable from the dataset's versions page;
they stop being tables in /data.

Order, and why each step waits for the previous one:

  1. export each table with COPY to a local CSV (every source column plus
     ``first_seen``; not the internal ``row_hash``/``geom``), counting rows;
  2. upload it to R2 and read the object's size back: the file must be there
     and whole before anything that points at the table changes;
  3. repoint every version that referenced the table at the file, change the
     version's type so the page stops offering SQL tables, and switch the
     dataset's storage plan to R2 so the next poll writes files, not rows;
  4. commit that, and only then DROP the tables.

A failure in 1-3 leaves the tables and versions exactly as they were. A
failure in 4 leaves a table nothing points at, which the storage tab reports
as an orphan and a re-run drops.

``apply=False`` is a dry run: what would move, and how big it is.
"""
from __future__ import annotations

import logging
import os
import tempfile
import uuid as uuidlib
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex

logger = logging.getLogger(__name__)

# Internal columns that are not part of the source data.
EXCLUDED_COLUMNS = {"row_hash", "_row_hash", "geom"}

# change_summary types that mean "this version's rows are SQL tables".
SQL_TYPES = {"append_db", "append_db_multi"}

_running: dict[str, dict] = {}


def status(ds_id: str) -> dict | None:
    return _running.get(ds_id)


def export_query(table: str, columns: list[str]) -> str:
    """The SELECT whose rows become the CSV (pure, for tests)."""
    cols = [c for c in columns if c not in EXCLUDED_COLUMNS]
    q = lambda n: '"' + n.replace('"', '""') + '"'  # noqa: E731
    return f"SELECT {', '.join(q(c) for c in cols)} FROM public.{q(table)}"


def repoint_mappings(mappings: dict, moved: dict[str, dict]) -> dict:
    """A version's resource_mappings with its moved tables replaced by files.

    ``moved`` maps table → {"value": "r2:<key>", "resource_id", "name"}. Pure.
    Handles both shapes of ``_append_tables`` (dict by resource id, list of
    {"resource", "table"}) and the single ``append_table``."""
    m = dict(mappings or {})
    names = dict(m.get("_names") or {})
    rids = list(m.get("_resource_ids") or [])

    def attach(info: dict) -> None:
        rid = info.get("resource_id") or f"sql-{uuidlib.uuid5(uuidlib.NAMESPACE_URL, info['value']).hex[:8]}"
        m[rid] = info["value"]
        names[rid] = info["name"]
        if rid not in rids:
            rids.append(rid)

    multi = m.get("_append_tables")
    if isinstance(multi, dict):
        left = {}
        for rid, table in multi.items():
            if table in moved:
                attach({**moved[table], "resource_id": rid})
            else:
                left[rid] = table
        if left:
            m["_append_tables"] = left
        else:
            m.pop("_append_tables", None)
    elif isinstance(multi, list):
        left = []
        for entry in multi:
            table = entry.get("table") if isinstance(entry, dict) else None
            if table in moved:
                attach(moved[table])
            else:
                left.append(entry)
        if left:
            m["_append_tables"] = left
        else:
            m.pop("_append_tables", None)
    single = m.get("append_table")
    if isinstance(single, str) and single in moved:
        attach(moved[single])
        m.pop("append_table", None)

    m["_names"] = names
    m["_resource_ids"] = rids
    return m


async def _versions(db, ds_id: uuidlib.UUID) -> list[VersionIndex]:
    res = await db.execute(
        select(VersionIndex).where(VersionIndex.tracked_dataset_id == ds_id)
        .order_by(VersionIndex.version_number))
    return list(res.scalars().all())


async def plan(db, ds_id: uuidlib.UUID) -> dict:
    """Which tables would move, with rows and bytes. Read-only."""
    from app.services import append_store

    ds = await db.get(TrackedDataset, ds_id)
    if ds is None:
        raise LookupError("dataset not found")
    versions = await _versions(db, ds_id)
    tables: dict[str, dict] = {}
    for v in versions:
        for t in append_store.tables_from_mappings(ds, v.resource_mappings):
            e = tables.setdefault(t["table"], {**t, "versions": []})
            e["versions"].append(v.version_number)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        for name, e in tables.items():
            reg = await conn.fetchval("SELECT to_regclass($1)", f'public."{name}"')
            e["exists"] = reg is not None
            e["bytes"] = int(await conn.fetchval(
                "SELECT pg_total_relation_size(to_regclass($1))", f'public."{name}"') or 0) if reg else 0
            e["est_rows"] = int(await conn.fetchval(
                "SELECT reltuples::bigint FROM pg_class WHERE oid = to_regclass($1)",
                f'public."{name}"') or 0) if reg else 0
    present = [e for e in tables.values() if e["exists"]]
    return {
        "dataset_id": str(ds.id),
        "title": ds.title,
        "tables": present,
        "missing_tables": [n for n, e in tables.items() if not e["exists"]],
        "bytes": sum(e["bytes"] for e in present),
        "est_rows": sum(e["est_rows"] for e in present),
    }


async def offload(db, ds_id: uuidlib.UUID) -> dict:
    """Export → upload → verify → repoint → drop. See the module docstring."""
    from app.api.datasets import apply_storage_target
    from app.services import append_store
    from app.services import storage_client as storage
    from app.services.storage_client import storage_client

    if not storage_client.is_configured():
        raise RuntimeError("R2 is not configured; nothing can be offloaded")
    key = str(ds_id)
    st = _running[key] = {"state": "running", "done": 0, "total": None,
                          "current": None, "error": None,
                          "started_at": datetime.now(timezone.utc).isoformat()}
    try:
        p = await plan(db, ds_id)
        ds = await db.get(TrackedDataset, ds_id)
        st["total"] = len(p["tables"])
        pool = await append_store.get_pool()
        moved: dict[str, dict] = {}
        latest_version = max((v for e in p["tables"] for v in e["versions"]), default=1)

        for e in p["tables"]:
            table = e["table"]
            st["current"] = table
            async with pool.acquire() as conn:
                cols = [r["column_name"] for r in await conn.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=$1 ORDER BY ordinal_position",
                    table)]
                rows_in_table = await conn.fetchval(f'SELECT count(*) FROM public."{table}"',
                                                    timeout=1800)
                fd, path = tempfile.mkstemp(suffix=".csv")
                os.close(fd)
                try:
                    tag = await conn.copy_from_query(
                        export_query(table, cols), output=path,
                        format="csv", header=True, timeout=3600)
                    copied = int(str(tag).split()[-1])
                    if copied != rows_in_table:
                        raise RuntimeError(f"{table}: exported {copied} of {rows_in_table} rows")
                    local_size = os.path.getsize(path)
                    name = e.get("resource_name") or table
                    obj_key = storage.build_key(str(ds_id), latest_version, f"{name}.csv")
                    await storage_client.upload_object(
                        obj_key, file_path=path, content_type="text/csv; charset=utf-8")
                    remote = await storage_client.object_size(obj_key)
                    if remote != local_size:
                        raise RuntimeError(
                            f"{table}: R2 holds {remote} bytes, the export was {local_size}")
                finally:
                    try:
                        os.unlink(path)
                    except OSError:
                        pass
            moved[table] = {"value": storage.mark(obj_key), "resource_id": e.get("resource_id"),
                            "name": f"{name} (כל {copied:,} השורות, עם first_seen)"}
            st["done"] += 1

        # Every file is in place and verified: repoint, re-plan, commit.
        now = datetime.now(timezone.utc).isoformat()
        for v in await _versions(db, ds_id):
            touched = {t["table"] for t in append_store.tables_from_mappings(ds, v.resource_mappings)}
            if not touched & set(moved):
                continue
            v.resource_mappings = repoint_mappings(v.resource_mappings, moved)
            cs = dict(v.change_summary or {})
            if cs.get("type") in SQL_TYPES:
                cs["sql_offload"] = {"from_type": cs["type"], "at": now,
                                     "tables": sorted(touched & set(moved))}
                cs["type"] = "sql_offloaded"
            v.change_summary = cs
            flag_modified(v, "resource_mappings")
            flag_modified(v, "change_summary")
        ds.scraper_config = apply_storage_target(ds.scraper_config, "r2")
        flag_modified(ds, "scraper_config")
        await db.commit()

        # Nothing points at the tables any more.
        for table in moved:
            st["current"] = f"DROP {table}"
            await append_store.drop_table(table)
        st.update(state="done", current=None, bytes_freed=p["bytes"],
                  finished_at=datetime.now(timezone.utc).isoformat())
        logger.info("sql offload: %s — %d tables, %d bytes → R2", ds_id, len(moved), p["bytes"])
        return st
    except Exception as ex:  # noqa: BLE001 — report, keep the tables
        await db.rollback()
        st.update(state="failed", error=f"{type(ex).__name__}: {ex}"[:500])
        logger.exception("sql offload failed for %s", ds_id)
        return st
