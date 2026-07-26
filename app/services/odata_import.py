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

import asyncio
import csv
import io
import logging
import os
import re
import tempfile
import unicodedata
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
    # Added later: the original file URL, kept alongside the resource-page
    # source_url so provenance links survive (see import flow).
    await conn.execute(
        f"ALTER TABLE {_qt(REGISTRY)} ADD COLUMN IF NOT EXISTS source_file_url text")


async def _record_import(*, resource_id: str, table: str, package_id: str | None,
                         dataset_name: str | None, title: str, organization: str | None,
                         fmt: str, source_url: str, file_url: str | None,
                         rows: int, columns: int) -> None:
    """Upsert the registry row, grant the console role SELECT, drop the catalog
    cache — shared by both the server-side and client-upload import paths."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_registry(conn)
        await conn.execute(f"""
            INSERT INTO {_qt(REGISTRY)}
                (resource_id, table_name, package_id, dataset_name, title,
                 organization, format, source_url, source_file_url, rows,
                 columns, imported_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())
            ON CONFLICT (resource_id) DO UPDATE SET
                table_name = EXCLUDED.table_name,
                package_id = EXCLUDED.package_id,
                dataset_name = EXCLUDED.dataset_name,
                title = EXCLUDED.title,
                organization = EXCLUDED.organization,
                format = EXCLUDED.format,
                source_url = EXCLUDED.source_url,
                source_file_url = EXCLUDED.source_file_url,
                rows = EXCLUDED.rows,
                columns = EXCLUDED.columns,
                imported_at = now()
        """, resource_id, table, package_id, dataset_name, title, organization,
             fmt, source_url, file_url, rows, columns)
        role = _readonly_role()
        if role:
            try:
                await conn.execute(f"GRANT SELECT ON {_qt(table)} TO {_qi(role)}")
            except Exception:  # noqa: BLE001
                logger.warning("odata: grant on %s failed", table, exc_info=True)
    from app.services.data_catalog import invalidate_catalog_cache
    invalidate_catalog_cache()


async def import_uploaded(
    *, resource_id: str, fmt: str, dataset_name: str | None, title: str | None,
    organization: str | None, source_url: str | None, file_url: str | None,
    tmp_path: str,
) -> dict:
    """Import a file the ADMIN's BROWSER already downloaded and uploaded to us.

    odata's file downloads are behind Cloudflare and 403 datacenter IPs (Render),
    while the admin's browser can fetch them (CORS is open). So the file arrives
    as an upload; we only parse + load it here. Provenance links (source_url =
    resource page, file_url = original file) are stored with the table."""
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured (APPEND_DATABASE_URL missing)")
    f = (fmt or "").upper()
    if f not in SUPPORTED_FILE_FORMATS:
        raise ValueError(
            f"פורמט לא נתמך: {fmt or '—'}. נתמכים: CSV, XLS, XLSX, ICS/ICAL.")
    columns, rows = await asyncio.to_thread(_parse_file, tmp_path, f)
    if not columns:
        raise ValueError("לא נמצאו עמודות בקובץ")
    table = table_name_for(resource_id, dataset_name)
    loaded = await _load_rows(table, columns, _batches_from_rows(rows, len(columns)))
    final_title = (title or "").strip() or dataset_name or resource_id
    src = source_url or (
        f"{ODATA_BASE}/dataset/{dataset_name}/resource/{resource_id}"
        if dataset_name else f"{ODATA_BASE}/dataset")
    await _record_import(
        resource_id=resource_id, table=table, package_id=None,
        dataset_name=dataset_name, title=final_title, organization=organization,
        fmt=f, source_url=src, file_url=file_url,
        rows=loaded["rows"], columns=loaded["columns"])
    logger.info("odata import (upload): %s -> odata.%s (%d rows, %s)",
                resource_id, table, loaded["rows"], f)
    return {"resource_id": resource_id, "table": table, "title": final_title,
            "organization": organization, "rows": loaded["rows"],
            "columns": loaded["columns"], "source_url": src, "format": f}


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


# ── file-format parsing (ported from OCAL's ckan.ts import engine) ───────────
# odata resources come as CSV/XLS/XLSX (spreadsheets) or ICS/ICAL (calendars).
# Spreadsheets are downloaded RAW (not via the datastore) because CKAN's
# datastore mangles Hebrew column names into ASCII; parsing the file keeps them.

SPREADSHEET_FORMATS = {"XLS", "XLSX"}
ICAL_FORMATS = {"ICS", "ICAL", "ICA"}
SUPPORTED_FILE_FORMATS = {"CSV"} | SPREADSHEET_FORMATS | ICAL_FORMATS

# A cell holds a real header if it is a string carrying a Hebrew or Latin letter.
_LETTER_RE = re.compile("[A-Za-zא-ת]")
# Invisible marks Israeli gov files embed in headers (RTL/LTR/BOM/ZWS/…).
_INVISIBLE_RE = re.compile(
    "[​-‏﻿­  ‪-‮⁠]")


def is_supported_file_format(fmt: str | None) -> bool:
    return (fmt or "").upper() in SUPPORTED_FILE_FORMATS


def _norm_header(s: str) -> str:
    """NFC-normalise, strip invisible directional marks, collapse whitespace."""
    s = unicodedata.normalize("NFC", s or "")
    s = _INVISIBLE_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


async def _download_file(url: str, path: str) -> None:
    """Stream a resource's raw file to ``path``."""
    async with httpx.AsyncClient(
        timeout=httpx.Timeout(600.0), follow_redirects=True
    ) as client:
        async with client.stream(
            "GET", url, headers={"User-Agent": _UA},
        ) as resp:
            resp.raise_for_status()
            with open(path, "wb") as fh:
                async for chunk in resp.aiter_bytes(64 * 1024):
                    fh.write(chunk)


def _rows_to_table(rows: list[list]) -> tuple[list[str], list[list]]:
    """A matrix of cell values → (safe column names, data rows).

    Ports OCAL's header handling: auto-detect the real header row (gov files
    often start with merged title/logo rows) by picking the first row with the
    most letter-bearing string cells; strip invisible marks; and fall back to
    synthetic ``col_N`` headers for header-less files."""
    if not rows:
        return [], []
    best_i, best_score = 0, -1
    for i, r in enumerate(rows[:20]):
        score = sum(1 for v in r if isinstance(v, str) and _LETTER_RE.search(v))
        if score > best_score:
            best_score, best_i = score, i
        if best_score >= 5:
            break
    if best_score <= 0:
        # Header-less: synthesise col_1..col_n, every row is data.
        ncols = max((len(r) for r in rows), default=0)
        columns = [f"col_{k + 1}" for k in range(ncols)]
        data = [[None if j >= len(r) or r[j] is None else str(r[j])
                 for j in range(ncols)] for r in rows]
        return columns, data
    header = rows[best_i]
    raw = [_norm_header(str(v)) if v is not None else "" for v in header]
    safe = append_store.safe_column_names(raw)
    keep = [j for j, c in enumerate(safe) if c and c not in ("_id", "_full_text")]
    columns = [safe[j] for j in keep]
    data = [[None if j >= len(r) or r[j] is None else str(r[j]) for j in keep]
            for r in rows[best_i + 1:]]
    return columns, data


def _parse_xlsx(path: str) -> tuple[list[str], list[list]]:
    import openpyxl
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        best_rows: list[list] = []
        best_cols = -1
        for ws in wb.worksheets:
            sheet_rows = [list(r) for r in ws.iter_rows(values_only=True)]
            ncols = max((len(r) for r in sheet_rows), default=0)
            if ncols > best_cols:            # widest sheet wins (skip chart junk)
                best_cols, best_rows = ncols, sheet_rows
        return _rows_to_table(best_rows)
    finally:
        wb.close()


def _parse_xls(path: str) -> tuple[list[str], list[list]]:
    import xlrd
    book = xlrd.open_workbook(path)
    best = max(book.sheets(), key=lambda s: s.ncols, default=None)
    if best is None:
        return [], []
    rows = [best.row_values(i) for i in range(best.nrows)]
    return _rows_to_table(rows)


def _parse_csv_file(path: str) -> tuple[list[str], list[list]]:
    with open(path, "rb") as fh:
        raw = fh.read()
    text = raw.decode("utf-8-sig", errors="replace")
    # Many Israeli gov CSVs are Windows-1255, not UTF-8. If the UTF-8 decode
    # produced no Hebrew but the bytes are in the win-1255 Hebrew range, redecode.
    if not re.search(r"[א-ת]", text) and any(0xC0 <= b <= 0xFA for b in raw):
        try:
            text = raw.decode("windows-1255")
        except Exception:  # noqa: BLE001 — keep the utf-8 attempt on failure
            pass
        if text[:1] == "﻿":
            text = text[1:]
    rows = [list(r) for r in csv.reader(io.StringIO(text))]
    return _rows_to_table(rows)


def _ical_dt(prop) -> str:
    if prop is None:
        return ""
    try:
        dt = getattr(prop, "dt", prop)
        return dt.isoformat() if hasattr(dt, "isoformat") else str(dt)
    except Exception:  # noqa: BLE001
        return str(prop)


def _parse_ical(path: str) -> tuple[list[str], list[list]]:
    from icalendar import Calendar
    with open(path, "rb") as fh:
        cal = Calendar.from_ical(fh.read())
    columns = ["title", "start_time", "end_time", "location", "description",
               "organizer", "participants", "uid", "status"]
    data: list[list] = []
    for comp in cal.walk("VEVENT"):
        def g(k: str) -> str:
            v = comp.get(k)
            return "" if v is None else str(v)
        organizer = g("organizer").replace("mailto:", "").replace("MAILTO:", "")
        att = comp.get("attendee")
        attendees = att if isinstance(att, list) else ([att] if att else [])
        parts: list[str] = []
        for a in attendees:
            cn = None
            try:
                cn = a.params.get("CN")
            except Exception:  # noqa: BLE001
                cn = None
            parts.append(str(cn) if cn else
                         str(a).replace("mailto:", "").replace("MAILTO:", ""))
        data.append([
            g("summary"), _ical_dt(comp.get("dtstart")), _ical_dt(comp.get("dtend")),
            g("location"), g("description"), organizer, ", ".join(parts),
            g("uid"), g("status"),
        ])
    return columns, data


def _parse_file(path: str, fmt: str) -> tuple[list[str], list[list]]:
    """Dispatch a downloaded file to the right parser → (columns, rows)."""
    f = (fmt or "").upper()
    if f in ICAL_FORMATS:
        return _parse_ical(path)
    if f == "XLSX":
        return _parse_xlsx(path)
    if f == "XLS":
        return _parse_xls(path)
    if f == "CSV":
        return _parse_csv_file(path)
    raise ValueError(f"unsupported format: {fmt}")


def _batches_from_rows(rows: list[list], ncols: int):
    """Yield COPY batches of tuples from parsed rows, bounded by rows AND bytes
    (matches _iter_batches). Rows are normalised to exactly ``ncols`` values."""
    batch: list[tuple] = []
    nbytes = 0
    for r in rows:
        rec = tuple(r[j] if j < len(r) else None for j in range(ncols))
        batch.append(rec)
        nbytes += sum(len(v) for v in rec if isinstance(v, str))
        if len(batch) >= 20_000 or nbytes >= 16 * 1024 * 1024:
            yield batch
            batch, nbytes = [], 0
    if batch:
        yield batch


# ── NEON loaders ─────────────────────────────────────────────────────────────

async def _load_rows(table: str, columns: list[str], batch_iter) -> dict:
    """Create ``odata.<table>`` from ``columns`` + an iterable of COPY batches
    (each a list of tuples), replacing any existing table atomically.

    Every column is ``text``; rows land in a staging table and a single
    transaction drops the old table and renames staging into place, so readers
    see the old table or the new one, never a partial load."""
    if not columns:
        raise ValueError("no usable columns")
    staging = append_store.clip_ident_bytes(table, 63 - len("__stg")) + "__stg"
    defs = ", ".join(f"{_qi(c)} text" for c in columns)
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await ensure_schema(conn)
        await conn.execute(f"DROP TABLE IF EXISTS {_qt(staging)}")
        await conn.execute(f"CREATE TABLE {_qt(staging)} ({defs})")
        rows = 0
        try:
            for batch in batch_iter:
                if not batch:
                    continue
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


async def _load_csv_into(path: str, table: str) -> dict:
    """Load a datastore-dump CSV file into ``odata.<table>`` (streaming)."""
    with open(path, "r", encoding="utf-8-sig", newline="") as fh:
        header = next(csv.reader(fh), None)
    if not header:
        raise ValueError("dump CSV is empty (no header row)")
    safe = append_store.safe_column_names(header)
    keep = [i for i, c in enumerate(safe) if c and c not in ("_id", "_full_text")]
    columns = [safe[i] for i in keep]
    if not columns:
        raise ValueError("dump CSV has no usable columns")
    return await _load_rows(table, columns, _iter_batches(path, columns, keep))


async def import_resource(resource_id: str) -> dict:
    """Import one odata resource into a queryable ``odata`` table.

    Handles CSV / XLS / XLSX / ICS / ICAL (ported from OCAL), plus any
    datastore-active resource via its CSV dump. Spreadsheets and calendars are
    downloaded RAW and parsed (keeps Hebrew column names). Raises ValueError for
    an unsupported, non-datastore resource."""
    if not append_store.is_configured():
        raise RuntimeError("append DB is not configured (APPEND_DATABASE_URL missing)")

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(60.0), follow_redirects=True
    ) as client:
        res = await _fetch_json(client, "resource_show", id=resource_id)
        pkg: dict = {}
        if res.get("package_id"):
            try:
                pkg = await _fetch_json(client, "package_show", id=res["package_id"])
            except Exception:  # noqa: BLE001 — metadata is nice-to-have
                logger.debug("odata: package_show failed for %s", res.get("package_id"),
                             exc_info=True)

    fmt = (res.get("format") or "").upper()
    url = (res.get("url") or "").strip()
    datastore_active = bool(res.get("datastore_active"))
    supported = fmt in SUPPORTED_FILE_FORMATS
    if not supported and not datastore_active:
        raise ValueError(
            f"פורמט לא נתמך: {res.get('format') or '—'}. "
            f"נתמכים: CSV, XLS, XLSX, ICS/ICAL, או משאב עם datastore.")

    dataset_name = pkg.get("name")
    title = (pkg.get("title") or "").strip() or (res.get("name") or "").strip() \
        or dataset_name or resource_id
    org = ((pkg.get("organization") or {}) or {}).get("title")
    table = table_name_for(resource_id, dataset_name)
    source_url = (
        f"{ODATA_BASE}/dataset/{dataset_name}/resource/{resource_id}"
        if dataset_name else f"{ODATA_BASE}/dataset"
    )

    suffix = ".ics" if fmt in ICAL_FORMATS else (".xlsx" if fmt in SPREADSHEET_FORMATS else ".csv")
    fd, tmp = tempfile.mkstemp(suffix=suffix, prefix="odata-import-")
    os.close(fd)
    try:
        if fmt == "CSV" and datastore_active:
            # Clean, CKAN-normalised CSV — stream the datastore dump.
            await _download_dump(resource_id, tmp)
            loaded = await _load_csv_into(tmp, table)
        elif supported:
            if not url:
                raise ValueError("למשאב אין קובץ להורדה")
            await _download_file(url, tmp)
            # Parsing is CPU-bound (openpyxl / ical) — keep it off the event loop.
            columns, rows = await asyncio.to_thread(_parse_file, tmp, fmt)
            if not columns:
                raise ValueError("לא נמצאו עמודות בקובץ")
            loaded = await _load_rows(table, columns,
                                      _batches_from_rows(rows, len(columns)))
        else:
            # datastore-active but blank/unknown format → CSV dump.
            await _download_dump(resource_id, tmp)
            loaded = await _load_csv_into(tmp, table)
    finally:
        try:
            os.remove(tmp)
        except OSError:
            pass

    await _record_import(
        resource_id=resource_id, table=table, package_id=res.get("package_id"),
        dataset_name=dataset_name, title=title, organization=org, fmt=fmt,
        source_url=source_url, file_url=url or None,
        rows=loaded["rows"], columns=loaded["columns"])
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
                       r.organization, r.format, r.source_url, r.source_file_url,
                       r.rows, r.columns, r.imported_at
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
        "source_url": r["source_url"], "source_file_url": r["source_file_url"],
        "rows": r["rows"], "columns": r["columns"], "imported_at": r["imported_at"],
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
