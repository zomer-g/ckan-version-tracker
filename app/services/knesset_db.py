"""Knesset ODATA-v4 → NEON mirror: the "מסד הנתונים של הכנסת" feature.

Syncs ALL entity sets of the Knesset parliament ODATA-v4 feed
(https://knesset.gov.il/OdataV4/ParliamentInfo — bills, laws, committees,
plenum votes, MKs, queries, lobbyists…) into a dedicated ``knesset`` schema in
the NEON append DB (settings.append_database_url), with REAL column types
derived from $metadata (int/bigint/bool/timestamptz/text — unlike the all-text
append_store convention), so the public SQL console can JOIN and filter
natively. Exposed via app/api/knesset_db.py and the /knesset frontend page.

Design:
  * Metadata-driven: the entity-set list, columns and types are parsed from the
    live $metadata at startup — a new column upstream becomes an ALTER TABLE
    ADD COLUMN, a new set becomes a new table. Nothing is hardcoded except
    Hebrew descriptions (knesset_tables_meta.py) and the feed quirks below.
  * Resumable sync: every set is walked with ``$filter=Id gt <checkpoint>
    &$orderby=Id`` pages (the server caps every page at 100 rows regardless of
    $top, and $skip/@odata.nextLink are unreliable — see the committee-protocols
    engine). The checkpoint is persisted in ``knesset.sync_state`` after every
    insert batch, so a dyno restart loses at most one batch of progress.
  * Incremental after the initial full walk: ``LastUpdatedDate gt <watermark>``
    (48h overlap for clock skew), same Id-ordered paging. Sets WITHOUT a
    LastUpdatedDate column (the lobbyist views) are small and re-walked fully.
  * Upsert by Id (ON CONFLICT DO UPDATE) — rows updated upstream are refreshed.
    Deletions upstream are NOT detected (rare in this feed; documented in UI).
  * The scheduler tick (app/worker/scheduler.py) calls sync_tick() with a time
    budget; the initial ~3M-row load completes across ticks within hours.

Feed quirks (all verified live 2026-07-10):
  * Page size is hard-capped at 100 by the server.
  * The declared entity set ``KNS_DocumentQuerie`` 404s; the working URL is the
    TYPE name ``KNS_DocumentQuery`` — and that endpoint returns a BARE JSON
    array with camelCase property names and offset-less datetimes, so row
    field lookup is case-insensitive and naive datetimes get Asia/Jerusalem.
  * ``V_Lobbyists``/``V_LobbyistsClients`` 404 under /ParliamentInfo and are
    served from the sibling /OdataV4/Lobbyist service instead.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx

from app.config import settings
from app.services import append_store
from app.services.knesset_tables_meta import description_of, group_of

logger = logging.getLogger(__name__)

PARLIAMENT_BASE = "https://knesset.gov.il/OdataV4/ParliamentInfo"
LOBBYIST_BASE = "https://knesset.gov.il/OdataV4/Lobbyist"
PG_SCHEMA = "knesset"
PAGE_SIZE = 100          # server-enforced ceiling — larger $top is ignored
INSERT_BATCH_PAGES = 10  # pages accumulated per INSERT (+ checkpoint)
INCREMENTAL_OVERLAP = timedelta(hours=48)
_ISRAEL_TZ = ZoneInfo("Asia/Jerusalem")

# Entity sets whose URL segment differs from the declared set name.
_SET_URL_OVERRIDES = {"KNS_DocumentQuerie": "KNS_DocumentQuery"}
# Entity sets served from the Lobbyist service, not ParliamentInfo.
_LOBBYIST_SETS = {"V_Lobbyists", "V_LobbyistsClients"}

_EDM_TO_PG = {
    "Edm.Int16": "smallint",
    "Edm.Byte": "smallint",
    "Edm.Int32": "integer",
    "Edm.Int64": "bigint",
    "Edm.Boolean": "boolean",
    "Edm.DateTimeOffset": "timestamptz",
    "Edm.String": "text",
}

_metadata_cache: list["EntitySet"] | None = None
_infra_ready = False
# Serializes sync work (scheduler tick vs. admin-triggered run).
sync_lock = asyncio.Lock()


class EntitySet:
    __slots__ = ("name", "url_name", "base_url", "table", "columns", "has_last_updated")

    def __init__(self, name: str, columns: list[tuple[str, str]]):
        self.name = name
        self.url_name = _SET_URL_OVERRIDES.get(name, name)
        self.base_url = LOBBYIST_BASE if name in _LOBBYIST_SETS else PARLIAMENT_BASE
        self.table = name.lower()
        # [(odata_property, edm_type)] — Id first, as declared in $metadata.
        self.columns = columns
        self.has_last_updated = any(c.lower() == "lastupdateddate" for c, _ in columns)


def is_configured() -> bool:
    return bool(settings.append_database_url) and settings.knesset_db_enabled


def _qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _qtable(table: str) -> str:
    return f"{_qi(PG_SCHEMA)}.{_qi(table)}"


# ── $metadata → EntitySet list ────────────────────────────────────────────────

async def fetch_metadata(client: httpx.AsyncClient) -> list[EntitySet]:
    """Parse the ParliamentInfo $metadata into EntitySet descriptors.

    The ParliamentInfo document declares ALL sets including the lobbyist views
    (whose data lives on the sibling service), so one fetch covers everything."""
    resp = await client.get(f"{PARLIAMENT_BASE}/$metadata")
    resp.raise_for_status()
    root = ET.fromstring(resp.text)
    ns = {"edm": "http://docs.oasis-open.org/odata/ns/edm"}
    types: dict[str, list[tuple[str, str]]] = {}
    for et in root.findall(".//edm:EntityType", ns):
        cols = [
            (p.get("Name"), p.get("Type"))
            for p in et.findall("./edm:Property", ns)
            if p.get("Type") in _EDM_TO_PG
        ]
        types[et.get("Name")] = cols
    sets: list[EntitySet] = []
    for es in root.findall(".//edm:EntityContainer/edm:EntitySet", ns):
        type_name = (es.get("EntityType") or "").rsplit(".", 1)[-1]
        cols = types.get(type_name)
        if not cols:
            logger.warning("knesset_db: set %s has unknown type %s — skipped",
                           es.get("Name"), type_name)
            continue
        sets.append(EntitySet(es.get("Name"), cols))
    return sets


async def get_entity_sets(client: httpx.AsyncClient | None = None) -> list[EntitySet]:
    global _metadata_cache
    if _metadata_cache is None:
        if client is not None:
            _metadata_cache = await fetch_metadata(client)
        else:
            async with _http_client() as c:
                _metadata_cache = await fetch_metadata(c)
    return _metadata_cache


def _http_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(90.0, connect=20.0),
        headers={"Accept": "application/json",
                 "User-Agent": "over.org.il knesset-db mirror (+https://over.org.il)"},
    )


# ── DDL / infrastructure ─────────────────────────────────────────────────────

async def ensure_infra() -> list[EntitySet]:
    """Create the schema, sync_state and every entity table (+ drift ALTERs).
    Runs the DDL once per process; always returns the entity-set list."""
    global _infra_ready
    sets = await get_entity_sets()
    if _infra_ready:
        return sets
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qi(PG_SCHEMA)}")
        await conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_qtable('sync_state')} (
                table_name text PRIMARY KEY,
                entity_set text NOT NULL,
                columns jsonb NOT NULL DEFAULT '[]',
                full_loaded boolean NOT NULL DEFAULT false,
                last_id bigint NOT NULL DEFAULT 0,
                updated_watermark timestamptz,
                source_count bigint,
                total_rows bigint NOT NULL DEFAULT 0,
                status text NOT NULL DEFAULT 'pending',
                error text,
                last_synced_at timestamptz,
                updated_at timestamptz NOT NULL DEFAULT now()
            )
        """)
        for es in sets:
            defs = []
            for prop, edm in es.columns:
                col = prop.lower()
                pg = _EDM_TO_PG[edm]
                defs.append(f"{_qi(col)} {pg} PRIMARY KEY" if col == "id"
                            else f"{_qi(col)} {pg}")
            defs.append('"_synced_at" timestamptz NOT NULL DEFAULT now()')
            await conn.execute(
                f"CREATE TABLE IF NOT EXISTS {_qtable(es.table)} ({', '.join(defs)})"
            )
            # Column drift: upstream added a property since the table was created.
            existing = {
                r["column_name"] for r in await conn.fetch(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema=$1 AND table_name=$2", PG_SCHEMA, es.table)
            }
            for prop, edm in es.columns:
                if prop.lower() not in existing:
                    await conn.execute(
                        f"ALTER TABLE {_qtable(es.table)} "
                        f"ADD COLUMN {_qi(prop.lower())} {_EDM_TO_PG[edm]}"
                    )
            await conn.execute(f"""
                INSERT INTO {_qtable('sync_state')} (table_name, entity_set, columns)
                VALUES ($1, $2, $3)
                ON CONFLICT (table_name) DO UPDATE SET columns = EXCLUDED.columns
            """, es.table, es.name,
                json.dumps([[p, _EDM_TO_PG[e]] for p, e in es.columns]))
    _infra_ready = True
    logger.info("knesset_db: infra ensured (%d tables)", len(sets))
    return sets


# ── Value conversion ─────────────────────────────────────────────────────────

def _to_datetime(v) -> datetime | None:
    if v in (None, ""):
        return None
    try:
        s = str(v).replace("Z", "+00:00")
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            # KNS_DocumentQuery serializes offset-less local (Israel) times.
            d = d.replace(tzinfo=_ISRAEL_TZ)
        return d
    except ValueError:
        logger.warning("knesset_db: unparseable datetime %r", v)
        return None


def _convert(value, edm: str):
    if value is None:
        return None
    if edm == "Edm.DateTimeOffset":
        return _to_datetime(value)
    if edm == "Edm.Boolean":
        return bool(value)
    if edm in ("Edm.Int16", "Edm.Int32", "Edm.Int64", "Edm.Byte"):
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    # Edm.String — Postgres text can't hold NUL bytes.
    s = str(value)
    return s.replace("\x00", "") if "\x00" in s else s


def _row_values(row: dict, es: EntitySet) -> list:
    # Case-insensitive lookup: normal endpoints match $metadata casing exactly,
    # but the KNS_DocumentQuery fallback route serializes camelCase.
    lowered = {k.lower(): v for k, v in row.items()}
    return [_convert(lowered.get(prop.lower()), edm) for prop, edm in es.columns]


# ── Fetch + upsert ───────────────────────────────────────────────────────────

async def _fetch_page(client: httpx.AsyncClient, es: EntitySet, flt: str,
                      *, want_count: bool) -> tuple[list[dict], int | None]:
    """One Id-ordered page. Returns (rows, @odata.count|None). 3 tries."""
    params = {"$filter": flt, "$orderby": "Id", "$top": str(PAGE_SIZE)}
    if want_count:
        params["$count"] = "true"
    last_exc: Exception | None = None
    for attempt in range(3):
        try:
            resp = await client.get(f"{es.base_url}/{es.url_name}", params=params)
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, list):        # bare-array quirk (KNS_DocumentQuery)
                return data, None
            return data.get("value", []), data.get("@odata.count")
        except Exception as e:  # noqa: BLE001 — flaky gov feed, retry with backoff
            last_exc = e
            await asyncio.sleep(2 * (attempt + 1))
    raise RuntimeError(f"knesset_db: {es.name} page fetch failed after retries: {last_exc}")


async def _upsert(conn, es: EntitySet, rows: list[dict]) -> None:
    if not rows:
        return
    cols = [prop.lower() for prop, _ in es.columns]
    n = len(cols)
    cols_sql = ", ".join(_qi(c) for c in cols)
    update_sql = ", ".join(f"{_qi(c)}=EXCLUDED.{_qi(c)}" for c in cols if c != "id")
    max_rows = max(1, append_store._MAX_PARAMS // n)
    for i in range(0, len(rows), max_rows):
        chunk = rows[i:i + max_rows]
        groups, params = [], []
        seen_ids: set = set()
        for r in chunk:
            vals = _row_values(r, es)
            rid = vals[cols.index("id")] if "id" in cols else None
            if rid is None or rid in seen_ids:
                continue
            seen_ids.add(rid)
            ph = ", ".join(f"${len(params) + j + 1}" for j in range(n))
            groups.append(f"({ph})")
            params.extend(vals)
        if not groups:
            continue
        await conn.execute(
            f"INSERT INTO {_qtable(es.table)} ({cols_sql}) VALUES {', '.join(groups)} "
            f'ON CONFLICT ("id") DO UPDATE SET {update_sql}, "_synced_at"=now()',
            *params,
        )


def _fmt_odata_dt(d: datetime) -> str:
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def _sync_one_pass(client: httpx.AsyncClient, es: EntitySet, state: dict,
                         deadline: float) -> tuple[bool, dict]:
    """Advance one table's current pass (full or incremental) until it completes
    or the deadline hits. Returns (pass_completed, updated_state_fields)."""
    pool = await append_store.get_pool()
    watermark = state["updated_watermark"]
    full = (not state["full_loaded"]) or (not es.has_last_updated) or watermark is None
    last_id = int(state["last_id"] or 0)
    pass_started = datetime.now(timezone.utc)

    def flt(after_id: int) -> str:
        if full:
            return f"Id gt {after_id}"
        w = _fmt_odata_dt(watermark - INCREMENTAL_OVERLAP)
        return f"LastUpdatedDate gt {w} and Id gt {after_id}"

    want_count = full and last_id == 0
    pending: list[dict] = []
    pages_since_flush = 0
    source_count = state.get("source_count")

    async def flush() -> None:
        nonlocal pending, pages_since_flush
        if not pending:
            return
        async with pool.acquire() as conn:
            await _upsert(conn, es, pending)
            # total_rows refreshed on every checkpoint (not just pass completion)
            # so the UI shows live progress on a long table instead of 0 — a
            # 200k-row table mid-load otherwise looks stuck for hours.
            await conn.execute(
                f"UPDATE {_qtable('sync_state')} SET last_id=$2, source_count=$3, "
                f"total_rows=(SELECT count(*) FROM {_qtable(es.table)}), "
                f"status='syncing', error=NULL, updated_at=now() WHERE table_name=$1",
                es.table, last_id, source_count,
            )
        pending = []
        pages_since_flush = 0

    completed = False
    while True:
        rows, count = await _fetch_page(client, es, flt(last_id), want_count=want_count)
        if want_count and count is not None:
            source_count = int(count)
        want_count = False
        if rows:
            pending.extend(rows)
            pages_since_flush += 1
            ids = [r.get("Id", r.get("id")) for r in rows]
            last_id = max(int(i) for i in ids if i is not None)
        if len(rows) < PAGE_SIZE:
            completed = True
            await flush()
            break
        if pages_since_flush >= INSERT_BATCH_PAGES:
            await flush()
        if time.monotonic() >= deadline:
            await flush()
            break

    fields: dict = {"source_count": source_count}
    if completed:
        fields.update(
            last_id=0,
            full_loaded=True,
            # New watermark = pass start (minus the overlap applied at query
            # time); rows updated mid-pass are caught by the next pass.
            updated_watermark=pass_started,
            last_synced_at=pass_started,
            status="ok",
            error=None,
        )
    else:
        fields.update(last_id=last_id, status="syncing")
    return completed, fields


async def _write_state(table: str, fields: dict) -> None:
    pool = await append_store.get_pool()
    sets, params = [], [table]
    for k, v in fields.items():
        params.append(v)
        sets.append(f"{_qi(k)}=${len(params)}")
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {_qtable('sync_state')} SET {', '.join(sets)}, updated_at=now() "
            f"WHERE table_name=$1", *params)


async def _refresh_total(table: str) -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            f"UPDATE {_qtable('sync_state')} "
            f"SET total_rows=(SELECT count(*) FROM {_qtable(table)}) "
            f"WHERE table_name=$1", table)


async def sync_tick(budget_seconds: float = 240.0,
                    sync_interval_hours: float = 12.0,
                    only_table: str | None = None) -> dict:
    """One scheduler tick: advance the most-needy tables within the budget.

    Priority: tables mid-full-load (smallest known source first, so most tables
    become queryable early and the 1.9M-row vote table fills last), then tables
    whose last successful sync is older than sync_interval_hours."""
    if not is_configured():
        return {"skipped": "not configured"}
    async with sync_lock:
        deadline = time.monotonic() + budget_seconds
        sets = {es.table: es for es in await ensure_infra()}
        pool = await append_store.get_pool()
        async with pool.acquire() as conn:
            states = [dict(r) for r in await conn.fetch(
                f"SELECT * FROM {_qtable('sync_state')}")]
        by_table = {s["table_name"]: s for s in states}
        now = datetime.now(timezone.utc)

        def due(s: dict) -> bool:
            if s["table_name"] not in sets:
                return False
            if only_table:
                return s["table_name"] == only_table
            if not s["full_loaded"]:
                return True
            last = s["last_synced_at"]
            return last is None or (now - last) > timedelta(hours=sync_interval_hours)

        queue = sorted(
            (s for s in states if due(s)),
            key=lambda s: (s["full_loaded"],                      # full loads first
                           s["last_id"] == 0,                     # resume in-progress first
                           s["source_count"] if s["source_count"] is not None else 2**62),
        )
        worked, completed_tables = [], []
        for s in queue:
            if time.monotonic() >= deadline:
                break
            es = sets[s["table_name"]]
            try:
                completed, fields = await _run_pass(es, s, deadline)
                await _write_state(es.table, fields)
                if completed:
                    await _refresh_total(es.table)
                    completed_tables.append(es.table)
                worked.append(es.table)
            except Exception as e:  # noqa: BLE001 — isolate per-table failures
                logger.exception("knesset_db: sync of %s failed", es.table)
                await _write_state(es.table, {"status": "error", "error": str(e)[:500]})
        return {"worked": worked, "completed": completed_tables}


async def _run_pass(es: EntitySet, state: dict, deadline: float) -> tuple[bool, dict]:
    async with _http_client() as client:
        return await _sync_one_pass(client, es, state, deadline)


async def reset_table(table: str) -> None:
    """Admin: force a fresh full walk of one table (data stays; rows re-upsert)."""
    await _write_state(table, {"full_loaded": False, "last_id": 0,
                               "status": "pending", "error": None})


# ── Read side ────────────────────────────────────────────────────────────────

async def list_tables() -> list[dict]:
    """All tables with schema, sync state and Hebrew descriptions (for the UI)."""
    sets = {es.table: es for es in await ensure_infra()}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        states = await conn.fetch(f"SELECT * FROM {_qtable('sync_state')} ORDER BY table_name")
    out = []
    for r in states:
        if r["table_name"] not in sets:
            continue
        es = sets[r["table_name"]]
        cols = json.loads(r["columns"]) if isinstance(r["columns"], str) else r["columns"]
        out.append({
            "table": r["table_name"],
            "entity_set": es.name,
            "group": group_of(es.name),
            "description": description_of(es.name),
            "columns": [{"name": p.lower(), "type": t} for p, t in cols],
            "total_rows": int(r["total_rows"] or 0),
            "source_count": int(r["source_count"]) if r["source_count"] is not None else None,
            "full_loaded": bool(r["full_loaded"]),
            "status": r["status"],
            "error": r["error"],
            "last_synced_at": r["last_synced_at"].isoformat() if r["last_synced_at"] else None,
        })
    return out


async def run_sql(sql: str, *, max_rows: int = 1000, timeout_ms: int = 20000) -> dict:
    """User-supplied read-only SELECT, executed with search_path=knesset so the
    manual's table names work unqualified (SELECT * FROM kns_bill). Same
    defense-in-depth as append_store.run_readonly_sql: single statement,
    SELECT/WITH only, READ ONLY tx, statement_timeout, row cap."""
    s = (sql or "").strip().rstrip(";").strip()
    if not s:
        raise ValueError("השאילתה ריקה")
    if ";" in s:
        raise ValueError("רק משפט יחיד מותר (ללא ';')")
    if not append_store._SQL_STARTS_OK.match(s):
        raise ValueError("רק שאילתות SELECT / WITH מותרות")
    if append_store._SQL_DENY.search(s):
        raise ValueError("רק קריאה (SELECT) מותרת — אסורות פעולות כתיבה/שינוי")
    wrapped = f"SELECT * FROM (\n{s}\n) _q LIMIT {int(max_rows) + 1}"
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            await conn.execute(f"SET LOCAL search_path = {_qi(PG_SCHEMA)}, public")
            stmt = await conn.prepare(wrapped)
            cols = [a.name for a in stmt.get_attributes()]
            recs = await stmt.fetch()
    truncated = len(recs) > max_rows
    rows = [
        {k: (v if (v is None or isinstance(v, (str, int, float, bool))) else str(v))
         for k, v in dict(r).items()}
        for r in recs[:max_rows]
    ]
    return {"columns": cols, "rows": rows, "truncated": truncated, "row_count": len(rows)}


async def iter_sql_csv(sql: str, *, max_rows: int = 200_000, timeout_ms: int = 60_000):
    """Stream a query result as CSV lines (utf-8 BOM first) without holding the
    whole result in memory — for the export endpoint."""
    s = (sql or "").strip().rstrip(";").strip()
    if not s or ";" in s or not append_store._SQL_STARTS_OK.match(s) \
            or append_store._SQL_DENY.search(s):
        raise ValueError("שאילתת ייצוא לא חוקית — SELECT יחיד בלבד")
    wrapped = f"SELECT * FROM (\n{s}\n) _q LIMIT {int(max_rows)}"

    def _csv_cell(v) -> str:
        if v is None:
            return ""
        sv = v.isoformat() if isinstance(v, datetime) else str(v)
        if any(ch in sv for ch in (',', '"', '\n', '\r')):
            sv = '"' + sv.replace('"', '""') + '"'
        return sv

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction(readonly=True):
            await conn.execute(f"SET LOCAL statement_timeout = {int(timeout_ms)}")
            await conn.execute(f"SET LOCAL search_path = {_qi(PG_SCHEMA)}, public")
            first = True
            async for rec in conn.cursor(wrapped, prefetch=2000):
                if first:
                    yield "﻿" + ",".join(_csv_cell(k) for k in rec.keys()) + "\r\n"
                    first = False
                yield ",".join(_csv_cell(v) for v in rec.values()) + "\r\n"
            if first:  # empty result — still emit the header if we can't know cols
                yield "﻿\r\n"


async def status_summary() -> dict:
    """Compact stats for the page header / nav badge."""
    if not is_configured():
        return {"enabled": False}
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        try:
            r = await conn.fetchrow(f"""
                SELECT count(*) AS tables,
                       count(*) FILTER (WHERE full_loaded) AS loaded,
                       coalesce(sum(total_rows), 0) AS rows,
                       max(last_synced_at) AS last_sync,
                       max(updated_at) AS last_activity
                FROM {_qtable('sync_state')}
            """)
        except Exception:  # schema not created yet (first boot)
            return {"enabled": True, "tables": 0, "loaded": 0, "rows": 0, "last_sync": None}
    return {
        "enabled": True,
        "tables": int(r["tables"]),
        "loaded": int(r["loaded"]),
        "rows": int(r["rows"]),
        "last_sync": r["last_sync"].isoformat() if r["last_sync"] else None,
        "last_activity": r["last_activity"].isoformat() if r["last_activity"] else None,
    }
