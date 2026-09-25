"""One queryable table over a dataset that arrives split into many tables.

A multi-resource dataset archived to SQL gets one table per resource
(append_store.table_name_for_resource). When the resources are the SAME table
cut by one dimension — the rain forecast is 60 CSVs, one per station — the
dimension exists only in the resource's NAME, so no single query can compare
stations: every table holds identical-looking rows with no station column.

This module lays a VIEW over such a dataset: UNION ALL of its tables, with the
dimension recovered from each resource name as a real column. A view costs no
storage and is always current; a filter on the added column is constant-folded
per branch, so ``WHERE station = 'ELON'`` reads one table, not sixty.

Which datasets get one, and how the label is read from the resource name, is
declared in UNION_VIEWS. The view is rebuilt when the set of tables or their
columns changes (a signature in the view's comment), checked at boot and when
the /data catalog is read.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import time
import uuid

from app.services import append_store

logger = logging.getLogger(__name__)

# Internal columns a view does not carry.
_INTERNAL = {"row_hash", "_row_hash", "geom"}
_CHECK_TTL = 600
_state: dict = {"checked_at": 0.0}

UNION_VIEWS: dict[str, dict] = {
    # נתונים חזויים- גשם (השירות המטאורולוגי): 60 resources named
    # "<STATION>_pr_15models_rcp45_rcp85_QDM.csv", one table each.
    "d3d20a89-5c41-4b49-a7a8-cc1c73e070f3": {
        "view": "rain_forecast_stations",
        "label_column": "station",
        "label_regex": r"^(.+?)_pr_",
        "label_caption": "תחנה (מתוך שם הקובץ במקור, למשל ELON)",
        "title": "תחזית גשם ארוכת טווח — כל התחנות (טבלה אחידה)",
        "description": (
            "נתונים חזויים של השירות המטאורולוגי: משקעים יומיים (pr, מ\"מ) 2006-2100 "
            "לפי מודל ותרחיש, כל 60 התחנות בטבלה אחת עם עמודת station. "
            "סננו לפי station / model / scenario / year; כל הערכים טקסט, "
            "המירו עם NULLIF(pr,'')::numeric."
        ),
    },
}


def _qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _lit(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def label_of(resource_name: str | None, table: str, regex: str | None) -> str:
    """The dimension value for one resource (pure). Falls back to the whole
    name, then to the table, so a renamed resource still gets a distinct label."""
    name = (resource_name or "").strip()
    if name and regex:
        m = re.search(regex, name)
        if m:
            return m.group(1)
    return name or table


def view_sql(spec: dict, branches: list[tuple[str, str]], columns: dict[str, list[str]]) -> str | None:
    """UNION ALL over ``branches`` [(table, label)] (pure).

    Columns are the union of the tables' own columns in first-seen order; a
    table missing one reads NULL there rather than being dropped."""
    order: list[str] = []
    for table, _ in branches:
        for c in columns.get(table, []):
            if c not in _INTERNAL and c not in order:
                order.append(c)
    if not order:
        return None
    parts = []
    for table, label in branches:
        have = set(columns.get(table, []))
        if not have:
            continue
        cols = ", ".join(_qi(c) if c in have else f"NULL::text AS {_qi(c)}" for c in order)
        parts.append(f"SELECT {_lit(label)}::text AS {_qi(spec['label_column'])}, {cols} "
                     f"FROM public.{_qi(table)}")
    return "\nUNION ALL\n".join(parts) if parts else None


def signature(branches: list[tuple[str, str]], columns: dict[str, list[str]]) -> str:
    raw = json.dumps([[t, l, columns.get(t, [])] for t, l in branches], ensure_ascii=False)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


async def _branches(db, ds_id: str, spec: dict) -> list[tuple[str, str]]:
    """(table, label) for the dataset's latest version, in its resource order."""
    from sqlalchemy import select
    from app.models.tracked_dataset import TrackedDataset
    from app.models.version_index import VersionIndex

    ds = await db.get(TrackedDataset, uuid.UUID(ds_id))
    if ds is None:
        return []
    v = (await db.execute(
        select(VersionIndex).where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc()).limit(1))).scalar_one_or_none()
    if v is None:
        return []
    out = []
    for t in append_store.tables_from_mappings(ds, v.resource_mappings):
        out.append((t["table"], label_of(t.get("resource_name"), t["table"], spec.get("label_regex"))))
    return out


async def ensure_views(*, force: bool = False) -> dict:
    """(Re)build every declared view whose tables changed. Cheap when none did."""
    from app.database import async_session
    from app.services.index_mirror import _readonly_role

    now = time.monotonic()
    if not force and now - _state["checked_at"] < _CHECK_TTL:
        return {"skipped": "checked recently"}
    _state["checked_at"] = now
    built = []
    pool = await append_store.get_pool()
    for ds_id, spec in UNION_VIEWS.items():
        async with async_session() as db:
            branches = await _branches(db, ds_id, spec)
        if not branches:
            continue
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_schema='public' AND table_name = ANY($1::text[]) "
                "ORDER BY table_name, ordinal_position", [t for t, _ in branches])
            columns: dict[str, list[str]] = {}
            for r in rows:
                columns.setdefault(r["table_name"], []).append(r["column_name"])
            present = [(t, l) for t, l in branches if t in columns]
            sig = signature(present, columns)
            view = spec["view"]
            comment = await conn.fetchval(
                "SELECT obj_description(to_regclass($1), 'pg_class')", f"public.{_qi(view)}")
            if not force and f"sig={sig}" in (comment or ""):
                continue
            body = view_sql(spec, present, columns)
            if not body:
                continue
            role = _readonly_role()
            async with conn.transaction():
                await conn.execute("SELECT pg_advisory_xact_lock(hashtext($1))", f"union_views:{view}")
                await conn.execute(f"DROP VIEW IF EXISTS public.{_qi(view)}")
                await conn.execute(f"CREATE VIEW public.{_qi(view)} AS\n{body}")
                await conn.execute(
                    f"COMMENT ON VIEW public.{_qi(view)} IS "
                    f"{_lit(spec['title'] + ' · over.org.il/versions/' + ds_id + ' · sig=' + sig)}")
                if role:
                    await conn.execute(f"GRANT SELECT ON public.{_qi(view)} TO {_qi(role)}")
            built.append({"view": view, "tables": len(present)})
    if built:
        logger.info("union views rebuilt: %s", built)
    return {"built": built}


async def ensure_views_safely() -> None:
    try:
        await ensure_views()
    except Exception as e:  # noqa: BLE001 — never break a read or boot on this
        logger.warning("union views not rebuilt: %s", e)


def spec_for_view(view: str) -> tuple[str, dict] | None:
    """(dataset_id, spec) for a view name, for the /data catalog."""
    for ds_id, spec in UNION_VIEWS.items():
        if spec["view"] == view:
            return ds_id, spec
    return None
