"""GovMap layer index: ``public.over_govmap_layers`` + ``over_govmap_layer_changes``.

Every layer in GovMap's catalog, one row each, with what the site shows under
"אודות השכבה": name, description, source (office or local authority), last
update, category, keywords. No layer data. Each layer's features are a
separate govmap dataset on their own schedule (govmap_coverage.py).

It is an index OF the catalog, built by גרסאות לעם, so it sits with the other
``over_*`` processed indexes in /data rather than being a tracked dataset. The
point is following which layers exist, so the table is never truncated. A
layer that leaves the catalog keeps its row with ``removed_at`` set, and every
difference between two reads goes to ``over_govmap_layer_changes`` (added,
removed, returned, renamed, republished, metadata).

Built from the same catalog response ``populate_from_catalog`` already fetches
daily (catalog_watch), so it costs no extra request.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.services import append_store, data_catalog
from app.services.append_store import _qi

logger = logging.getLogger(__name__)

LAYERS_TABLE = "over_govmap_layers"
CHANGES_TABLE = "over_govmap_layer_changes"

LAYER_URL = "https://www.govmap.gov.il/?lay={id}"

# A read with far fewer layers than the table holds is a GovMap deploy window
# or a schema change, not hundreds of layers deleted overnight. Below this
# fraction of the present layers, nothing is marked removed.
MIN_PRESENT_FRACTION = 0.8

# Catalog fields that, when they change, are logged as a "metadata" change.
# caption has its own kind (renamed), update_date its own (republished).
_METADATA_FIELDS = ("description", "publisher", "publisher_type", "category",
                    "keywords", "layer_groups", "wfs_name", "needs_credentials")

_LAYERS_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(LAYERS_TABLE)} (
    layer_id           text PRIMARY KEY,
    caption            text,
    description        text,
    publisher          text,
    publisher_type     text,
    category           text,
    keywords           text,
    layer_groups       text,
    technical_name     text,
    wfs_name           text,
    data_version       integer,
    layer_kind         integer,
    needs_credentials  boolean,
    source_updated_at  timestamptz,
    layer_url          text,
    in_catalog         boolean,
    first_seen_at      timestamptz,
    last_seen_at       timestamptz,
    removed_at         timestamptz,
    refreshed_at       timestamptz
)
"""

_CHANGES_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(CHANGES_TABLE)} (
    changed_at  timestamptz,
    layer_id    text,
    caption     text,
    change      text,
    field       text,
    old_value   text,
    new_value   text
)
"""

_LAYER_COLS = [
    "layer_id", "caption", "description", "publisher", "publisher_type",
    "category", "keywords", "layer_groups", "technical_name", "wfs_name",
    "data_version", "layer_kind", "needs_credentials", "source_updated_at",
    "layer_url",
]

# publicPublishType → the groups table publisherId indexes into. The key
# spaces overlap, so the type is load-bearing (see govmap_coverage).
_PUBLISHER_TABLE = {0: "offices", 1: "settlements"}


def parse_source_date(raw) -> datetime | None:
    """``updateDate`` as aware UTC. GovMap writes it naive, in UTC: layer 414's
    "2024-07-30 07:47:33" is shown on the site as 10:47 (IDT)."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.fromisoformat(raw.strip().replace(" ", "T").replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _name(table, key) -> str | None:
    if key is None or not isinstance(table, dict):
        return None
    entry = table.get(str(key))
    name = entry.get("name") if isinstance(entry, dict) else None
    return (str(name).strip() or None) if name else None


def _int(v) -> int | None:
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def layer_rows(catalog: dict) -> list[dict]:
    """One dict per catalog layer (keys = ``_LAYER_COLS``), sorted by id."""
    if not isinstance(catalog, dict):
        return []
    groups = catalog.get("groups") or {}
    sources = catalog.get("sources") or {}

    in_groups: dict[str, list[str]] = {}
    for g in catalog.get("userGroups") or []:
        if not isinstance(g, dict):
            continue
        gname = str(g.get("groupName") or "").strip()
        for node in g.get("nodes") or []:
            if gname and node.get("nodeType", "layer") == "layer" and node.get("nodeId") is not None:
                in_groups.setdefault(str(node["nodeId"]), []).append(gname)

    out: dict[str, dict] = {}
    for e in catalog.get("catalog") or []:
        if not isinstance(e, dict) or e.get("id") is None:
            continue
        lid = str(e["id"]).strip()
        ptype = e.get("publicPublishType")
        kw = e.get("keywords") or []
        if isinstance(kw, str):
            kw = [kw]
        out[lid] = {
            "layer_id": lid,
            "caption": (str(e.get("caption") or "").strip() or None),
            "description": (str(e.get("description") or "").strip() or None),
            "publisher": _name(groups.get(_PUBLISHER_TABLE.get(ptype, "")), e.get("publisherId")),
            "publisher_type": _name(sources, ptype),
            "category": _name(groups.get("categories"), e.get("categoryId")),
            "keywords": ", ".join(str(k).strip() for k in kw if str(k).strip()) or None,
            "layer_groups": ", ".join(in_groups.get(lid, [])) or None,
            "technical_name": (str(e.get("name") or "").strip() or None),
            "wfs_name": (str(e.get("serviceLayerId") or "").strip() or None),
            "data_version": _int(e.get("featureVersion")),
            "layer_kind": _int(e.get("layerKind")),
            "needs_credentials": bool(e.get("needCredentials")),
            "source_updated_at": parse_source_date(e.get("updateDate")),
            "layer_url": LAYER_URL.format(id=lid),
        }
    return sorted(out.values(),
                  key=lambda r: (0, int(r["layer_id"])) if r["layer_id"].isdigit() else (1, 0))


def _text(v) -> str | None:
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.astimezone(timezone.utc).isoformat()
    return str(v)


def diff(old: dict[str, dict], new: list[dict], now: datetime,
         *, allow_removals: bool) -> list[tuple]:
    """Change-log records between the stored rows and a fresh read.

    ``old`` is layer_id → stored row (with ``in_catalog``). An empty ``old`` is
    the first build: the baseline, not 900 additions, so it logs nothing."""
    if not old:
        return []
    out: list[tuple] = []
    seen = set()
    for r in new:
        lid = r["layer_id"]
        seen.add(lid)
        prev = old.get(lid)
        cap = r["caption"]
        if prev is None:
            out.append((now, lid, cap, "added", None, None, None))
            continue
        if not prev.get("in_catalog"):
            out.append((now, lid, cap, "returned", None, None, None))
        if prev.get("caption") != cap:
            out.append((now, lid, cap, "renamed", "caption", prev.get("caption"), cap))
        if (r["source_updated_at"] and prev.get("source_updated_at") != r["source_updated_at"]) \
                or (r["data_version"] is not None and prev.get("data_version") != r["data_version"]):
            out.append((now, lid, cap, "republished", "source_updated_at",
                        _text(prev.get("source_updated_at")), _text(r["source_updated_at"])))
        for f in _METADATA_FIELDS:
            if prev.get(f) != r[f]:
                out.append((now, lid, cap, "metadata", f, _text(prev.get(f)), _text(r[f])))
    if allow_removals:
        for lid, prev in old.items():
            if lid not in seen and prev.get("in_catalog"):
                out.append((now, lid, prev.get("caption"), "removed", None, None, None))
    return out


async def refresh(catalog: dict) -> dict:
    """Merge one catalog read into the index. Never truncates."""
    if not append_store.is_configured():
        return {"skipped": "append DB not configured"}
    rows = layer_rows(catalog)
    if not rows:
        return {"error": "no layers in catalog"}
    now = datetime.now(timezone.utc)

    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_LAYERS_DDL)
        await conn.execute(_CHANGES_DDL)
        from app.services.index_mirror import _readonly_role
        role = _readonly_role()
        if role:
            for t in (LAYERS_TABLE, CHANGES_TABLE):
                await conn.execute(f"GRANT SELECT ON public.{_qi(t)} TO {_qi(role)}")

        old = {
            rec["layer_id"]: dict(rec)
            for rec in await conn.fetch(f"SELECT * FROM public.{_qi(LAYERS_TABLE)}")
        }
        present = sum(1 for r in old.values() if r.get("in_catalog"))
        allow_removals = len(rows) >= MIN_PRESENT_FRACTION * present
        if not allow_removals:
            logger.warning("govmap_layer_index: read has %d layers vs %d present; "
                           "not marking any removed", len(rows), present)
        changes = diff(old, rows, now, allow_removals=allow_removals)

        cols = ", ".join(_qi(c) for c in _LAYER_COLS)
        params = ", ".join(f"${i}" for i in range(1, len(_LAYER_COLS) + 1))
        n = len(_LAYER_COLS)
        updates = ", ".join(f"{_qi(c)} = EXCLUDED.{_qi(c)}" for c in _LAYER_COLS[1:])
        upsert = (
            f"INSERT INTO public.{_qi(LAYERS_TABLE)} ({cols}, in_catalog, first_seen_at, "
            f"last_seen_at, removed_at, refreshed_at) "
            f"VALUES ({params}, true, ${n + 1}, ${n + 1}, NULL, ${n + 1}) "
            f"ON CONFLICT (layer_id) DO UPDATE SET {updates}, in_catalog = true, "
            f"last_seen_at = EXCLUDED.last_seen_at, removed_at = NULL, "
            f"refreshed_at = EXCLUDED.refreshed_at"
        )
        removed = [lid for (_, lid, _, kind, *_r) in changes if kind == "removed"]
        async with conn.transaction():
            await conn.executemany(upsert, [
                tuple(r[c] for c in _LAYER_COLS) + (now,) for r in rows
            ])
            if removed:
                await conn.execute(
                    f"UPDATE public.{_qi(LAYERS_TABLE)} SET in_catalog = false, "
                    f"removed_at = $1, refreshed_at = $1 WHERE layer_id = ANY($2::text[])",
                    now, removed)
            if changes:
                await conn.copy_records_to_table(
                    CHANGES_TABLE, schema_name="public",
                    columns=["changed_at", "layer_id", "caption", "change",
                             "field", "old_value", "new_value"],
                    records=changes)
        await conn.execute(f"ANALYZE public.{_qi(LAYERS_TABLE)}")

    if not old:
        # New tables ⇒ the /data catalog must not serve a stale list.
        data_catalog.invalidate_catalog_cache()
    summary: dict = {"layers": len(rows), "changes": len(changes)}
    for c in changes:
        summary[c[3]] = summary.get(c[3], 0) + 1
    logger.info("govmap_layer_index: %s", summary)
    return summary
