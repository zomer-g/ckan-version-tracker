"""The removals log — what publishers took down, one row per event.

Three kinds of removal, each read from where it is already recorded:

* ``dataset_gone``   a whole tracked source the publisher removed
                     (TrackedDataset.source_gone_at — set only on a certain
                     verdict, see the model).
* ``file_removed``   a file (resource) present in one version of a dataset and
                     absent from the next. ``replaced`` when a new file with the
                     same name arrived in that same version — the publisher
                     swapped the file rather than dropping it.
* ``layer_removed``  a GovMap layer that left GovMap's catalog
                     (over_govmap_layer_changes, change='removed').

Why file removals compare only versions of the SAME kind
--------------------------------------------------------
A dataset's history mixes version kinds that list different sets of files: the
legacy CKAN snapshot (change_summary.type NULL) lists every resource, PDFs
included; the datastore poll (append_db / append_db_multi) lists only the
resources it loads; a scraper pass lists its own outputs. Diffing across kinds
reported files as "removed" that never left data.gov.il — measured on a sample
of 80 data.gov.il datasets, every apparent removal was such a kind change. So a
pair of versions is compared only when both belong to one family, both carry a
non-empty ``_resource_ids`` list, and neither is a partial run (a sample of the
corpus on purpose, which omits files by design).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

MAX_DAYS = 365


def family(change_summary: dict | None) -> str | None:
    """Which kind of version this is, for the purpose of comparing file lists.

    None means "do not compare": a scraper pass or a partial run."""
    cs = change_summary or {}
    if cs.get("partial_run"):
        return None
    kind = cs.get("type")
    if kind is None:
        return "ckan"
    if str(kind).startswith("append_db"):
        return "datastore"
    return None


def diff_removals(versions: Iterable[dict]) -> list[dict]:
    """File removals across one dataset's versions.

    ``versions``: dicts with version_number, detected_at, change_summary and
    resource_mappings, in any order. Returns one dict per removed file, dated
    by the version it was missing from."""
    last_by_family: dict[str, dict] = {}
    out: list[dict] = []
    for v in sorted(versions, key=lambda v: v["version_number"]):
        fam = family(v.get("change_summary"))
        rm = v.get("resource_mappings") or {}
        ids = rm.get("_resource_ids")
        if fam is None or not isinstance(ids, list) or not ids:
            continue
        prev = last_by_family.get(fam)
        last_by_family[fam] = v
        if prev is None:
            continue
        prev_rm = prev.get("resource_mappings") or {}
        prev_ids = prev_rm.get("_resource_ids") or []
        now_ids = set(ids)
        prev_names: dict = prev_rm.get("_names") or {}
        new_names: dict = rm.get("_names") or {}
        added_names = {_base(new_names.get(i)) for i in ids if i not in set(prev_ids)}
        added_names.discard(None)
        for rid in prev_ids:
            if rid in now_ids:
                continue
            name = prev_names.get(rid)
            out.append({
                "kind": "file_removed",
                "at": v["detected_at"],
                "resource_id": rid,
                "name": name,
                "replaced": _base(name) is not None and _base(name) in added_names,
                "version_number": v["version_number"],
                "previous_version": prev["version_number"],
            })
    return out


def _base(name: str | None) -> str | None:
    """A file's name without the ' — 13.07.2026' stamp OVER appends to it."""
    if not name:
        return None
    return str(name).split(" — ")[0].strip() or None


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).isoformat()


_FILE_SQL = text("""
    SELECT v.tracked_dataset_id AS ds, v.version_number, v.detected_at,
           v.change_summary, v.resource_mappings
    FROM version_index v
    WHERE v.tracked_dataset_id IN (
        SELECT tracked_dataset_id FROM version_index WHERE detected_at >= :since
    )
    AND jsonb_typeof(v.resource_mappings -> '_resource_ids') = 'array'
    AND jsonb_array_length(v.resource_mappings -> '_resource_ids') > 0
""")


async def collect(db: AsyncSession, days: int = 30, *, now: datetime | None = None) -> dict[str, Any]:
    """Every removal in the last ``days`` days, newest first."""
    from app.models.tracked_dataset import TrackedDataset
    from sqlalchemy import select

    days = max(1, min(int(days), MAX_DAYS))
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    events: list[dict] = []
    errors: list[str] = []

    # Files. The scan reads every version of each dataset that had a version
    # in the window — the version before the window is what the first one in
    # it is compared against.
    by_ds: dict[Any, list[dict]] = {}
    for r in (await db.execute(_FILE_SQL, {"since": since})).mappings():
        by_ds.setdefault(r["ds"], []).append(dict(r))
    file_events: list[tuple[Any, dict]] = []
    for ds_id, vs in by_ds.items():
        for e in diff_removals(vs):
            at = e["at"] if e["at"].tzinfo else e["at"].replace(tzinfo=timezone.utc)
            if at >= since:
                file_events.append((ds_id, e))

    # Titles for the file rows, and the datasets whose source is gone.
    ids = {ds for ds, _ in file_events}
    titles: dict[Any, tuple[str, str | None]] = {}
    if ids:
        for i, t, u in (await db.execute(
            select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.source_url)
            .where(TrackedDataset.id.in_(ids))
        )).all():
            titles[i] = (t, u)
    for ds_id, e in file_events:
        t, u = titles.get(ds_id, (None, None))
        events.append({**e, "at": _iso(e["at"]), "dataset_id": str(ds_id),
                       "dataset_title": t, "source_url": u})

    for i, t, u, at in (await db.execute(
        select(TrackedDataset.id, TrackedDataset.title, TrackedDataset.source_url,
               TrackedDataset.source_gone_at)
        .where(TrackedDataset.source_gone_at >= since)
    )).all():
        events.append({"kind": "dataset_gone", "at": _iso(at), "dataset_id": str(i),
                       "dataset_title": t, "source_url": u, "name": t})

    # GovMap layers — in the archive DB, not the app DB.
    try:
        from app.services import append_store
        from app.services.govmap_layer_index import CHANGES_TABLE, LAYER_URL
        if append_store.is_configured():
            pool = await append_store.get_pool()
            async with pool.acquire() as conn:
                exists = await conn.fetchval("SELECT to_regclass($1)", f"public.{CHANGES_TABLE}")
                if exists:
                    for r in await conn.fetch(
                        f'SELECT changed_at, layer_id, caption FROM public."{CHANGES_TABLE}" '
                        "WHERE change = 'removed' AND changed_at >= $1", since):
                        events.append({
                            "kind": "layer_removed", "at": _iso(r["changed_at"]),
                            "name": r["caption"], "layer_id": r["layer_id"],
                            "source_url": LAYER_URL.format(id=r["layer_id"]),
                        })
    except Exception as e:  # noqa: BLE001 — one missing source must not blank the log
        logger.warning("removals log: GovMap layer changes unreadable: %s", e)
        errors.append(f"שכבות GovMap: {e}"[:300])

    events.sort(key=lambda e: e.get("at") or "", reverse=True)
    counts: dict[str, int] = {}
    for e in events:
        k = "file_replaced" if e.get("replaced") else e["kind"]
        counts[k] = counts.get(k, 0) + 1
    return {"days": days, "since": since.isoformat(), "counts": counts,
            "events": events, "errors": errors}
