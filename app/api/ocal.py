"""Public read API for "יומן לעם" (Ocal), migrated into OVER.

Ports Ocal's ``/api/public/*`` Express routes to ``/api/ocal/*`` on OVER's
FastAPI, querying the migrated Ocal database through app/services/ocal_db.py
(a dedicated Neon Postgres — see that module and the startup guard in main.py).

Endpoints (all public, rate-limited):
    GET  /api/ocal/events                  full-text + faceted event search
    GET  /api/ocal/events/{id}             single event
    GET  /api/ocal/events/{id}/entities    extracted entities for an event
    GET  /api/ocal/events/{id}/cross-refs  cross-diary verification refs
    GET  /api/ocal/events/{id}/matches     same-day cross-diary duplicates
    GET  /api/ocal/sources                 enabled diary sources
    GET  /api/ocal/sources/{id}            single diary source
    GET  /api/ocal/calendar                events for a calendar window
    GET  /api/ocal/stats                   hero counters (cached 5m)
    GET  /api/ocal/entities                top entities (matview + cache)
    GET  /api/ocal/content                 site CMS key/values
    GET  /api/ocal/download/source/{id}    single-diary CSV/JSON export
    POST /api/ocal/download/bulk           multi-diary ZIP export

The Hebrew tsquery construction (geresh/gershayim stripping, prefix vs exact
abbreviation matching, boolean AND/OR/NOT) mirrors Ocal's DiaryEvent model and
must stay aligned with the ``search_vector`` trigger (Ocal migration 021).

Not ported here: the MK-expenses layer (deferred) and the admin surface
(app/api/ocal_admin.py, later phase).
"""
from __future__ import annotations

import csv
import io
import re
import time
import uuid
import zipfile

from fastapi import APIRouter, HTTPException, Path, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.rate_limit import limiter
from app.services import ocal_db

router = APIRouter(prefix="/api/ocal", tags=["ocal"])

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_configured() -> None:
    if not ocal_db.is_configured():
        raise HTTPException(
            status_code=503,
            detail="יומן לעם אינו זמין כרגע (OCAL_DATABASE_URL not configured).",
        )


def _valid_uuid(s: str) -> bool:
    try:
        uuid.UUID(str(s))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def _check_date(name: str, val: str | None) -> None:
    if val is not None and not _DATE_RE.match(val):
        raise HTTPException(status_code=400, detail=f"{name} must be YYYY-MM-DD")


# asyncpg binds parameters by their Postgres type and does NOT coerce a Python
# str into a DATE / UUID column (unlike node-pg, which the original Ocal used).
# Convert at bind time so ``e.event_date >= $1`` / ``e.id = $1`` work.
def _as_date(s):
    from datetime import date as _d
    return _d.fromisoformat(s) if isinstance(s, str) else s


def _as_uuid(s):
    return s if isinstance(s, uuid.UUID) else uuid.UUID(str(s))


# ── Hebrew full-text query construction (mirrors Ocal DiaryEvent model) ──────
# Strip geresh (׳ U+05F3), gershayim (״ U+05F4) and ASCII quote/apostrophe so an
# abbreviation like מח"ש collapses into one token, matching the search_vector
# trigger (Ocal migration 021).
_GERESH_RE = re.compile("[״׳\"']")
_BOOL_RE = re.compile(r"\b(AND|OR|NOT)\b", re.IGNORECASE)


def _strip_geresh(s: str) -> str:
    return _GERESH_RE.sub("", s)


def _build_token(tok: str) -> str:
    # Original had a geresh → exact match (abbreviation). Else → prefix match, so
    # Hebrew word-form variants still match.
    stripped = _strip_geresh(tok)
    return stripped if stripped != tok else f"{tok}:*"


def _build_tsquery(q: str) -> str:
    trimmed = q.strip()
    toks = [t for t in re.split(r"\s+", trimmed) if t]
    if not _BOOL_RE.search(trimmed):
        return " & ".join(_build_token(t) for t in toks)
    out: list[str] = []
    for tok in toks:
        u = tok.upper()
        if u == "AND":
            out.append("&")
        elif u == "OR":
            out.append("|")
        elif u == "NOT":
            out.append("!")
        else:
            out.append(_build_token(tok))
    return " ".join(out)


class _Args:
    """Accumulates positional asyncpg params, handing back ``$N`` placeholders."""

    def __init__(self) -> None:
        self.vals: list = []

    def add(self, v) -> str:
        self.vals.append(v)
        return f"${len(self.vals)}"


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------

_SEARCH_SELECT = [
    "e.*",
    "s.name AS source_name",
    "s.color AS source_color",
    "(s.reviewed_at IS NOT NULL) AS source_reviewed",
    "(SELECT total_events FROM similar_events WHERE id = e.match_group_id) AS match_count",
    "(SELECT json_agg(sub) FROM ("
    "  SELECT ee.entity_name AS name, ee.entity_type AS type"
    "  FROM event_entities ee"
    "  WHERE ee.event_id = e.id AND ee.confidence >= 0.5"
    "  GROUP BY ee.entity_name, ee.entity_type"
    "  ORDER BY MAX(ee.confidence) DESC LIMIT 5"
    ") sub) AS top_entities",
    "(SELECT json_build_object("
    "  'confirmed', COUNT(*) FILTER (WHERE status='confirmed'),"
    "  'unconfirmed', COUNT(*) FILTER (WHERE status='unconfirmed'),"
    "  'total', COUNT(*)"
    ") FROM entity_cross_refs WHERE source_event_id = e.id) AS cross_ref_summary",
]

_FROM = "diary_events e JOIN diary_sources s ON e.source_id = s.id"


async def _search_events(
    *, q, from_date, to_date, source_ids, location, participants,
    entity_names, cross_ref_status, sort, offset, limit,
) -> tuple[list[dict], int]:
    a = _Args()
    where = ["e.is_active = true", "s.is_enabled = true"]

    ts_ph = None
    if q:
        tsq = _build_tsquery(q)
        ts_ph = a.add(tsq)
        if _BOOL_RE.search(q):
            # Boolean mode: only tsquery (ILIKE can't replicate boolean logic).
            where.append(f"e.search_vector @@ to_tsquery('hebrew', {ts_ph})")
        else:
            il = a.add(f"%{q}%")
            where.append(
                f"(e.search_vector @@ to_tsquery('hebrew', {ts_ph}) "
                f"OR e.title ILIKE {il})"
            )
    if from_date:
        where.append(f"e.event_date >= {a.add(_as_date(from_date))}")
    if to_date:
        where.append(f"e.event_date <= {a.add(_as_date(to_date))}")
    if source_ids:
        phs = ",".join(a.add(_as_uuid(sid)) for sid in source_ids)
        where.append(f"e.source_id IN ({phs})")
    if location:
        where.append(f"e.location ILIKE {a.add('%' + location + '%')}")
    if participants:
        where.append(f"e.participants ILIKE {a.add('%' + participants + '%')}")
    if entity_names:
        phs = ",".join(a.add(n) for n in entity_names)
        where.append(
            "EXISTS (SELECT 1 FROM event_entities ee WHERE ee.event_id = e.id "
            f"AND LOWER(TRIM(ee.entity_name)) IN ({phs}))"
        )
    if cross_ref_status:
        where.append(
            "EXISTS (SELECT 1 FROM entity_cross_refs ecr "
            f"WHERE ecr.source_event_id = e.id AND ecr.status = {a.add(cross_ref_status)})"
        )

    where_sql = " AND ".join(where)

    # Count (uses only the WHERE params accumulated so far).
    total = await ocal_db.fetchval(
        f"SELECT count(*) FROM {_FROM} WHERE {where_sql}", *a.vals
    )

    select_cols = list(_SEARCH_SELECT)
    if q:
        select_cols.append(
            f"ts_rank_cd(e.search_vector, to_tsquery('hebrew', {ts_ph})) AS rank"
        )

    if q and sort == "relevance":
        order = "rank DESC NULLS LAST"
    elif sort == "date_asc":
        order = "e.start_time ASC"
    else:
        order = "e.start_time DESC"

    limit_ph = a.add(limit)
    offset_ph = a.add(offset)
    data_sql = (
        f"SELECT {', '.join(select_cols)} FROM {_FROM} WHERE {where_sql} "
        f"ORDER BY {order} LIMIT {limit_ph} OFFSET {offset_ph}"
    )
    rows = await ocal_db.fetch(data_sql, *a.vals)
    return [dict(r) for r in rows], int(total or 0)


@router.get("/events")
@limiter.limit("60/minute")
async def list_events(
    request: Request,
    q: str | None = Query(None),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
    source_ids: str | None = Query(None, description="comma-separated source UUIDs"),
    location: str | None = Query(None),
    participants: str | None = Query(None),
    entity_names: str | None = Query(None, description="'||'-separated names"),
    cross_ref_status: str | None = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=500),
    sort: str | None = Query(None),
):
    _require_configured()
    _check_date("from_date", from_date)
    _check_date("to_date", to_date)
    if cross_ref_status is not None and cross_ref_status not in ("confirmed", "unconfirmed"):
        raise HTTPException(400, "cross_ref_status must be confirmed|unconfirmed")
    if sort is not None and sort not in ("date_asc", "date_desc", "relevance"):
        raise HTTPException(400, "sort must be date_asc|date_desc|relevance")

    src = [s for s in source_ids.split(",") if s] if source_ids else None
    ents = (
        [n.strip().lower() for n in entity_names.split("||") if n.strip()]
        if entity_names else None
    )
    effective_sort = sort or ("relevance" if q else "date_desc")
    offset = (page - 1) * per_page

    rows, total = await _search_events(
        q=q, from_date=from_date, to_date=to_date, source_ids=src,
        location=location, participants=participants, entity_names=ents,
        cross_ref_status=cross_ref_status, sort=effective_sort,
        offset=offset, limit=per_page,
    )
    return {
        "data": rows,
        "pagination": {
            "page": page,
            "per_page": per_page,
            "total": total,
            "total_pages": (total + per_page - 1) // per_page if per_page else 0,
        },
    }


@router.get("/events/{event_id}")
@limiter.limit("120/minute")
async def get_event(request: Request, event_id: str = Path(...)):
    _require_configured()
    if not _valid_uuid(event_id):
        raise HTTPException(404, "Event not found")
    row = await ocal_db.fetchrow(
        "SELECT e.*, s.name AS source_name, s.color AS source_color, "
        "(s.reviewed_at IS NOT NULL) AS source_reviewed "
        f"FROM {_FROM} WHERE e.id = $1",
        _as_uuid(event_id),
    )
    if not row:
        raise HTTPException(404, "Event not found")
    return dict(row)


@router.get("/events/{event_id}/entities")
@limiter.limit("120/minute")
async def get_event_entities(request: Request, event_id: str = Path(...)):
    _require_configured()
    if not _valid_uuid(event_id):
        raise HTTPException(404, "Event not found")
    rows = await ocal_db.fetch(
        "SELECT entity_type, entity_name, role, confidence, extraction_method "
        "FROM event_entities WHERE event_id = $1 "
        "ORDER BY confidence DESC, entity_type, entity_name",
        _as_uuid(event_id),
    )
    return {"data": [dict(r) for r in rows]}


@router.get("/events/{event_id}/cross-refs")
@limiter.limit("120/minute")
async def get_event_cross_refs(request: Request, event_id: str = Path(...)):
    _require_configured()
    if not _valid_uuid(event_id):
        raise HTTPException(404, "Event not found")
    rows = await ocal_db.fetch(
        """
        SELECT ecr.id, ecr.status, ecr.match_method, ecr.match_score, ecr.event_date,
               ee.entity_name,
               p.name AS target_person_name,
               ds.name AS target_source_name,
               ds.color AS target_source_color,
               me.id AS matched_event_id,
               me.title AS matched_title,
               me.start_time AS matched_start_time,
               me.location AS matched_location
        FROM entity_cross_refs ecr
        JOIN event_entities ee ON ee.id = ecr.event_entity_id
        JOIN people p ON p.id = ecr.target_person_id
        JOIN diary_sources ds ON ds.id = ecr.target_source_id
        LEFT JOIN diary_events me ON me.id = ecr.matched_event_id
        WHERE ecr.source_event_id = $1
        ORDER BY ecr.status, ee.entity_name
        """,
        _as_uuid(event_id),
    )
    refs = [dict(r) for r in rows]
    confirmed = sum(1 for r in refs if r["status"] == "confirmed")
    unconfirmed = sum(1 for r in refs if r["status"] == "unconfirmed")
    return {
        "cross_refs": refs,
        "summary": {"confirmed": confirmed, "unconfirmed": unconfirmed, "total": len(refs)},
    }


@router.get("/events/{event_id}/matches")
@limiter.limit("120/minute")
async def get_event_matches(request: Request, event_id: str = Path(...)):
    _require_configured()
    if not _valid_uuid(event_id):
        return {"match_group": None, "matched_events": []}
    event = await ocal_db.fetchrow(
        "SELECT match_group_id FROM diary_events WHERE id = $1", _as_uuid(event_id)
    )
    if not event or not event["match_group_id"]:
        return {"match_group": None, "matched_events": []}
    group = await ocal_db.fetchrow(
        "SELECT id, event_date, common_title, total_events FROM similar_events WHERE id = $1",
        event["match_group_id"],
    )
    if not group:
        return {"match_group": None, "matched_events": []}
    matched = await ocal_db.fetch(
        """
        SELECT e.id, e.title, e.start_time, e.end_time, e.location, e.participants,
               e.event_date, s.name AS source_name, s.color AS source_color
        FROM diary_events e
        JOIN diary_sources s ON e.source_id = s.id
        WHERE e.match_group_id = $1 AND e.id <> $2
              AND e.is_active = true AND s.is_enabled = true
        ORDER BY s.name
        """,
        group["id"], _as_uuid(event_id),
    )
    return {
        "match_group": {
            "id": group["id"],
            "event_date": group["event_date"],
            "common_title": group["common_title"],
            "total_events": group["total_events"],
        },
        "matched_events": [dict(r) for r in matched],
    }


# ---------------------------------------------------------------------------
# Sources
# ---------------------------------------------------------------------------

_SOURCE_SELECT = (
    "SELECT diary_sources.*, people.name AS person_name, "
    "organizations.name AS organization_name "
    "FROM diary_sources "
    "LEFT JOIN people ON diary_sources.person_id = people.id "
    "LEFT JOIN organizations ON diary_sources.organization_id = organizations.id "
)


@router.get("/sources")
@limiter.limit("60/minute")
async def list_sources(request: Request):
    _require_configured()
    rows = await ocal_db.fetch(
        _SOURCE_SELECT + "WHERE diary_sources.is_enabled = true ORDER BY diary_sources.name"
    )
    return {"data": [dict(r) for r in rows]}


@router.get("/sources/{source_id}")
@limiter.limit("120/minute")
async def get_source(request: Request, source_id: str = Path(...)):
    _require_configured()
    if not _valid_uuid(source_id):
        raise HTTPException(404, "Source not found")
    row = await ocal_db.fetchrow(_SOURCE_SELECT + "WHERE diary_sources.id = $1", _as_uuid(source_id))
    if not row:
        raise HTTPException(404, "Source not found")
    return dict(row)


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

def _iso(y: int, m: int, d: int) -> str:
    from datetime import date
    return date(y, m, d).isoformat()


def _calendar_window(date_str: str, view: str) -> tuple[str, str]:
    from datetime import date, timedelta
    d = date.fromisoformat(date_str)
    if view == "month":
        first = date(d.year, d.month, 1)
        # last day of month
        if d.month == 12:
            last = date(d.year, 12, 31)
        else:
            last = date(d.year, d.month + 1, 1) - timedelta(days=1)
        # JS getDay(): Sunday=0. Python weekday(): Monday=0; Sunday=6.
        start = first - timedelta(days=(first.weekday() + 1) % 7)
        end = last + timedelta(days=(6 - (last.weekday() + 1) % 7))
        return start.isoformat(), end.isoformat()
    if view == "week":
        start = d - timedelta(days=(d.weekday() + 1) % 7)
        return start.isoformat(), (start + timedelta(days=6)).isoformat()
    if view == "4day":
        return d.isoformat(), (d + timedelta(days=3)).isoformat()
    return d.isoformat(), d.isoformat()


async def _events_by_date_range(from_d, to_d, source_ids, entity_names) -> list[dict]:
    a = _Args()
    where = [
        "e.is_active = true", "s.is_enabled = true",
        f"e.event_date >= {a.add(_as_date(from_d))}", f"e.event_date <= {a.add(_as_date(to_d))}",
    ]
    if source_ids:
        phs = ",".join(a.add(_as_uuid(sid)) for sid in source_ids)
        where.append(f"e.source_id IN ({phs})")
    if entity_names:
        norm = [n.strip().lower() for n in entity_names]
        phs = ",".join(a.add(n) for n in norm)
        where.append(
            "EXISTS (SELECT 1 FROM event_entities ee WHERE ee.event_id = e.id "
            f"AND LOWER(TRIM(ee.entity_name)) IN ({phs}))"
        )
    rows = await ocal_db.fetch(
        "SELECT e.*, s.name AS source_name, s.color AS source_color, "
        "(SELECT total_events FROM similar_events WHERE id = e.match_group_id) AS match_count "
        f"FROM {_FROM} WHERE {' AND '.join(where)} ORDER BY e.start_time ASC",
        *a.vals,
    )
    return [dict(r) for r in rows]


async def _counts_by_date_range(from_d, to_d, source_ids, entity_names) -> dict:
    a = _Args()
    where = [
        "e.is_active = true", "s.is_enabled = true",
        f"e.event_date >= {a.add(_as_date(from_d))}", f"e.event_date <= {a.add(_as_date(to_d))}",
    ]
    if source_ids:
        phs = ",".join(a.add(_as_uuid(sid)) for sid in source_ids)
        where.append(f"e.source_id IN ({phs})")
    if entity_names:
        norm = [n.strip().lower() for n in entity_names]
        phs = ",".join(a.add(n) for n in norm)
        where.append(
            "EXISTS (SELECT 1 FROM event_entities ee WHERE ee.event_id = e.id "
            f"AND LOWER(TRIM(ee.entity_name)) IN ({phs}))"
        )
    rows = await ocal_db.fetch(
        f"SELECT e.event_date, COUNT(*) AS count FROM {_FROM} "
        f"WHERE {' AND '.join(where)} GROUP BY e.event_date",
        *a.vals,
    )
    return {r["event_date"].isoformat(): int(r["count"]) for r in rows}


@router.get("/calendar")
@limiter.limit("60/minute")
async def calendar(
    request: Request,
    date: str = Query(...),
    view: str = Query("month"),
    source_ids: str | None = Query(None),
    entity_names: str | None = Query(None, description="comma-separated names"),
    max_date: str | None = Query(None),
):
    _require_configured()
    _check_date("date", date)
    _check_date("max_date", max_date)
    if view not in ("month", "week", "4day", "day"):
        raise HTTPException(400, "view must be month|week|4day|day")

    src = [s for s in source_ids.split(",") if s] if source_ids else None
    ents = [n for n in entity_names.split(",") if n] if entity_names else None

    from_d, to_d = _calendar_window(date, view)
    if max_date and to_d > max_date:
        to_d = max_date

    events = await _events_by_date_range(from_d, to_d, src, ents)
    counts = await _counts_by_date_range(from_d, to_d, src, ents)
    return {"events": events, "date_range": {"from": from_d, "to": to_d}, "event_counts": counts}


# ---------------------------------------------------------------------------
# Stats (cached 5 minutes)
# ---------------------------------------------------------------------------

_stats_cache: dict | None = None
_stats_expires: float = 0.0
_STATS_TTL = 5 * 60


@router.get("/stats")
@limiter.limit("60/minute")
async def stats(request: Request):
    _require_configured()
    global _stats_cache, _stats_expires
    if _stats_cache is not None and time.time() < _stats_expires:
        return _stats_cache
    total_events = await ocal_db.fetchval(
        "SELECT count(*) FROM diary_events WHERE is_active = true"
    )
    total_sources = await ocal_db.fetchval(
        "SELECT count(*) FROM diary_sources WHERE is_enabled = true"
    )
    total_orgs = await ocal_db.fetchval(
        "SELECT count(DISTINCT organization_id) FROM diary_sources "
        "WHERE is_enabled = true AND organization_id IS NOT NULL"
    )
    data = {
        "total_events": int(total_events or 0),
        "total_sources": int(total_sources or 0),
        "total_organizations": int(total_orgs or 0),
    }
    _stats_cache = data
    _stats_expires = time.time() + _STATS_TTL
    return data


# ---------------------------------------------------------------------------
# Entities (materialized-view fast path + in-memory cache)
# ---------------------------------------------------------------------------

_ent_cache: dict[str, tuple[list, float]] = {}
_ENT_TTL = 5 * 60
_ENT_ALL_TTL = 15 * 60


async def _query_entities_matview(type_filter) -> list[dict]:
    a = _Args()
    where = ""
    if type_filter:
        where = f"WHERE entity_type = {a.add(type_filter)}"
    rows = await ocal_db.fetch(
        "SELECT entity_name, entity_type, entity_id, event_count "
        f"FROM mv_entity_counts {where} ORDER BY event_count DESC LIMIT 200",
        *a.vals,
    )
    return [dict(r) for r in rows]


async def _query_entities_live(source_ids, type_filter, from_date, to_date) -> list[dict]:
    a = _Args()
    where = ["ee.confidence >= 0.5"]
    if source_ids:
        phs = ",".join(a.add(_as_uuid(sid)) for sid in source_ids)
        where.append(f"de.source_id IN ({phs})")
    else:
        where.append("de.source_id IN (SELECT id FROM diary_sources WHERE is_enabled = true)")
    if from_date:
        where.append(f"de.event_date >= {a.add(_as_date(from_date))}")
    if to_date:
        where.append(f"de.event_date <= {a.add(_as_date(to_date))}")
    if type_filter:
        where.append(f"ee.entity_type = {a.add(type_filter)}")
    rows = await ocal_db.fetch(
        "SELECT ee.entity_name, ee.entity_type, "
        "MAX(ee.entity_id::text)::uuid AS entity_id, "
        "COUNT(DISTINCT ee.event_id) AS event_count "
        "FROM event_entities ee JOIN diary_events de ON de.id = ee.event_id "
        f"WHERE {' AND '.join(where)} "
        "GROUP BY ee.entity_name, ee.entity_type "
        "ORDER BY event_count DESC LIMIT 200",
        *a.vals,
    )
    return [dict(r) for r in rows]


@router.get("/entities")
@limiter.limit("60/minute")
async def list_entities(
    request: Request,
    source_ids: str | None = Query(None),
    type: str | None = Query(None, description="person|organization|place"),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
):
    _require_configured()
    _check_date("from_date", from_date)
    _check_date("to_date", to_date)
    src = [s for s in source_ids.split(",") if s] if source_ids else None

    is_unfiltered = not src and not from_date and not to_date
    is_all = is_unfiltered and not type
    key = f"{','.join(sorted(src)) if src else 'all'}:{type or ''}:{from_date or ''}:{to_date or ''}"

    cached = _ent_cache.get(key)
    if cached and time.time() < cached[1]:
        return Response(
            content=_json_bytes({"data": cached[0]}),
            media_type="application/json",
            headers={"Cache-Control": "public, max-age=120, stale-while-revalidate=300"},
        )

    if is_unfiltered:
        try:
            entities = await _query_entities_matview(type)
        except Exception:  # noqa: BLE001 — matview may not exist yet
            entities = await _query_entities_live(src, type, from_date, to_date)
    else:
        entities = await _query_entities_live(src, type, from_date, to_date)

    _ent_cache[key] = (entities, time.time() + (_ENT_ALL_TTL if is_all else _ENT_TTL))
    return Response(
        content=_json_bytes({"data": entities}),
        media_type="application/json",
        headers={"Cache-Control": "public, max-age=120, stale-while-revalidate=300"},
    )


def _json_bytes(obj) -> bytes:
    from fastapi.encoders import jsonable_encoder
    import json as _json
    return _json.dumps(jsonable_encoder(obj), ensure_ascii=False).encode("utf-8")


# ---------------------------------------------------------------------------
# Site content (CMS key/values)
# ---------------------------------------------------------------------------

@router.get("/content")
@limiter.limit("60/minute")
async def content(request: Request):
    _require_configured()
    import json as _json
    rows = await ocal_db.fetch("SELECT key, value FROM site_content")
    out: dict = {}
    for r in rows:
        try:
            out[r["key"]] = _json.loads(r["value"])
        except (ValueError, TypeError):
            out[r["key"]] = r["value"]
    return {"content": out}


# ---------------------------------------------------------------------------
# Downloads (CSV / JSON / bulk ZIP)
# ---------------------------------------------------------------------------

_EXPORT_COLS = (
    "e.title, e.event_date, e.start_time, e.end_time, e.location, e.participants, "
    "s.name AS source_name, e.dataset_link"
)
_HEBREW_HEADERS = ["כותרת", "תאריך", "שעת התחלה", "שעת סיום", "מיקום", "משתתפים", "מקור", "קישור לדאטסט"]
_MAX_SOURCES_PER_BULK = 1000


def _time_str(ts) -> str:
    if not ts:
        return ""
    from datetime import datetime, timezone
    if isinstance(ts, datetime):
        return (ts.astimezone(timezone.utc) if ts.tzinfo else ts).strftime("%H:%M")
    m = re.search(r"[T ](\d{2}:\d{2})", str(ts))
    return m.group(1) if m else ""


def _row_values(r: dict) -> list[str]:
    ed = r.get("event_date")
    return [
        r.get("title") or "",
        ed.isoformat() if hasattr(ed, "isoformat") else (ed or ""),
        _time_str(r.get("start_time")),
        _time_str(r.get("end_time")),
        r.get("location") or "",
        r.get("participants") or "",
        r.get("source_name") or "",
        r.get("dataset_link") or "",
    ]


def _build_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(_HEBREW_HEADERS)
    for r in rows:
        w.writerow(_row_values(r))
    # UTF-8 BOM so Excel opens Hebrew correctly.
    return b"\xef\xbb\xbf" + buf.getvalue().encode("utf-8")


def _build_json(rows: list[dict]) -> bytes:
    import json as _json
    payload = []
    for r in rows:
        ed = r.get("event_date")
        st = r.get("start_time")
        et = r.get("end_time")
        payload.append({
            "title": r.get("title"),
            "event_date": ed.isoformat() if hasattr(ed, "isoformat") else ed,
            "start_time": st.isoformat() if hasattr(st, "isoformat") else st,
            "end_time": et.isoformat() if hasattr(et, "isoformat") else et,
            "location": r.get("location"),
            "participants": r.get("participants"),
            "source_name": r.get("source_name"),
            "dataset_link": r.get("dataset_link"),
        })
    return _json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _safe_name(name: str) -> str:
    return re.sub(r"\s+", "_", re.sub(r'[<>:"/\\|?*]', "", name or "").strip())


async def _fetch_rows_for_source(source_id, from_date, to_date) -> list[dict]:
    a = _Args()
    where = [
        f"e.source_id = {a.add(_as_uuid(source_id))}", "e.is_active = true", "s.is_enabled = true",
    ]
    if from_date:
        where.append(f"e.event_date >= {a.add(_as_date(from_date))}")
    if to_date:
        where.append(f"e.event_date <= {a.add(_as_date(to_date))}")
    rows = await ocal_db.fetch(
        f"SELECT {_EXPORT_COLS} FROM {_FROM} WHERE {' AND '.join(where)} ORDER BY e.start_time ASC",
        *a.vals,
    )
    return [dict(r) for r in rows]


def _content_disposition(filename: str) -> str:
    from urllib.parse import quote
    return f"attachment; filename*=UTF-8''{quote(filename)}"


@router.get("/download/source/{source_id}")
@limiter.limit("20/minute")
async def download_source(
    request: Request,
    source_id: str = Path(...),
    format: str = Query("csv"),
    from_date: str | None = Query(None),
    to_date: str | None = Query(None),
):
    _require_configured()
    if format not in ("csv", "json"):
        raise HTTPException(400, "format must be csv|json")
    if not _valid_uuid(source_id):
        raise HTTPException(400, "Invalid source ID")
    _check_date("from_date", from_date)
    _check_date("to_date", to_date)

    source = await ocal_db.fetchrow(
        "SELECT name FROM diary_sources WHERE id = $1 AND is_enabled = true", _as_uuid(source_id)
    )
    if not source:
        raise HTTPException(404, "Source not found")

    rows = await _fetch_rows_for_source(source_id, from_date, to_date)
    filename = _safe_name(source["name"]) or "diary"
    if format == "json":
        return Response(
            content=_build_json(rows),
            media_type="application/json; charset=utf-8",
            headers={"Content-Disposition": _content_disposition(f"{filename}.json")},
        )
    return Response(
        content=_build_csv(rows),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": _content_disposition(f"{filename}.csv")},
    )


class BulkDownloadBody(BaseModel):
    source_ids: list[str] = Field(..., min_length=1, max_length=_MAX_SOURCES_PER_BULK)
    format: str = "csv"
    from_date: str | None = None
    to_date: str | None = None


@router.post("/download/bulk")
@limiter.limit("10/minute")
async def download_bulk(request: Request, body: BulkDownloadBody):
    _require_configured()
    if body.format not in ("csv", "json"):
        raise HTTPException(400, "format must be csv|json")
    _check_date("from_date", body.from_date)
    _check_date("to_date", body.to_date)
    ids = [s for s in body.source_ids if _valid_uuid(s)]
    if not ids:
        raise HTTPException(400, "No valid source IDs")

    valid = await ocal_db.fetch(
        "SELECT id, name FROM diary_sources WHERE id = ANY($1::uuid[]) AND is_enabled = true "
        "ORDER BY name",
        ids,
    )
    if not valid:
        raise HTTPException(404, "None of the requested sources exist or are enabled")

    ext = "json" if body.format == "json" else "csv"
    zip_buf = io.BytesIO()
    used: set[str] = set()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        for src in valid:
            rows = await _fetch_rows_for_source(str(src["id"]), body.from_date, body.to_date)
            base = _safe_name(src["name"]) or "diary"
            name = f"{base}.{ext}"
            n = 2
            while name in used:
                name = f"{base}_{n}.{ext}"
                n += 1
            used.add(name)
            data = _build_json(rows) if body.format == "json" else _build_csv(rows)
            zf.writestr(name, data)

    if len(valid) == 1:
        zip_name = f"{_safe_name(valid[0]['name']) or 'diary'}.zip"
    else:
        zip_name = f"ocal-{len(valid)}-diaries.zip"
    return Response(
        content=zip_buf.getvalue(),
        media_type="application/zip",
        headers={"Content-Disposition": _content_disposition(zip_name)},
    )


# TEMPORARY diagnostic — remove after fixing the ocal fdw setup. Returns the
# fdw setup outcome + parsed target (NO password) so we can see why Neon fdw
# fails without server-log access.
@router.get("/_fdw_debug")
async def _fdw_debug(request: Request):
    from app.services import ocal_fdw
    tgt = ocal_fdw._target() or {}
    res = await ocal_fdw.ensure_fdw()
    return {
        "configured": ocal_fdw.is_configured(),
        "target": {"host": tgt.get("host"), "port": tgt.get("port"),
                   "dbname": tgt.get("dbname"), "user": tgt.get("user"),
                   "has_password": bool(tgt.get("password"))},
        "result": res,
    }
