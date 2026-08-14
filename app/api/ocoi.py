"""Public read API for "ניגוד עניינים לעם" (OCOI), migrated into OVER.

Ports OCOI's ``/api/v1/*`` public routes to ``/api/ocoi/*`` on OVER, querying the
migrated corpus through app/services/ocoi_db.py (schema ``ocoi`` in the append
DB). Unlike the ocal port this is a re-homing rather than a translation — OCOI
already ran FastAPI + SQLAlchemy — so the query semantics below are deliberately
faithful to the originals in ``ocoi_db/graph.py`` and ``ocoi_api/routers/*``.

Endpoints (all public, rate-limited):
    GET  /api/ocoi/search                    entity search across 4 types
    GET  /api/ocoi/search/suggest            typeahead (max 10)
    GET  /api/ocoi/persons|companies|associations|domains        list
    GET  /api/ocoi/{type}/{id}                                    detail
    GET  /api/ocoi/{type}/{id}/documents                          source docs
    GET  /api/ocoi/entities/top-connected    ranked by edge count
    GET  /api/ocoi/entities/ministries       per-ministry aggregation
    GET  /api/ocoi/lookup                    cross-type lookup
    GET  /api/ocoi/registry/lookup           gov registry mirror
    GET  /api/ocoi/graph/neighbors/{id}      N-hop neighbourhood
    GET  /api/ocoi/graph/path                path between two entities
    GET  /api/ocoi/graph/showcase            home-page "two suns" pair
    GET  /api/ocoi/graph/subgraph            radius subgraph
    GET  /api/ocoi/documents                 document list
    GET  /api/ocoi/documents/{id}[/markdown|/entities|/graph|/file]
    GET  /api/ocoi/external/by-company|by-person|by-ministry|stats
    GET  /api/ocoi/site/content/{key}        CMS key/value
    POST /api/ocoi/suggestions               anonymous correction submission

Three invariants carried over from OCOI, each load-bearing:

* **The ``{status, data, meta}`` envelope is preserved verbatim.** OCOI publishes
  a documented public API and a Chrome extension consumes it; changing the shape
  would break both for no benefit.
* **``hidden`` entities are pruned along with every edge that touches them.**
  Placeholder rows ("עניינים אישיים" and similar) are flagged rather than
  deleted, so filtering only the nodes would leave dangling edges in the graph.
* **IDs are CHAR(36) strings, not native uuid** (see ocoi_db). They bind as
  ``str``; we validate the format to reject junk early but never cast.

Deliberately NOT ported: ``/api/db-health`` (it returns 3000 chars of traceback
in a 500) and the SPA catch-all ``GET /{path:path}`` (OVER owns its own routing).
Admin lives in a later phase.
"""
from __future__ import annotations

import re
import time
import uuid as _uuid

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from app.rate_limit import limiter
from app.services import ocoi_db

router = APIRouter(prefix="/api/ocoi", tags=["ocoi"])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_configured() -> None:
    if not ocoi_db.is_configured():
        raise HTTPException(
            status_code=503,
            detail="ניגוד עניינים לעם אינו זמין כרגע (OCOI_DATABASE_URL not configured).",
        )


def _ok(data, meta: dict | None = None) -> dict:
    """OCOI's response envelope. Kept byte-compatible — see module docstring."""
    out: dict = {"status": "ok", "data": data}
    if meta is not None:
        out["meta"] = meta
    return out


def _page_meta(total: int, page: int, limit: int) -> dict:
    return {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": (total + limit - 1) // limit if limit else 0,
    }


def _valid_id(s: str) -> bool:
    """OCOI ids are uuid4 rendered as CHAR(36). Validate the SHAPE, bind the STR.

    Casting to uuid.UUID would be wrong here: the column is CHAR(36), so asyncpg
    must send text or the comparison never matches.
    """
    try:
        _uuid.UUID(str(s))
        return True
    except (ValueError, AttributeError, TypeError):
        return False


def _require_id(s: str, name: str = "id") -> str:
    if not _valid_id(s):
        raise HTTPException(status_code=400, detail=f"{name} must be a UUID")
    return str(s)


# The four entity kinds and their tables. Every place that interpolates a table
# name resolves it through THIS dict and never from user input — the same
# discipline OCOI used, made explicit because `top_connected` there built SQL
# with an f-string.
_ENTITY_TABLES = {
    "person": "persons",
    "company": "companies",
    "association": "associations",
    "domain": "domains",
}
_ENTITY_TYPES = tuple(_ENTITY_TABLES)


def _require_entity_type(t: str) -> str:
    if t not in _ENTITY_TABLES:
        raise HTTPException(
            status_code=400,
            detail=f"type must be one of: {', '.join(_ENTITY_TYPES)}",
        )
    return t


# Only these origins may be excluded. A free-form list would let a caller shape
# the SQL predicate; OCOI whitelisted the same two values.
_KNOWN_ORIGINS = ("coi_declaration", "mk_expense")


def _parse_origins(raw: str | None) -> list[str]:
    """CSV of origin_kind values to EXCLUDE from graph/aggregate queries.

    The public site passes ``mk_expense`` so Knesset expense edges don't drown
    the conflict-of-interest graph. Unknown values are dropped rather than
    rejected: this is a display filter on a public URL, and a stale bookmark
    should degrade to "no filter", not to a 400.
    """
    if not raw:
        return []
    return [p for p in (x.strip() for x in raw.split(",")) if p in _KNOWN_ORIGINS]


def _origin_clause(origins: list[str], start_idx: int = 1,
                   alias: str = "r") -> tuple[str, list]:
    """``AND r.origin_kind NOT IN ($n, $n+1, …)`` plus the params to bind.

    Must be applied to BOTH the anchor and the recursive step of a walk —
    otherwise an excluded edge still works as a hop-springboard and the filter
    leaks paths it was meant to remove. OCOI documents this explicitly.
    """
    if not origins:
        return "", []
    ph = ", ".join(f"${start_idx + i}" for i in range(len(origins)))
    return f" AND {alias}.origin_kind NOT IN ({ph})", list(origins)


def _entity_row(rec, entity_type: str) -> dict:
    """Shared entity serialisation. ``aliases`` is TEXT-holding-JSON (see ocoi_db)."""
    d = dict(rec)
    d["entity_type"] = entity_type
    d["aliases"] = ocoi_db.decode_aliases(d.get("aliases"))
    return d


# ---------------------------------------------------------------------------
# Hidden-entity pruning
# ---------------------------------------------------------------------------

# `hidden` is a curation flag an admin toggles by hand, not a bulk state, so the
# id set is tiny and changes rarely — but it is needed by EVERY graph and
# document read. Re-reading four tables per request cost measurable latency
# against the live corpus, so it is cached briefly. The TTL is the staleness an
# admin sees after hiding an entity; a minute is well inside "it took effect".
_hidden_cache: tuple[float, dict[str, set[str]]] | None = None
_HIDDEN_TTL = 60.0


async def _hidden_ids() -> dict[str, set[str]]:
    """Ids flagged ``hidden`` per entity type (cached ~60s).

    Applied in Python rather than joined per query: the graph walk returns
    type-tagged endpoints with no FK, so there is nothing to join against
    without four LEFT JOINs per row.
    """
    global _hidden_cache
    now = time.time()
    if _hidden_cache is not None and now - _hidden_cache[0] < _HIDDEN_TTL:
        return _hidden_cache[1]
    out: dict[str, set[str]] = {}
    for etype, table in _ENTITY_TABLES.items():
        rows = await ocoi_db.fetch(f"SELECT id FROM {table} WHERE hidden IS TRUE")
        out[etype] = {r["id"] for r in rows}
    _hidden_cache = (now, out)
    return out


def _invalidate_hidden_cache() -> None:
    """Called by the admin surface after toggling `hidden` (later phase)."""
    global _hidden_cache
    _hidden_cache = None


def _prune_hidden(edges: list[dict], hidden: dict[str, set[str]]) -> list[dict]:
    """Drop every edge with a hidden endpoint on EITHER side."""
    keep = []
    for e in edges:
        if e["source_entity_id"] in hidden.get(e["source_entity_type"], ()):
            continue
        if e["target_entity_id"] in hidden.get(e["target_entity_type"], ()):
            continue
        keep.append(e)
    return keep


# ---------------------------------------------------------------------------
# Graph
# ---------------------------------------------------------------------------

# Canonical projection for every walk. `_subgraph_from_rows` reads these by NAME
# (OCOI read them positionally, which made the column order load-bearing).
_EDGE_COLS = """
    r.source_entity_type, r.source_entity_id,
    r.target_entity_type, r.target_entity_id,
    r.relationship_type, r.details, r.origin_kind, r.verified,
    r.document_id, d.title AS doc_title, d.file_url AS doc_url
"""

# A hard ceiling on how many edges one walk may materialise.
#
# OCOI's recursive CTE has NO visited-set — it relies on the depth counter and
# UNION's dedup alone — so a hub with thousands of edges expands combinatorially
# and can pin the process. That was survivable on a dyno serving one project;
# here it would take the whole site down. The cap bounds the blast radius, and
# `truncated` tells the caller the view is partial rather than silently lying.
_MAX_EDGES = 4000

# Ceiling for exact row counting on the big mirrored table — see registry_lookup.
_COUNT_CAP = 10_000


async def _walk(anchor_type: str, anchor_id: str, depth: int,
                origins: list[str]) -> list[dict]:
    """Undirected N-hop walk from one entity. Returns edge rows."""
    if depth <= 1:
        oc, oparams = _origin_clause(origins, 3)
        sql = f"""
            SELECT {_EDGE_COLS}
            FROM entity_relationships r
            LEFT JOIN documents d ON d.id = r.document_id
            WHERE ((r.source_entity_type = $1 AND r.source_entity_id = $2)
                OR (r.target_entity_type = $1 AND r.target_entity_id = $2))
              {oc}
            LIMIT {_MAX_EDGES}
        """
        rows = await ocoi_db.fetch(sql, anchor_type, anchor_id, *oparams)
        return [dict(r) for r in rows]

    # Recursive: the join condition is undirected on purpose — a declaration
    # links a person to a company in one direction, but the graph a reader wants
    # is the connected component, not the arrows.
    oc_a, p_a = _origin_clause(origins, 4)
    oc_r, p_r = _origin_clause(origins, 4 + len(p_a))
    sql = f"""
        WITH RECURSIVE walk AS (
            SELECT r.source_entity_type, r.source_entity_id,
                   r.target_entity_type, r.target_entity_id,
                   r.relationship_type, r.details, r.origin_kind, r.verified,
                   r.document_id, 1 AS depth
            FROM entity_relationships r
            WHERE ((r.source_entity_type = $1 AND r.source_entity_id = $2)
                OR (r.target_entity_type = $1 AND r.target_entity_id = $2))
              {oc_a}
            UNION
            SELECT r.source_entity_type, r.source_entity_id,
                   r.target_entity_type, r.target_entity_id,
                   r.relationship_type, r.details, r.origin_kind, r.verified,
                   r.document_id, w.depth + 1
            FROM entity_relationships r
            JOIN walk w ON (
                    (r.source_entity_type = w.target_entity_type
                     AND r.source_entity_id = w.target_entity_id)
                 OR (r.target_entity_type = w.source_entity_type
                     AND r.target_entity_id = w.source_entity_id)
            )
            WHERE w.depth < $3
              {oc_r}
        )
        SELECT w.source_entity_type, w.source_entity_id,
               w.target_entity_type, w.target_entity_id,
               w.relationship_type, w.details, w.origin_kind, w.verified,
               w.document_id, d.title AS doc_title, d.file_url AS doc_url
        FROM walk w
        LEFT JOIN documents d ON d.id = w.document_id
        LIMIT {_MAX_EDGES}
    """
    rows = await ocoi_db.fetch(sql, anchor_type, anchor_id, depth, *p_a, *p_r)
    return [dict(r) for r in rows]


async def _hydrate_names(edges: list[dict]) -> dict[tuple[str, str], dict]:
    """Resolve (type, id) → {name, …} for every endpoint in the edge set.

    The walk deliberately does not join entity names (four polymorphic LEFT
    JOINs per row); OCOI hydrated afterwards and so do we — one query per type
    that actually appears.
    """
    wanted: dict[str, set[str]] = {t: set() for t in _ENTITY_TYPES}
    for e in edges:
        if e["source_entity_type"] in wanted:
            wanted[e["source_entity_type"]].add(e["source_entity_id"])
        if e["target_entity_type"] in wanted:
            wanted[e["target_entity_type"]].add(e["target_entity_id"])

    out: dict[tuple[str, str], dict] = {}
    for etype, ids in wanted.items():
        if not ids:
            continue
        table = _ENTITY_TABLES[etype]
        extra = ", registration_number" if etype in ("company", "association") else ""
        if etype == "person":
            extra = ", position, ministry"
        rows = await ocoi_db.fetch(
            f"SELECT id, name_hebrew{extra} FROM {table} WHERE id = ANY($1::text[])",
            list(ids),
        )
        for r in rows:
            d = dict(r)
            out[(etype, d["id"])] = {
                "id": d["id"],
                "entity_type": etype,
                "name": d.get("name_hebrew") or "",
                **{k: v for k, v in d.items() if k not in ("id", "name_hebrew")},
            }
    return out


def _subgraph(edges: list[dict], names: dict[tuple[str, str], dict],
              truncated: bool = False) -> dict:
    """Assemble {nodes, edges} — the shape the frontend's ConnectionMap expects."""
    nodes: dict[tuple[str, str], dict] = {}
    out_edges = []
    for e in edges:
        sk = (e["source_entity_type"], e["source_entity_id"])
        tk = (e["target_entity_type"], e["target_entity_id"])
        for k in (sk, tk):
            if k not in nodes:
                nodes[k] = names.get(k, {
                    "id": k[1], "entity_type": k[0], "name": "",
                })
        out_edges.append({
            "source_id": e["source_entity_id"],
            "source_type": e["source_entity_type"],
            "target_id": e["target_entity_id"],
            "target_type": e["target_entity_type"],
            "relationship_type": e["relationship_type"],
            "details": e.get("details"),
            "origin_kind": e.get("origin_kind"),
            "verified": e.get("verified"),
            "document_id": e.get("document_id"),
            "document_title": e.get("doc_title"),
            "document_url": e.get("doc_url"),
        })
    return {
        "nodes": list(nodes.values()),
        "edges": out_edges,
        "truncated": truncated,
    }


async def _subgraph_for(anchor_type: str, anchor_id: str, depth: int,
                        origins: list[str]) -> dict:
    edges = await _walk(anchor_type, anchor_id, depth, origins)
    truncated = len(edges) >= _MAX_EDGES
    edges = _prune_hidden(edges, await _hidden_ids())
    return _subgraph(edges, await _hydrate_names(edges), truncated)


@router.get("/graph/neighbors/{entity_id}")
@limiter.limit("60/minute")
async def graph_neighbors(
    request: Request,
    entity_id: str,
    type: str = Query(..., description="person|company|association|domain"),
    depth: int = Query(1, ge=1, le=3),
    exclude_origins: str | None = None,
):
    _require_configured()
    _require_id(entity_id, "entity_id")
    _require_entity_type(type)
    return _ok(await _subgraph_for(type, entity_id, depth, _parse_origins(exclude_origins)))


@router.get("/graph/subgraph")
@limiter.limit("60/minute")
async def graph_subgraph(
    request: Request,
    center: str = Query(...),
    type: str = Query(...),
    radius: int = Query(1, ge=1, le=3),
    limit: int = Query(200, ge=1, le=500),
    exclude_origins: str | None = None,
):
    _require_configured()
    _require_id(center, "center")
    _require_entity_type(type)
    sub = await _subgraph_for(type, center, radius, _parse_origins(exclude_origins))
    if len(sub["nodes"]) > limit:
        keep = {n["id"] for n in sub["nodes"][:limit]}
        sub["nodes"] = [n for n in sub["nodes"] if n["id"] in keep]
        sub["edges"] = [
            e for e in sub["edges"]
            if e["source_id"] in keep and e["target_id"] in keep
        ]
        sub["truncated"] = True
    return _ok(sub)


@router.get("/graph/path")
@limiter.limit("60/minute")
async def graph_path(
    request: Request,
    from_id: str = Query(...),
    from_type: str = Query(...),
    to_id: str = Query(...),
    to_type: str = Query(...),
    max_hops: int = Query(4, ge=1, le=6),
    exclude_origins: str | None = None,
):
    """Subgraph around `from_` that CONTAINS `to_`, or 404.

    Faithful to OCOI: it returns the reachable neighbourhood filtered to walks
    touching the target, not an accumulated shortest path — the original never
    built a path array either, and the frontend renders a subgraph.
    """
    _require_configured()
    _require_id(from_id, "from_id")
    _require_id(to_id, "to_id")
    _require_entity_type(from_type)
    _require_entity_type(to_type)

    edges = await _walk(from_type, from_id, max_hops, _parse_origins(exclude_origins))
    edges = _prune_hidden(edges, await _hidden_ids())
    touching = [
        e for e in edges
        if (e["target_entity_type"] == to_type and e["target_entity_id"] == to_id)
        or (e["source_entity_type"] == to_type and e["source_entity_id"] == to_id)
    ]
    if not touching:
        raise HTTPException(status_code=404, detail="No path found")
    return _ok(_subgraph(touching, await _hydrate_names(touching)))


# The showcase pair rotates daily and is expensive (hub ranking over the whole
# edge table), so it is cached per (israel-date, origin-filter) exactly as OCOI
# did. Process-local: OVER runs a single web process.
_showcase_cache: dict[tuple, tuple[float, dict]] = {}
_SHOWCASE_TTL = 3600.0


@router.get("/graph/showcase")
@limiter.limit("60/minute")
async def graph_showcase(request: Request, exclude_origins: str | None = None):
    """The home page's "two suns": two well-connected people and their orbits."""
    _require_configured()
    origins = _parse_origins(exclude_origins)
    key = (tuple(sorted(origins)),)
    hit = _showcase_cache.get(key)
    now = time.time()
    if hit and now - hit[0] < _SHOWCASE_TTL:
        return _ok(hit[1], {"cached": True})

    oc, oparams = _origin_clause(origins, 1)
    hubs = await ocoi_db.fetch(f"""
        SELECT r.source_entity_id AS person_id, COUNT(*) AS deg
        FROM entity_relationships r
        WHERE r.source_entity_type = 'person'
          AND r.target_entity_type IN ('company','association','domain')
          {oc}
        GROUP BY r.source_entity_id
        HAVING COUNT(*) >= 2
        ORDER BY deg DESC, r.source_entity_id
        LIMIT 40
    """, *oparams)

    hidden = await _hidden_ids()
    picks = [r["person_id"] for r in hubs
             if r["person_id"] not in hidden.get("person", ())][:2]
    if not picks:
        return _ok(None)

    merged: list[dict] = []
    for pid in picks:
        merged.extend(await _walk("person", pid, 1, origins))
    merged = _prune_hidden(merged, hidden)
    data = _subgraph(merged, await _hydrate_names(merged))
    data["anchors"] = picks
    _showcase_cache[key] = (now, data)
    return _ok(data, {"cached": False})


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------

async def _search_entities(q: str, etype: str | None, page: int, limit: int):
    """LIKE-based search across the four entity tables.

    OCOI has no full-text index — search is `ILIKE '%…%'` UNION ALL across the
    four tables, and the corpus is small enough (~13k entities) that this is
    fine. Kept as-is deliberately: the Hebrew FTS config on this cluster returns
    empty tsvectors (measured during the ocal port), so "upgrading" to
    to_tsvector here would silently return nothing.
    """
    types = [etype] if etype else list(_ENTITY_TYPES)
    pattern = f"%{q}%"

    parts, params = [], [pattern]
    for t in types:
        table = _ENTITY_TABLES[t]
        parts.append(f"""
            SELECT id, name_hebrew AS name, '{t}' AS entity_type
            FROM {table}
            WHERE hidden IS NOT TRUE AND name_hebrew ILIKE $1
        """)
    union = " UNION ALL ".join(parts)

    total = await ocoi_db.fetchval(
        f"SELECT COUNT(*) FROM ({union}) u", *params) or 0
    rows = await ocoi_db.fetch(
        f"SELECT * FROM ({union}) u ORDER BY length(u.name), u.name "
        f"LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
        *params, limit, (page - 1) * limit,
    )
    return [dict(r) for r in rows], int(total)


@router.get("/search")
@limiter.limit("60/minute")
async def search(
    request: Request,
    q: str = Query(..., min_length=1, max_length=200),
    type: str | None = Query(None),
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    _require_configured()
    if type is not None:
        _require_entity_type(type)
    rows, total = await _search_entities(q.strip(), type, page, limit)
    return _ok(rows, _page_meta(total, page, limit))


@router.get("/search/suggest")
@limiter.limit("120/minute")
async def search_suggest(request: Request, q: str = Query(..., min_length=1, max_length=200)):
    _require_configured()
    rows, _ = await _search_entities(q.strip(), None, 1, 10)
    return _ok([{"text": r["name"], "type": r["entity_type"], "id": r["id"]} for r in rows])


# ---------------------------------------------------------------------------
# Entity lists + detail
# ---------------------------------------------------------------------------

_ENTITY_COLUMNS = {
    "person": "id, name_hebrew, name_english, title, position, ministry, aliases, created_at",
    "company": ("id, name_hebrew, name_english, registration_number, company_type, "
                "status, match_confidence, aliases, created_at"),
    "association": ("id, name_hebrew, name_english, registration_number, status, "
                    "match_confidence, aliases, created_at"),
    "domain": "id, name_hebrew, name_english, description, aliases, created_at",
}


async def _entity_list(etype: str, page: int, limit: int, q: str | None):
    table = _ENTITY_TABLES[etype]
    cols = _ENTITY_COLUMNS[etype]
    where, params = "WHERE hidden IS NOT TRUE", []
    if q:
        params.append(f"%{q}%")
        where += f" AND name_hebrew ILIKE ${len(params)}"
    total = await ocoi_db.fetchval(f"SELECT COUNT(*) FROM {table} {where}", *params) or 0
    rows = await ocoi_db.fetch(
        f"SELECT {cols} FROM {table} {where} ORDER BY name_hebrew "
        f"LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
        *params, limit, (page - 1) * limit,
    )
    return [_entity_row(r, etype) for r in rows], int(total)


async def _entity_detail(etype: str, eid: str):
    table = _ENTITY_TABLES[etype]
    row = await ocoi_db.fetchrow(
        f"SELECT {_ENTITY_COLUMNS[etype]} FROM {table} "
        f"WHERE id = $1 AND hidden IS NOT TRUE", eid)
    if row is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return _entity_row(row, etype)


async def _entity_documents(etype: str, eid: str):
    """Distinct documents in which this entity appears (either edge side)."""
    rows = await ocoi_db.fetch("""
        SELECT DISTINCT d.id, d.title, d.file_url, d.file_format,
               d.conversion_status, d.verified, d.created_at
        FROM entity_relationships r
        JOIN documents d ON d.id = r.document_id
        WHERE (r.source_entity_type = $1 AND r.source_entity_id = $2)
           OR (r.target_entity_type = $1 AND r.target_entity_id = $2)
        ORDER BY d.created_at DESC NULLS LAST
    """, etype, eid)
    return [dict(r) for r in rows]


def _register_entity_routes(path: str, etype: str) -> None:
    """Register list/detail/documents for one entity kind.

    Generated rather than written four times: the three handlers are identical
    apart from the table, and OCOI's copies had already drifted (only `domains`
    was unpaginated). One definition keeps them honest.

    ``etype`` is closed over, NOT passed as a defaulted parameter: FastAPI reads
    the signature to build the request model, so `_etype: str = etype` would
    publish it as a client-settable query parameter — letting a caller point a
    /persons URL at another table. A closure is invisible to the signature.
    """

    @router.get(f"/{path}", name=f"ocoi_list_{path}")
    @limiter.limit("60/minute")
    async def _list(request: Request, page: int = Query(1, ge=1),
                    limit: int = Query(20, ge=1, le=100), q: str | None = None):
        _require_configured()
        rows, total = await _entity_list(etype, page, limit, q)
        return _ok(rows, _page_meta(total, page, limit))

    @router.get(f"/{path}/{{entity_id}}", name=f"ocoi_get_{path}")
    @limiter.limit("60/minute")
    async def _detail(request: Request, entity_id: str):
        _require_configured()
        return _ok(await _entity_detail(etype, _require_id(entity_id)))

    @router.get(f"/{path}/{{entity_id}}/documents", name=f"ocoi_docs_{path}")
    @limiter.limit("60/minute")
    async def _docs(request: Request, entity_id: str):
        _require_configured()
        return _ok(await _entity_documents(etype, _require_id(entity_id)))


for _p, _t in (("persons", "person"), ("companies", "company"),
               ("associations", "association"), ("domains", "domain")):
    _register_entity_routes(_p, _t)


@router.get("/entities/top-connected")
@limiter.limit("60/minute")
async def top_connected(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    type: str | None = Query(None),
    exclude_origins: str | None = None,
):
    """Entities ranked by edge count, both sides counted."""
    _require_configured()
    if type is not None:
        _require_entity_type(type)
    origins = _parse_origins(exclude_origins)

    # One param list, bound once. The origin placeholders are REUSED by both
    # UNION branches (Postgres allows the same $n twice), so they must not be
    # appended a second time — doing so shifts every later placeholder.
    oc, params = _origin_clause(origins, 1)
    type_clause = ""
    if type:
        params.append(type)
        type_clause = f" AND etype = ${len(params)}"

    # `type` is a bound parameter here, not interpolated — OCOI built this
    # predicate with an f-string (whitelisted, but the pattern invites drift).
    #
    # Over-fetch (limit*3) because `hidden` rows are pruned in Python afterwards
    # and would otherwise leave the page short.
    rows = await ocoi_db.fetch(f"""
        WITH deg AS (
            SELECT r.source_entity_type AS etype, r.source_entity_id AS eid
            FROM entity_relationships r WHERE TRUE {oc}
            UNION ALL
            SELECT r.target_entity_type, r.target_entity_id
            FROM entity_relationships r WHERE TRUE {oc}
        )
        SELECT etype, eid, COUNT(*) AS connections
        FROM deg
        WHERE TRUE {type_clause}
        GROUP BY etype, eid
        ORDER BY connections DESC, eid
        LIMIT ${len(params)+1} OFFSET ${len(params)+2}
    """, *params, limit * 3, (page - 1) * limit)

    hidden = await _hidden_ids()
    visible = [r for r in rows
               if r["eid"] not in hidden.get(r["etype"], ())][:limit]
    names = await _hydrate_names([
        {"source_entity_type": r["etype"], "source_entity_id": r["eid"],
         "target_entity_type": r["etype"], "target_entity_id": r["eid"]}
        for r in visible
    ])
    out = []
    for r in visible:
        base = names.get((r["etype"], r["eid"]), {"id": r["eid"], "name": ""})
        out.append({**base, "entity_type": r["etype"],
                    "connections": int(r["connections"])})
    return _ok(out)


@router.get("/entities/ministries")
@limiter.limit("60/minute")
async def ministries(request: Request, exclude_origins: str | None = None):
    """Per-ministry aggregation: how many officials and how many connections."""
    _require_configured()
    oc, oparams = _origin_clause(_parse_origins(exclude_origins))
    rows = await ocoi_db.fetch(f"""
        SELECT p.ministry,
               COUNT(DISTINCT p.id) AS person_count,
               COUNT(r.id) AS connection_count
        FROM persons p
        LEFT JOIN entity_relationships r
               ON r.source_entity_type = 'person' AND r.source_entity_id = p.id
               {oc}
        WHERE p.hidden IS NOT TRUE
          AND p.ministry IS NOT NULL AND btrim(p.ministry) <> ''
        GROUP BY p.ministry
        ORDER BY person_count DESC, p.ministry
    """, *oparams)
    return _ok([dict(r) for r in rows])


@router.get("/lookup")
@limiter.limit("60/minute")
async def lookup(
    request: Request,
    q: str | None = None,
    registration_number: str | None = None,
    entity_type: str | None = None,
    limit: int = Query(20, ge=1, le=100),
):
    """Cross-type lookup by name or registration number."""
    _require_configured()
    if not q and not registration_number:
        raise HTTPException(status_code=400, detail="q or registration_number is required")
    if entity_type is not None:
        _require_entity_type(entity_type)

    types = [entity_type] if entity_type else list(_ENTITY_TYPES)
    out: list[dict] = []
    for t in types:
        table = _ENTITY_TABLES[t]
        has_reg = t in ("company", "association")
        if registration_number and not has_reg:
            continue
        where, params = ["hidden IS NOT TRUE"], []
        if registration_number:
            params.append(registration_number)
            where.append(f"registration_number = ${len(params)}")
        if q:
            params.append(f"%{q}%")
            where.append(f"name_hebrew ILIKE ${len(params)}")
        rows = await ocoi_db.fetch(
            f"SELECT {_ENTITY_COLUMNS[t]} FROM {table} "
            f"WHERE {' AND '.join(where)} ORDER BY name_hebrew LIMIT ${len(params)+1}",
            *params, limit,
        )
        out.extend(_entity_row(r, t) for r in rows)
    return _ok(out[:limit])


@router.get("/registry/lookup")
@limiter.limit("60/minute")
async def registry_lookup(
    request: Request,
    q: str | None = None,
    registration_number: str | None = None,
    source_type: str | None = None,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    """Search the mirrored government registry (registry_records)."""
    _require_configured()
    where, params = ["TRUE"], []
    if registration_number:
        params.append(registration_number)
        where.append(f"registration_number = ${len(params)}")
    if q:
        params.append(f"%{q}%")
        where.append(f"name ILIKE ${len(params)}")
    if source_type:
        params.append(source_type)
        where.append(f"source_type = ${len(params)}")
    w = " AND ".join(where)

    # BOUNDED count. registry_records holds ~798k rows and `name` has no trigram
    # index, so a substring search is a sequential scan: an exact COUNT(*) for a
    # common fragment matched 711,689 rows and took 39 SECONDS against the live
    # corpus. Nobody pages to result 700,000 — stop counting at the cap and say
    # so, which turns a 39s request into a bounded one.
    #
    # The proper fix is a pg_trgm GIN index on the migrated table; this stays
    # regardless, because an unbounded count over a growing mirror is a latency
    # bomb waiting for the next big sync.
    total = await ocoi_db.fetchval(
        f"SELECT COUNT(*) FROM ("
        f"  SELECT 1 FROM registry_records WHERE {w} LIMIT {_COUNT_CAP + 1}"
        f") t", *params) or 0
    capped = int(total) > _COUNT_CAP
    rows = await ocoi_db.fetch(
        f"SELECT id, source_type, name, registration_number, status, updated_at "
        f"FROM registry_records WHERE {w} ORDER BY name "
        f"LIMIT ${len(params)+1} OFFSET ${len(params)+2}",
        *params, limit, (page - 1) * limit,
    )
    meta = _page_meta(min(int(total), _COUNT_CAP), page, limit)
    if capped:
        # "at least this many" — the UI must not print it as an exact figure.
        meta["total_capped"] = True
    return _ok([dict(r) for r in rows], meta)


# ---------------------------------------------------------------------------
# Documents
# ---------------------------------------------------------------------------

@router.get("/documents")
@limiter.limit("60/minute")
async def documents(
    request: Request,
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    status: str | None = None,
    q: str | None = None,
    source_type: str | None = None,
    verified: str | None = None,
):
    _require_configured()
    where, params = ["TRUE"], []
    if status:
        params.append(status)
        where.append(f"d.conversion_status = ${len(params)}")
    if q:
        params.append(f"%{q}%")
        where.append(f"d.title ILIKE ${len(params)}")
    if source_type:
        params.append(source_type)
        where.append(f"s.source_type = ${len(params)}")
    if verified is not None:
        v = str(verified).lower() in ("true", "1", "yes")
        params.append(v)
        where.append(f"d.verified = ${len(params)}")
    w = " AND ".join(where)

    total = await ocoi_db.fetchval(
        f"SELECT COUNT(*) FROM documents d "
        f"LEFT JOIN sources s ON s.id = d.source_id WHERE {w}", *params) or 0
    rows = await ocoi_db.fetch(f"""
        SELECT d.id, d.title, d.file_url, d.file_format, d.file_size,
               d.conversion_status, d.extraction_status, d.verified,
               d.created_at, s.title AS source_title, s.source_type,
               (SELECT COUNT(*) FROM entity_relationships r
                 WHERE r.document_id = d.id) AS relationships_count
        FROM documents d
        LEFT JOIN sources s ON s.id = d.source_id
        WHERE {w}
        ORDER BY d.created_at DESC NULLS LAST
        LIMIT ${len(params)+1} OFFSET ${len(params)+2}
    """, *params, limit, (page - 1) * limit)
    return _ok([dict(r) for r in rows], _page_meta(int(total), page, limit))


@router.get("/documents/{doc_id}")
@limiter.limit("60/minute")
async def document_detail(request: Request, doc_id: str):
    _require_configured()
    row = await ocoi_db.fetchrow("""
        SELECT d.id, d.title, d.file_url, d.file_format, d.file_size,
               d.conversion_status, d.extraction_status, d.verified,
               d.verified_at, d.created_at, d.converted_at, d.extracted_at,
               s.title AS source_title, s.source_type, s.url AS source_url
        FROM documents d
        LEFT JOIN sources s ON s.id = d.source_id
        WHERE d.id = $1
    """, _require_id(doc_id, "doc_id"))
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    return _ok(dict(row))


@router.get("/documents/{doc_id}/markdown")
@limiter.limit("60/minute")
async def document_markdown(request: Request, doc_id: str):
    _require_configured()
    row = await ocoi_db.fetchrow(
        "SELECT id, markdown_content FROM documents WHERE id = $1",
        _require_id(doc_id, "doc_id"))
    if row is None or not row["markdown_content"]:
        raise HTTPException(status_code=404, detail="Document not converted")
    return _ok({"id": row["id"], "markdown": row["markdown_content"]})


@router.get("/documents/{doc_id}/entities")
@limiter.limit("60/minute")
async def document_entities(
    request: Request, doc_id: str,
    limit: int = Query(_MAX_EDGES, ge=1, le=_MAX_EDGES),
):
    """Relationship rows extracted from one document.

    Capped like the graph walk. A single document really can carry thousands of
    edges — the MK-expenses workbook is imported as one "document" and produced
    5,218 — so an uncapped read here is a multi-megabyte response built from a
    single id, which is precisely the shape the walk cap exists to prevent.
    """
    _require_configured()
    doc_id = _require_id(doc_id, "doc_id")
    rows = await ocoi_db.fetch("""
        SELECT r.id, r.source_entity_type, r.source_entity_id,
               r.target_entity_type, r.target_entity_id, r.relationship_type,
               r.details, r.restriction_type, r.restriction_end_date,
               r.origin_kind, r.confidence, r.verified
        FROM entity_relationships r
        WHERE r.document_id = $1
        LIMIT $2
    """, doc_id, limit)
    edges = [dict(r) for r in rows]
    edges = _prune_hidden(edges, await _hidden_ids())
    names = await _hydrate_names(edges)
    for e in edges:
        e["source_name"] = names.get(
            (e["source_entity_type"], e["source_entity_id"]), {}).get("name", "")
        e["target_name"] = names.get(
            (e["target_entity_type"], e["target_entity_id"]), {}).get("name", "")
    return _ok(edges)


@router.get("/documents/{doc_id}/graph")
@limiter.limit("60/minute")
async def document_graph(request: Request, doc_id: str):
    """The document's own relationships as {nodes, edges} — same shape as
    /graph/*, so the frontend reuses one renderer."""
    _require_configured()
    doc_id = _require_id(doc_id, "doc_id")
    rows = await ocoi_db.fetch(f"""
        SELECT {_EDGE_COLS}
        FROM entity_relationships r
        LEFT JOIN documents d ON d.id = r.document_id
        WHERE r.document_id = $1
        LIMIT {_MAX_EDGES}
    """, doc_id)
    truncated = len(rows) >= _MAX_EDGES
    edges = _prune_hidden([dict(r) for r in rows], await _hidden_ids())
    return _ok(_subgraph(edges, await _hydrate_names(edges), truncated))


# RFC 5987: a Hebrew filename must not go out as a raw Latin-1 header value.
_UNSAFE_FILENAME = re.compile(r'[\\/:*?"<>|\r\n]+')

_CONTENT_TYPES = {
    "pdf": "application/pdf",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xls": "application/vnd.ms-excel",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "doc": "application/msword",
    "csv": "text/csv",
}


@router.get("/documents/{doc_id}/file")
@limiter.limit("30/minute")
async def document_file(request: Request, doc_id: str):
    """Serve the stored source file.

    Named ``/file`` rather than OCOI's ``/pdf``: the route already served xlsx,
    doc and csv there, and the old name made every caller guess. The public
    frontend iframes this, which is why OVER's X-Frame-Options must stay
    SAMEORIGIN for it.

    Bytes come from R2, not from Postgres. OCOI kept them in a `pdf_content`
    BYTEA column and spent the project fighting the consequences (a 4.5GB storage
    guard, a disabled backfill endpoint, a CKAN path that stores metadata only).
    The migration moved the 854 stored files to the `ocoi/` prefix of OVER's
    bucket, because the append DB backs the PUBLIC SQL console and has no
    business holding hundreds of megabytes of PDF.

    2,117 of the 2,971 documents never had bytes stored at all — they are
    re-fetchable from `file_url` and OCOI deliberately kept only metadata. Those
    404 with a message that says so rather than pretending the file is missing.
    """
    _require_configured()
    doc_id = _require_id(doc_id, "doc_id")
    row = await ocoi_db.fetchrow(
        "SELECT title, file_format, pdf_r2_key FROM documents WHERE id = $1", doc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Document not found")
    if not row["pdf_r2_key"]:
        raise HTTPException(
            status_code=404,
            detail="הקובץ אינו מאוחסן אצלנו — יש להוריד אותו מהמקור.",
        )
    from app.services.storage_client import storage_client
    data = await storage_client.get_object_bytes(row["pdf_r2_key"])
    if not data:
        # Key recorded but the object is gone/unreachable — a real fault, not
        # the ordinary "we never stored this one" case above.
        raise HTTPException(status_code=502, detail="הקובץ אינו זמין כרגע מהאחסון.")
    fmt = (row["file_format"] or "pdf").lower()
    name = _UNSAFE_FILENAME.sub("_", (row["title"] or "document").strip())
    if not name.lower().endswith(f".{fmt}"):
        name = f"{name}.{fmt}"
    from urllib.parse import quote
    return Response(
        content=bytes(data),
        media_type=_CONTENT_TYPES.get(fmt, "application/octet-stream"),
        headers={
            "Content-Disposition":
                f"inline; filename*=UTF-8''{quote(name, safe='')}",
            "Cache-Control": "public, max-age=3600",
        },
    )


# ---------------------------------------------------------------------------
# External integration API
# ---------------------------------------------------------------------------

@router.get("/external/by-company")
@limiter.limit("60/minute")
async def external_by_company(
    request: Request,
    registration_number: str = Query(..., min_length=1, max_length=20),
):
    _require_configured()
    row = await ocoi_db.fetchrow(
        f"SELECT {_ENTITY_COLUMNS['company']} FROM companies "
        f"WHERE registration_number = $1 AND hidden IS NOT TRUE",
        registration_number)
    if row is None:
        raise HTTPException(status_code=404, detail="Company not found")
    company = _entity_row(row, "company")
    return _ok({"company": company,
                "graph": await _subgraph_for("company", company["id"], 1, [])})


@router.get("/external/by-person")
@limiter.limit("60/minute")
async def external_by_person(
    request: Request, name: str = Query(..., min_length=2, max_length=100),
):
    _require_configured()
    rows = await ocoi_db.fetch(
        f"SELECT {_ENTITY_COLUMNS['person']} FROM persons "
        f"WHERE name_hebrew ILIKE $1 AND hidden IS NOT TRUE "
        f"ORDER BY name_hebrew LIMIT 5", f"%{name}%")
    out = []
    for r in rows:
        p = _entity_row(r, "person")
        out.append({"person": p,
                    "graph": await _subgraph_for("person", p["id"], 1, [])})
    return _ok(out)


@router.get("/external/by-ministry")
@limiter.limit("60/minute")
async def external_by_ministry(
    request: Request, name: str = Query(..., min_length=2, max_length=100),
):
    _require_configured()
    rows = await ocoi_db.fetch("""
        SELECT p.id, p.name_hebrew, p.position, p.ministry,
               COUNT(r.id) FILTER (WHERE r.restriction_type IS NOT NULL)
                   AS restrictions_count,
               COUNT(r.id) AS total_connections
        FROM persons p
        LEFT JOIN entity_relationships r
               ON r.source_entity_type = 'person' AND r.source_entity_id = p.id
        WHERE p.hidden IS NOT TRUE AND p.ministry ILIKE $1
        GROUP BY p.id, p.name_hebrew, p.position, p.ministry
        ORDER BY total_connections DESC
    """, f"%{name}%")
    return _ok([dict(r) for r in rows])


@router.get("/external/stats")
@limiter.limit("60/minute")
async def external_stats(request: Request):
    _require_configured()
    return _ok(await _counts())


async def _counts() -> dict:
    row = await ocoi_db.fetchrow("""
        SELECT (SELECT COUNT(*) FROM documents)             AS documents,
               (SELECT COUNT(*) FROM persons WHERE hidden IS NOT TRUE)      AS persons,
               (SELECT COUNT(*) FROM companies WHERE hidden IS NOT TRUE)    AS companies,
               (SELECT COUNT(*) FROM associations WHERE hidden IS NOT TRUE) AS associations,
               (SELECT COUNT(*) FROM domains WHERE hidden IS NOT TRUE)      AS domains,
               (SELECT COUNT(*) FROM entity_relationships)  AS relationships
    """)
    return dict(row) if row else {}


@router.get("/stats")
@limiter.limit("60/minute")
async def stats(request: Request):
    """Hero counters for the project page."""
    _require_configured()
    return _ok(await _counts())


# ---------------------------------------------------------------------------
# Site content + public suggestions
# ---------------------------------------------------------------------------

_ALLOWED_CONTENT_KEYS = {"header_links", "footer_text", "about_content"}


@router.get("/site/content/{key}")
@limiter.limit("60/minute")
async def site_content(request: Request, key: str):
    """CMS value. An unknown key returns an empty value (200), as OCOI did —
    the frontend renders whatever is there and must not 404 a missing block."""
    _require_configured()
    if key not in _ALLOWED_CONTENT_KEYS:
        return _ok({"key": key, "value": ""})
    row = await ocoi_db.fetchrow("SELECT key, value FROM site_content WHERE key = $1", key)
    return _ok({"key": key, "value": (row["value"] if row else "")})


_SUGGESTION_KINDS = {"document", "person", "company", "association", "domain", "relationship"}


class SuggestionIn(BaseModel):
    target_kind: str
    target_id: str
    field_name: str = Field(..., max_length=60)
    document_id: str | None = None
    current_value: str | None = None
    proposed_value: str | None = None
    comment: str | None = None
    submitter_email: str | None = Field(None, max_length=200)


@router.post("/suggestions")
@limiter.limit("5/minute")
async def create_suggestion(request: Request, body: SuggestionIn):
    """Anonymous correction submission.

    Rate-limited hard (OCOI had NO limiter at all on this anonymous write, which
    left a public INSERT wide open). Stores submitter_email only when supplied.
    """
    _require_configured()
    if body.target_kind not in _SUGGESTION_KINDS:
        raise HTTPException(
            status_code=400,
            detail=f"target_kind must be one of: {', '.join(sorted(_SUGGESTION_KINDS))}",
        )
    _require_id(body.target_id, "target_id")
    if body.document_id:
        _require_id(body.document_id, "document_id")
    if not (body.proposed_value or body.comment):
        raise HTTPException(
            status_code=400, detail="proposed_value or comment is required")

    new_id = str(_uuid.uuid4())
    await ocoi_db.execute("""
        INSERT INTO suggestions
            (id, target_kind, target_id, field_name, document_id,
             current_value, proposed_value, comment, submitter_email, status)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending')
    """, new_id, body.target_kind, body.target_id, body.field_name,
        body.document_id, body.current_value, body.proposed_value,
        body.comment, body.submitter_email)
    return _ok({"id": new_id})
