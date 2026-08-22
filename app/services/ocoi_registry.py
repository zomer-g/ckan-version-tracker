"""Government registry mirror + entity matching for ניגוד עניינים לעם (OCOI).

Five open CKAN resources on data.gov.il are mirrored into `registry_records`,
and extracted companies/associations are fuzzy-matched against them to recover
an official registration number.

Two things worth knowing before changing anything here:

* **data.gov.il is NOT the blocked host.** odata.org.il Cloudflare-blocks
  Render's datacenter IP, which is why document fetching lives on the worker;
  data.gov.il answers fine from the server, so this job runs on OVER.
* **Registry matching uses a DIFFERENT scorer from duplicate detection.** The
  duplicate matcher is the hand-rolled Hebrew token matcher in ocoi_match; this
  one is rapidfuzz over a legal-suffix-stripped name, exactly as OCOI had it.
  They are not interchangeable and were tuned separately.

The one behaviour deliberately changed: OCOI dedup'd a sync batch by
`registration_number` and **always inserted rows that had none**, so every sync
re-inserted the same numberless rows forever. Here the natural key falls back to
the normalised name, so a re-sync updates instead of duplicating.
"""
from __future__ import annotations

import logging
import re
import httpx
from rapidfuzz import fuzz

from app.services import ocoi_db, ocoi_match

logger = logging.getLogger(__name__)

DATAGOV = "https://data.gov.il"
_UA = "OVER/1.0 (+https://www.over.org.il) ocoi-registry"
PAGE = 2000
MATCH_THRESHOLD = 0.85

JOB_SYNC = "registry_sync"
JOB_MATCH = "registry_match"

# resource_id + the three fields we keep, per registry.
REGISTRY_SOURCES: dict[str, dict] = {
    "companies": {
        "resource_id": "f004176c-b85f-4542-8901-7b3176f9a054",
        "name": "שם חברה", "number": "מספר חברה", "status": "סטטוס חברה",
        "label": "חברות", "entity_type": "company",
    },
    "associations": {
        "resource_id": "be5b7935-3922-45d4-9638-08871b17ec95",
        "name": "שם עמותה בעברית", "number": "מספר עמותה",
        "status": "סטטוס עמותה", "label": "עמותות", "entity_type": "association",
    },
    "public_benefit": {
        "resource_id": "85e40960-5426-4f4c-874f-2d1ec1b94609",
        "name": "שם חלצ בעברית", "number": "מספר חלצ", "status": "סטטוס חלצ",
        "label": "חברות לתועלת הציבור", "entity_type": "company",
    },
    "local_authorities": {
        "resource_id": "c4916937-f5d3-4295-a22e-88a1af5cde6a",
        "name": "LocalAuthorityName", "number": "LocalAuthorityHPNumber",
        "status": None, "label": "רשויות מקומיות", "entity_type": "company",
    },
    # Kept visible, never synced. OCOI configured the number field as
    # `corporation_number`, which does not exist in the resource — CKAN answers
    # 409 to any request naming it, which is why this registry sat at 0 rows
    # since the day it was added. Correcting the field name would not help: the
    # resource is a board-MEMBER list (3,436 rows, one per seat) carrying no
    # registration number at all, and match_entity can only attach a number.
    "municipal_corporations": {
        "resource_id": "4d7e9bb8-2457-46f9-9eb3-0c0acf5cd766",
        "name": "corporation", "number": None, "status": None,
        "label": "תאגידים עירוניים", "entity_type": "company",
        "enabled": False,
        "note": "המקור אינו כולל מספר תאגיד — אין ממה להתאים",
    },
}

COMPANY_SOURCES = ("companies", "public_benefit", "local_authorities")
ASSOCIATION_SOURCES = ("associations",)

_ENABLED = tuple(k for k, v in REGISTRY_SOURCES.items() if v.get("enabled", True))

# Faithful port of ocoi_matcher.fuzzy_match.STRIP_PATTERNS.
_STRIP = [
    r'\bבע"מ\b', r"\bבע״מ\b", r"\bבע'מ\b",
    r"\bחברה ל\b", r"\bחברת\b", r"\bמפעלי\b", r"\bקבוצת\b",
    r"\bתעשיות\b", r"\bהולדינגס\b", r"\bישראל\b", r"\bלישראל\b",
    r"\bבית השקעות\b", r"\(\s*\)", r"\s+",
]


def normalize_company_name(name: str) -> str:
    """Strip legal boilerplate and collapse whitespace. NOT lowercased — Hebrew
    is unicase and the original did not lowercase either."""
    if not name:
        return ""
    out = name.strip()
    for pat in _STRIP:
        out = re.sub(pat, " ", out)
    return " ".join(out.split()).strip()


def match_score(a: str, b: str) -> float:
    """rapidfuzz blend, as OCOI had it: the best of ratio, a discounted
    partial_ratio and a slightly discounted token_sort_ratio."""
    n1, n2 = normalize_company_name(a), normalize_company_name(b)
    if not n1 or not n2:
        return 0.0
    if n1 == n2:
        return 1.0
    return max(
        fuzz.ratio(n1, n2) / 100.0,
        fuzz.partial_ratio(n1, n2) / 100.0 * 0.9,
        fuzz.token_sort_ratio(n1, n2) / 100.0 * 0.95,
    )


async def _fetch_page(client: httpx.AsyncClient, resource_id: str,
                      offset: int, fields: list[str]) -> tuple[list[dict], int]:
    """One datastore page. CKAN answers 409 while a datastore rebuilds, which is
    routine and can last minutes — that is retried by the caller, not treated as
    a failure."""
    r = await client.get(
        f"{DATAGOV}/api/3/action/datastore_search",
        params={"resource_id": resource_id, "limit": PAGE, "offset": offset,
                "fields": ",".join(fields)})
    r.raise_for_status()
    body = r.json() or {}
    if not body.get("success"):
        raise RuntimeError(str(body.get("error"))[:200])
    res = body.get("result") or {}
    return res.get("records") or [], int(res.get("total") or 0)


async def sync_registry(source: str) -> dict:
    """Mirror one registry into `registry_records`."""
    cfg = REGISTRY_SOURCES[source]
    if not cfg.get("enabled", True):
        raise RuntimeError(cfg.get("note") or f"{source} is disabled")
    fields = [f for f in (cfg["name"], cfg["number"], cfg["status"]) if f]
    stats = {"source": source, "fetched": 0, "inserted": 0, "updated": 0,
             "skipped": 0, "total": 0}
    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0),
                                 follow_redirects=True,
                                 headers={"User-Agent": _UA}) as client:
        offset = 0
        while True:
            try:
                records, total = await _fetch_page(client, cfg["resource_id"],
                                                   offset, fields)
            except httpx.HTTPStatusError as e:
                # 409 means CKAN would not answer this query — a datastore
                # mid-rebuild, or a field name that does not exist. Either way
                # the page is missing, so the run is NOT complete; recording it
                # as completed is what hid municipal_corporations' broken field
                # config for as long as it was broken.
                if e.response.status_code == 409:
                    await _stamp(source, stats["fetched"], "failed",
                                 f"CKAN 409 at offset {offset}")
                    raise RuntimeError(
                        f"data.gov.il refused the query for {source} at offset "
                        f"{offset} (409) — datastore rebuilding, or a field in "
                        f"the config does not exist in the resource") from e
                raise
            stats["total"] = total
            if not records:
                break
            stats["fetched"] += len(records)
            ins, upd, skip = await _upsert_batch(source, cfg, records)
            stats["inserted"] += ins
            stats["updated"] += upd
            stats["skipped"] += skip
            await ocoi_match.set_progress(
                JOB_SYNC, source=source, fetched=stats["fetched"], total=total,
                inserted=stats["inserted"], updated=stats["updated"])
            offset += PAGE
            if offset >= total:
                break

    await _stamp(source, stats["fetched"], "completed", None)
    return stats


async def _stamp(source: str, count: int, status: str, error: str | None) -> None:
    """last_synced_at is the schema's only timestamptz here, so plain now()."""
    await ocoi_db.execute("""
        INSERT INTO registry_sync_status
            (id, source_type, last_synced_at, record_count, sync_status, error_message)
        VALUES (gen_random_uuid()::text, $1, now(), $2, $3, $4)
        ON CONFLICT (source_type) DO UPDATE
           SET last_synced_at = now(), record_count = $2,
               sync_status = $3, error_message = $4
    """, source, count, status, error)


async def _upsert_batch(source: str, cfg: dict, records: list[dict]) -> tuple[int, int, int]:
    """Insert or update one page.

    The natural key is (source_type, registration_number) when a number exists
    and (source_type, name_normalized) when it does not. OCOI keyed only on the
    number and blind-inserted numberless rows, so every sync duplicated them.
    """
    rows = []
    seen: set[tuple] = set()
    for rec in records:
        name = (rec.get(cfg["name"]) or "").strip()
        if not name:
            continue
        num = rec.get(cfg["number"])
        num = str(num).strip() if num not in (None, "") else None
        status = (rec.get(cfg["status"]) or None) if cfg["status"] else None
        norm = normalize_company_name(name)
        key = (num or f"~{norm}")
        if key in seen:
            continue
        seen.add(key)
        rows.append((name, norm, num, status, key))
    if not rows:
        return 0, 0, len(records)

    # Which of these do we already hold?
    with_num = [r[2] for r in rows if r[2]]
    without = [r[1] for r in rows if not r[2]]
    existing: dict[tuple, str] = {}
    if with_num:
        for e in await ocoi_db.fetch(
                "SELECT id, registration_number FROM registry_records "
                "WHERE source_type = $1 AND registration_number = ANY($2::text[])",
                source, with_num):
            existing[(e["registration_number"],)] = e["id"]
    if without:
        for e in await ocoi_db.fetch(
                "SELECT id, name_normalized FROM registry_records "
                "WHERE source_type = $1 AND registration_number IS NULL "
                "AND name_normalized = ANY($2::text[])", source, without):
            existing[(f"~{e['name_normalized']}",)] = e["id"]

    ins = upd = 0
    new_rows, upd_rows = [], []
    for name, norm, num, status, key in rows:
        eid = existing.get((key,))
        if eid:
            upd_rows.append((eid, name, norm, status))
        else:
            new_rows.append((name, norm, num, status))
    if new_rows:
        await ocoi_db.execute("""
            INSERT INTO registry_records
                (id, source_type, name, name_normalized, registration_number,
                 status, raw_data, created_at, updated_at)
            SELECT gen_random_uuid()::text, $1, x.n, x.nm, x.num, x.st,
                   NULL, now() AT TIME ZONE 'Asia/Jerusalem',
                   now() AT TIME ZONE 'Asia/Jerusalem'
              FROM unnest($2::text[], $3::text[], $4::text[], $5::text[])
                   AS x(n, nm, num, st)
        """, source, [r[0] for r in new_rows], [r[1] for r in new_rows],
            [r[2] for r in new_rows], [r[3] for r in new_rows])
        ins = len(new_rows)
    if upd_rows:
        await ocoi_db.execute("""
            UPDATE registry_records r
               SET name = x.n, name_normalized = x.nm, status = x.st,
                   updated_at = now() AT TIME ZONE 'Asia/Jerusalem'
              FROM unnest($1::text[], $2::text[], $3::text[], $4::text[])
                   AS x(id, n, nm, st)
             WHERE r.id = x.id
        """, [r[0] for r in upd_rows], [r[1] for r in upd_rows],
            [r[2] for r in upd_rows], [r[3] for r in upd_rows])
        upd = len(upd_rows)
    return ins, upd, len(records) - len(rows)


async def run_sync_all(sources: tuple[str, ...] | None = None) -> dict:
    out = {}
    try:
        for src in (sources or _ENABLED):
            if src not in REGISTRY_SOURCES:
                continue
            if not REGISTRY_SOURCES[src].get("enabled", True):
                out[src] = {"source": src, "skipped": True,
                            "reason": REGISTRY_SOURCES[src].get("note")}
                continue
            out[src] = await sync_registry(src)
        await ocoi_match.set_progress(JOB_SYNC, done=out)
        await ocoi_match.finish_job(JOB_SYNC)
    except Exception as e:  # noqa: BLE001
        logger.exception("ocoi registry sync failed")
        await ocoi_match.finish_job(JOB_SYNC, error=str(e)[:500])
        raise
    return out


# ── matching entities to the registry ────────────────────────────────────────

async def match_entity(entity_type: str, entity_id: str, name: str) -> dict | None:
    """Exact normalised match first, then a prefix block scored with rapidfuzz."""
    sources = COMPANY_SOURCES if entity_type == "company" else ASSOCIATION_SOURCES
    norm = normalize_company_name(name)
    if len(norm) < 2:
        return None
    hit = await ocoi_db.fetchrow("""
        SELECT id, name, registration_number FROM registry_records
         WHERE source_type = ANY($1::text[]) AND name_normalized = $2
         LIMIT 1""", list(sources), norm)
    score = 1.0
    if hit is None:
        cands = await ocoi_db.fetch("""
            SELECT id, name, registration_number FROM registry_records
             WHERE source_type = ANY($1::text[]) AND name_normalized LIKE $2
             LIMIT 1000""", list(sources), norm[:3] + "%")
        best, best_s = None, 0.0
        for c in cands:
            s = match_score(name, c["name"])
            if s > best_s:
                best, best_s = c, s
        if best is None or best_s < MATCH_THRESHOLD:
            return None
        hit, score = best, best_s
    if not hit["registration_number"]:
        return None
    table = "companies" if entity_type == "company" else "associations"
    await ocoi_db.execute(f"""
        UPDATE {table} SET registration_number = $2, match_confidence = $3,
               registry_record_id = $4 WHERE id = $1
    """, entity_id, hit["registration_number"], float(score), hit["id"])
    return {"entity_id": entity_id, "registration_number": hit["registration_number"],
            "score": float(score), "registry_record_id": hit["id"]}


async def _match_table(etype: str, table: str, limit: int | None,
                       stats: dict) -> None:
    """Match one entity table against the mirror, in bulk.

    OCOI issued two queries per entity — an exact lookup and a prefix scan. At
    ~9,700 unmatched entities that is ~19,000 round trips, and Neon bills
    compute by the second, so the cost is in the latency rather than the work
    (the queries themselves are index scans of 0.04ms and 0.2ms). Here the
    exact pass is one query, and the fuzzy pass is one query per distinct
    three-character prefix — the same blocking key, just shared.
    """
    sources = list(COMPANY_SOURCES if etype == "company" else ASSOCIATION_SOURCES)
    q = (f"SELECT id, name_hebrew FROM {table} "
         f"WHERE registration_number IS NULL AND hidden IS NOT TRUE "
         f"AND name_hebrew IS NOT NULL")
    if limit:
        q += f" LIMIT {int(limit)}"
    rows = await ocoi_db.fetch(q)

    todo = []  # (entity_id, original_name, normalised_name)
    for r in rows:
        norm = normalize_company_name(r["name_hebrew"])
        if len(norm) >= 2:
            todo.append((r["id"], r["name_hebrew"], norm))
    stats["considered"] += len(todo)
    if not todo:
        return

    hits: dict[str, tuple[str, str, float]] = {}  # entity_id -> (num, rec, score)

    # ── pass 1: exact on the normalised name ─────────────────────────────────
    # A name can hit several mirrored rows; prefer one that actually carries a
    # number. OCOI took LIMIT 1 and gave up if that row happened to have none.
    exact: dict[str, tuple[str, str]] = {}
    for chunk in _chunks(sorted({t[2] for t in todo}), 5000):
        for rec in await ocoi_db.fetch("""
            SELECT id, name_normalized, registration_number
              FROM registry_records
             WHERE source_type = ANY($1::text[])
               AND registration_number IS NOT NULL
               AND name_normalized = ANY($2::text[])""", sources, chunk):
            exact.setdefault(rec["name_normalized"],
                             (rec["registration_number"], rec["id"]))
    for eid, _name, norm in todo:
        if norm in exact:
            hits[eid] = (*exact[norm], 1.0)
    await ocoi_match.set_progress(JOB_MATCH, entity_type=etype, phase="exact",
                                  considered=len(todo), matched=len(hits))

    # ── pass 2: fuzzy inside the prefix block, one query per prefix ──────────
    rest = [t for t in todo if t[0] not in hits]
    blocks: dict[str, list] = {}
    for item in rest:
        blocks.setdefault(item[2][:3], []).append(item)
    for i, (prefix, group) in enumerate(blocks.items(), 1):
        cands = await ocoi_db.fetch("""
            SELECT id, name, registration_number FROM registry_records
             WHERE source_type = ANY($1::text[])
               AND registration_number IS NOT NULL
               AND name_normalized LIKE $2
             LIMIT 1000""", sources, prefix.replace("%", "").replace("_", "") + "%")
        if not cands:
            continue
        for eid, name, _norm in group:
            best, best_s = None, 0.0
            for c in cands:
                s = match_score(name, c["name"])
                if s > best_s:
                    best, best_s = c, s
            if best is not None and best_s >= MATCH_THRESHOLD:
                hits[eid] = (best["registration_number"], best["id"], best_s)
        if i % 100 == 0:
            await ocoi_match.set_progress(
                JOB_MATCH, entity_type=etype, phase="fuzzy",
                blocks_done=i, blocks=len(blocks), matched=len(hits))

    # ── write ────────────────────────────────────────────────────────────────
    for chunk in _chunks(list(hits.items()), 1000):
        await ocoi_db.execute(f"""
            UPDATE {table} t
               SET registration_number = x.num, match_confidence = x.score,
                   registry_record_id = x.rec
              FROM unnest($1::text[], $2::text[], $3::text[], $4::float8[])
                   AS x(id, num, rec, score)
             WHERE t.id = x.id
        """, [k for k, _ in chunk], [v[0] for _, v in chunk],
            [v[1] for _, v in chunk], [float(v[2]) for _, v in chunk])
    stats["matched"] += len(hits)


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


async def run_match_all(limit: int | None = None) -> dict:
    """Match every company/association still lacking a registration number."""
    stats = {"considered": 0, "matched": 0}
    try:
        for etype, table in (("company", "companies"),
                             ("association", "associations")):
            await _match_table(etype, table, limit, stats)
        await ocoi_match.set_progress(JOB_MATCH, **stats)
        await ocoi_match.finish_job(JOB_MATCH)
    except Exception as e:  # noqa: BLE001
        logger.exception("ocoi registry match failed")
        await ocoi_match.finish_job(JOB_MATCH, error=str(e)[:500])
        raise
    return stats
