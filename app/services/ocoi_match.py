"""Duplicate detection and job coordination for ניגוד עניינים לעם (OCOI).

Two things live here.

**1. The Hebrew name matcher — a FAITHFUL port.** Every constant, threshold and
short-circuit below is copied from `ocoi_matcher/hebrew_names.py` rather than
re-derived, because that module is tuned, not merely written: earlier versions
had prefix and substring rules and both were removed after they produced
95-member "ארומה <city>" clusters of legitimately different branches. The
token-subset rule is organisations-only for the same class of reason — Hebrew
first names overlap across unrelated people, and any subset rule for persons
chains them transitively through union-find into "everyone called X". Do not
"improve" this without measuring against the corpus first.

**2. Job coordination.** OCOI tracked every long job in a module-level dict.
That assumes one process: a status poll served by another worker reports "not
running", two workers will happily run the same scan at once, and a redeploy
mid-run leaves the flag stuck true forever with no way to clear it but a code
path built for that purpose. State lives in a table here.

The claim is an atomic conditional UPDATE rather than a Postgres advisory lock.
An advisory lock is held by a *session*, and these queries run over a pooled
connection through a pgbouncer-style pooler — the lock's lifetime would be the
pooled connection's, which is neither the job's nor the process's. A
compare-and-set on a row has none of that ambiguity and survives restarts.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Iterable

from app.services import ocoi_db

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Hebrew name matching — faithful port of ocoi_matcher.hebrew_names
# ═══════════════════════════════════════════════════════════════════════════

HONORIFICS: frozenset[str] = frozenset({
    "מר", "גב", "גבת", "האדון", "הגב", "האדם",
    "ד", "דר", "דוקטור", "ד״ר",
    "פרופ", "פרופסור",
    "עו", "עוד", "עו״ד",
    # OCR/parsing residues of "עו״ד" — see the original's note
    "וד", "עוייד", "עויד", "עוד״",
    "רב", "הרב",
    "מהנדס", "אדריכל",
    "השר", "השרה", "שר", "שרה", "סגן", "סגנית",
    "ח״כ", "חכ", "ראש", "ראשת",
})

COMPANY_SUFFIXES: frozenset[str] = frozenset({
    "בעמ", "בע", "ב.ע.מ",
    "חברה",
    "אגודה", "אגודה שיתופית", "שיתופית",
    "עמותה",
    "ושות", "ושותפיו", "ושותפים",
    "ltd", "limited", "llc", "inc", "corp",
})

ORG_PREFIXES: frozenset[str] = frozenset({
    "עמותת", "תנועת", "חברת", "אגודת", "מפלגת", "ארגון", "ארגונת",
    "קרן", "קבוצת", "מכון", "מועדון", "מפעל", "ועד", "ועדת",
    "ר", "רעמ",
    "עמותהת", "תנועהת",
    "עמותות", "תנועות", "חברות", "מפלגות",
    "ה", "של", "את",
})

_WHITESPACE_RE = re.compile(r"\s+")
_PUNCT_RE = re.compile(r"[\"'״׳`]")
_DASH_RE = re.compile(r"[‐-―\-]+")

# Only write a proposal at or above this confidence.
SCORE_THRESHOLD = 0.85
# `domain` is excluded: short topical labels, unsafe to fuzzy-merge.
SCAN_KINDS = ("person", "company", "association")
_TABLES = {"person": "persons", "company": "companies",
           "association": "associations", "domain": "domains"}


def normalize(name: str) -> str:
    if not name:
        return ""
    s = _DASH_RE.sub(" ", _PUNCT_RE.sub("", name)).strip()
    return _WHITESPACE_RE.sub(" ", s).lower()


def tokens(name: str, *, kind: str = "person") -> list[str]:
    """Sorted, honorific-stripped tokens. Single-character tokens are dropped:
    they are artefacts of gershayim splitting, not disambiguating names."""
    if not name:
        return []
    raw = [t for t in normalize(name).split(" ") if t]
    drops = HONORIFICS if kind == "person" else (
        HONORIFICS | COMPANY_SUFFIXES | ORG_PREFIXES)
    out = [t for t in raw if t not in drops and len(t) > 1]
    out.sort()
    return out


def blocking_key(name: str, *, kind: str = "person") -> str:
    """First two characters of the longest token. Two names that do not share
    this are never compared — the whole reason the scan is not O(N²)."""
    ts = tokens(name, kind=kind)
    if not ts:
        return ""
    return max(ts, key=len)[:2]


def _levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        cur = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cur[j] = min(cur[j - 1] + 1, prev[j] + 1,
                         prev[j - 1] + (0 if ca == cb else 1))
        prev = cur
    return prev[-1]


def _token_sort_ratio(a: list[str], b: list[str]) -> float:
    if not a or not b:
        return 0.0
    sa, sb = " ".join(a), " ".join(b)
    if sa == sb:
        return 1.0
    m = max(len(sa), len(sb))
    return 0.0 if m == 0 else 1.0 - _levenshtein(sa, sb) / m


def _jaccard(a: Iterable[str], b: Iterable[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def similarity(a: str, b: str, *, kind: str = "person") -> tuple[float, list[str]]:
    """(score, reasons). Short-circuit order is load-bearing — see module docs."""
    if not a or not b:
        return 0.0, []
    if normalize(a) == normalize(b):
        return 1.0, ["exact_normalised"]

    ta, tb = tokens(a, kind=kind), tokens(b, kind=kind)
    if not ta or not tb:
        return 0.0, []

    reasons: list[str] = []
    if set(ta) == set(tb):
        # Deliberately just under 1.0 so exact strings sort above word swaps.
        return 0.97, ["tokens_identical"]

    if kind != "person":
        sa, sb = set(ta), set(tb)
        small, big = (sa, sb) if len(sa) <= len(sb) else (sb, sa)
        if len(small) >= 2 and small.issubset(big):
            coverage = len(small) / len(big)
            if coverage >= 0.5:
                reasons.append(f"token_subset={len(small)}/{len(big)}")
                return max(0.88, 0.7 + 0.25 * coverage), reasons

    tsr = _token_sort_ratio(ta, tb)
    jac = _jaccard(ta, tb)
    if tsr > 0:
        reasons.append(f"token_sort={tsr:.2f}")
    if jac > 0:
        reasons.append(f"jaccard={jac:.2f}")
    swap_bonus = 0.0
    if len(ta) == 2 and len(tb) == 2 and set(ta) == set(tb) and ta != tb:
        swap_bonus = 0.08
        reasons.append("two_token_swap")
    if ta[0] == tb[0]:
        reasons.append("first_token_match")
    return max(0.0, min(1.0, 0.6 * tsr + 0.4 * jac + swap_bonus)), reasons


# ═══════════════════════════════════════════════════════════════════════════
# Job coordination
# ═══════════════════════════════════════════════════════════════════════════

_JOBS_DDL = [
    """CREATE TABLE IF NOT EXISTS ocoi_jobs (
        kind        text PRIMARY KEY,
        status      text NOT NULL DEFAULT 'idle',
        progress    jsonb NOT NULL DEFAULT '{}'::jsonb,
        started_at  timestamptz,
        finished_at timestamptz,
        error       text)""",
]
# A job whose heartbeat is older than this is presumed dead (the process was
# redeployed mid-run) and may be reclaimed. OCOI had no equivalent: a crash left
# `running: true` in memory forever, and only a restart cleared it.
STALE_AFTER = timedelta(minutes=30)

_jobs_ready = False


async def ensure_jobs_table() -> None:
    global _jobs_ready
    if _jobs_ready:
        return
    for stmt in _JOBS_DDL:
        await ocoi_db.execute(stmt)
    _jobs_ready = True


async def claim_job(kind: str) -> bool:
    """Atomically take the slot for `kind`. False if it is already running.

    One statement, so two callers racing cannot both win — which the module-dict
    version could not promise across processes.
    """
    await ensure_jobs_table()
    cutoff = datetime.now(timezone.utc) - STALE_AFTER
    got = await ocoi_db.fetchval("""
        INSERT INTO ocoi_jobs (kind, status, progress, started_at, finished_at, error)
        VALUES ($1, 'running', '{}'::jsonb, now(), NULL, NULL)
        ON CONFLICT (kind) DO UPDATE
           SET status = 'running', progress = '{}'::jsonb,
               started_at = now(), finished_at = NULL, error = NULL
         WHERE ocoi_jobs.status <> 'running'
            OR ocoi_jobs.started_at < $2
        RETURNING kind
    """, kind, cutoff)
    return got is not None


async def set_progress(kind: str, **fields) -> None:
    """Merge fields into the job's progress object.

    Bound as ``text`` and cast in SQL, NOT as jsonb: ocoi_db registers a jsonb
    codec whose encoder is json.dumps, so binding an already-serialised string
    to a jsonb parameter encodes it a second time. Postgres then sees a JSON
    *string* rather than an object, and ``object || string`` yields an ARRAY —
    so progress silently grew into a list of escaped blobs instead of merging.
    """
    await ocoi_db.execute(
        "UPDATE ocoi_jobs SET progress = progress || $2::text::jsonb "
        "WHERE kind = $1",
        kind, json.dumps(fields, ensure_ascii=False, default=str))


async def finish_job(kind: str, error: str | None = None) -> None:
    await ocoi_db.execute("""
        UPDATE ocoi_jobs SET status = $2, finished_at = now(), error = $3
         WHERE kind = $1""", kind, "failed" if error else "done", error)


async def job_status(kind: str | None = None) -> list[dict]:
    await ensure_jobs_table()
    if kind:
        rows = await ocoi_db.fetch("SELECT * FROM ocoi_jobs WHERE kind = $1", kind)
    else:
        rows = await ocoi_db.fetch("SELECT * FROM ocoi_jobs ORDER BY kind")
    out = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("progress"), str):
            try:
                d["progress"] = json.loads(d["progress"])
            except ValueError:
                d["progress"] = {}
        out.append(d)
    return out


async def reset_job(kind: str) -> None:
    """Force-clear a stuck slot. The escape hatch OCOI needed and lacked."""
    await ensure_jobs_table()
    await ocoi_db.execute(
        "UPDATE ocoi_jobs SET status='idle', finished_at=now() WHERE kind=$1", kind)


# ═══════════════════════════════════════════════════════════════════════════
# Duplicate scan
# ═══════════════════════════════════════════════════════════════════════════

JOB_SCAN = "duplicate_scan"


async def run_duplicate_scan(kinds: tuple[str, ...] = SCAN_KINDS) -> dict:
    """Bucket by blocking key, score every pair in a bucket, write proposals.

    Pairs already carrying a proposal in ANY status are skipped, so a reviewed
    pair is never re-offered — the same convergence rule the discovery path
    needed. Alias strings are indexed alongside the name, and a pair is scored
    at the best of the full alias cross-product.
    """
    summary = {"kinds": list(kinds), "candidate_pairs": 0, "written": 0,
               "skipped_existing": 0, "scanned": 0}
    try:
        for kind in kinds:
            if kind not in _TABLES or kind not in SCAN_KINDS:
                continue
            table = _TABLES[kind]
            rows = await ocoi_db.fetch(
                f"SELECT id, name_hebrew, aliases FROM {table} WHERE hidden IS NOT TRUE")
            await set_progress(JOB_SCAN, current_kind=kind, total=len(rows))

            buckets: dict[str, list[tuple[str, list[str]]]] = {}
            for r in rows:
                names = [r["name_hebrew"] or ""]
                names += ocoi_db.decode_aliases(r["aliases"])
                names = [n for n in names if n]
                for n in names:
                    k = blocking_key(n, kind=kind)
                    if k:
                        buckets.setdefault(k, []).append((r["id"], names))
            summary["scanned"] += len(rows)

            existing: set[frozenset] = set()
            for p in await ocoi_db.fetch(
                    "SELECT entity_id, target_id FROM entity_match_proposals "
                    "WHERE proposal_kind='duplicate' AND entity_type=$1", kind):
                existing.add(frozenset((p["entity_id"], p["target_id"])))

            seen: set[frozenset] = set()
            batch: list[tuple] = []
            for members in buckets.values():
                if len(members) < 2:
                    continue
                for i in range(len(members)):
                    for j in range(i + 1, len(members)):
                        ida, na = members[i]
                        idb, nb = members[j]
                        if ida == idb:
                            continue
                        pair = frozenset((ida, idb))
                        if pair in seen:
                            continue
                        seen.add(pair)
                        if pair in existing:
                            summary["skipped_existing"] += 1
                            continue
                        summary["candidate_pairs"] += 1
                        best, why = 0.0, []
                        for x in na:
                            for y in nb:
                                sc, rs = similarity(x, y, kind=kind)
                                if sc > best:
                                    best, why = sc, rs
                                if best >= 0.999:
                                    break
                            if best >= 0.999:
                                break
                        if best >= SCORE_THRESHOLD:
                            batch.append((kind, ida, idb, best,
                                          json.dumps(why, ensure_ascii=False)))
            for i in range(0, len(batch), 200):
                chunk = batch[i:i + 200]
                await ocoi_db.execute("""
                    INSERT INTO entity_match_proposals
                      (id, proposal_kind, entity_type, entity_id, target_kind,
                       target_type, target_id, score, reasons, status, created_at)
                    SELECT gen_random_uuid()::text, 'duplicate', x.k, x.a, 'entity',
                           x.k, x.b, x.s, x.r, 'pending',
                           now() AT TIME ZONE 'Asia/Jerusalem'
                      FROM unnest($1::text[], $2::text[], $3::text[],
                                  $4::float8[], $5::text[]) AS x(k, a, b, s, r)
                    ON CONFLICT DO NOTHING
                """, [c[0] for c in chunk], [c[1] for c in chunk],
                    [c[2] for c in chunk], [c[3] for c in chunk],
                    [c[4] for c in chunk])
                summary["written"] += len(chunk)
                await set_progress(JOB_SCAN, written=summary["written"])
        await set_progress(JOB_SCAN, **summary)
        await finish_job(JOB_SCAN)
    except Exception as e:  # noqa: BLE001 — a stuck slot is worse than a failure
        logger.exception("ocoi duplicate scan failed")
        await finish_job(JOB_SCAN, error=str(e)[:500])
        raise
    return summary


# ═══════════════════════════════════════════════════════════════════════════
# Clusters
# ═══════════════════════════════════════════════════════════════════════════

async def build_clusters(entity_type: str | None = None, min_score: float = 0.85,
                         limit: int = 30) -> tuple[list[dict], dict]:
    """Union-find over pending proposals, truncated BEFORE hydration.

    Truncating first is the whole performance trick from the original: hydrating
    every component and then slicing turned a 0.3s response into 12s.
    """
    where, args = ["proposal_kind = 'duplicate'", "status = 'pending'"], []
    args.append(min_score)
    where.append(f"score >= ${len(args)}")
    if entity_type:
        args.append(entity_type)
        where.append(f"entity_type = ${len(args)}")
    props = await ocoi_db.fetch(
        f"SELECT id, entity_type, entity_id, target_id, score, reasons "
        f"FROM entity_match_proposals WHERE {' AND '.join(where)}", *args)
    if not props:
        return [], {"total": 0, "limit": limit}

    parent: dict[tuple, tuple] = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for p in props:
        union((p["entity_type"], p["entity_id"]), (p["entity_type"], p["target_id"]))

    comps: dict[tuple, list] = {}
    for p in props:
        comps.setdefault(find((p["entity_type"], p["entity_id"])), []).append(p)

    groups = []
    for root, plist in comps.items():
        members = set()
        for p in plist:
            members.add((p["entity_type"], p["entity_id"]))
            members.add((p["entity_type"], p["target_id"]))
        if len(members) >= 2:
            groups.append((root, sorted(members), plist))
    total = len(groups)
    groups.sort(key=lambda g: (-len(g[1]), g[0][0], g[0][1]))
    groups = groups[:limit]

    # hydrate: one query per entity type
    want: dict[str, set[str]] = {}
    for _, members, _ in groups:
        for t, i in members:
            want.setdefault(t, set()).add(i)
    info: dict[tuple, dict] = {}
    for t, ids in want.items():
        table = _TABLES[t]
        for r in await ocoi_db.fetch(
                f"SELECT id, name_hebrew, aliases FROM {table} "
                f"WHERE id = ANY($1::text[])", list(ids)):
            info[(t, r["id"])] = {"id": r["id"], "type": t,
                                  "name": r["name_hebrew"] or "",
                                  "aliases": ocoi_db.decode_aliases(r["aliases"])}
        deg = await ocoi_db.fetch("""
            SELECT id, sum(c)::int AS c FROM (
              SELECT source_entity_id AS id, count(*) AS c FROM entity_relationships
               WHERE source_entity_type = $1 AND source_entity_id = ANY($2::text[])
               GROUP BY source_entity_id
              UNION ALL
              SELECT target_entity_id, count(*) FROM entity_relationships
               WHERE target_entity_type = $1 AND target_entity_id = ANY($2::text[])
               GROUP BY target_entity_id) x GROUP BY id""", t, list(ids))
        for r in deg:
            if (t, r["id"]) in info:
                info[(t, r["id"])]["connections"] = int(r["c"] or 0)

    out = []
    for root, members, plist in groups:
        hydrated = []
        for key in members:
            m = info.get(key, {"id": key[1], "type": key[0], "name": "(נמחק)",
                               "aliases": []})
            m.setdefault("connections", 0)
            hydrated.append(m)
        # canonical: most connections, then longest name, then lowest id
        hydrated.sort(key=lambda m: (-m["connections"], -len(m["name"] or ""), m["id"]))
        out.append({
            "entity_type": root[0],
            "size": len(hydrated),
            "canonical_id": hydrated[0]["id"],
            "members": hydrated,
            "proposals": [{"id": p["id"], "left_id": p["entity_id"],
                           "right_id": p["target_id"], "score": float(p["score"]),
                           "reasons": _reasons(p["reasons"])} for p in plist],
        })
    return out, {"total": total, "limit": limit, "shown": len(out)}


def _reasons(raw) -> list[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw]
    try:
        v = json.loads(raw)
        return [str(x) for x in v] if isinstance(v, list) else []
    except (TypeError, ValueError):
        return []
