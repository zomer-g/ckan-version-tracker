"""נדל"ן לעם — geocoding the addresses that have no coordinates, via the queue.

617,876 addresses in ``over_re_addresses``; only ~358k carry a point. An address
without a point gets no parcel (the link is point-in-polygon), so ~260k are
stranded. GovMap's search service geocodes them one at a time — verified against
40 of our own confirmed addresses at a **median 1.5 m** from the point we already
had, i.e. effectively the same MAPI source.

**Why this rides the scrape queue rather than a scheduler loop.** The work runs
on the WORKER, not on Render: a residential IP, which is the whole reason the
worker exists. And putting it in the queue is what makes the priority promise
expressible — a GovMap layer entering the queue must be claimed before the next
geocoding batch, which is exactly ``PRIORITY_GEOCODE < PRIORITY_COVERAGE``.

**The batch selection is IDEMPOTENT, and that is load-bearing.** The worker
deliberately keeps no checkpoint: a batch that aborts sends back whatever it
finished, and the rest simply reappear because the selection re-runs against
``point IS NULL``. That only holds if this module never hands out a "reserved"
batch it then has to reconcile — a second copy of the truth that can disagree
with the first is precisely how work silently goes missing. So: no reservation,
re-select every call.

**The three outcomes are not the same thing** (the worker splits them and the
distinction matters):

* ``results``   — geocoded. Recorded with the point.
* ``not_found`` — GovMap says no such address. Recorded as a TERMINAL miss, so
  the address stops being offered; otherwise ``point IS NULL`` would hand it
  back forever.
* ``failed``    — we never got an answer (transient error, or the batch aborted
  before reaching it). Recorded NOWHERE, so it returns on its own next time.
  Treating this as absence would quietly drop an address that was available.
"""
from __future__ import annotations

import logging
import uuid as _uuid

from sqlalchemy import select

from app.models.scrape_task import PRIORITY_GEOCODE, ScrapeTask
from app.models.tracked_dataset import TrackedDataset
from app.services import append_store
from app.services.append_store import _qi
from app.services.nadlan_index import ADDRESSES_TABLE

logger = logging.getLogger(__name__)

GEOCODE_TABLE = "over_re_geocode"
KIND = "govmap_geocode"
DATASET_SLUG = "govmap-geocode"
BATCH_SIZE = 10_000
# A miss is terminal, but not on a single word: GovMap can answer "no" for a
# transient reason of its own. Three separate refusals is a real absence.
MAX_ATTEMPTS = 3

_DDL = f"""
CREATE TABLE IF NOT EXISTS public.{_qi(GEOCODE_TABLE)} (
    address_key   text PRIMARY KEY,
    status        text NOT NULL,          -- 'hit' | 'not_found'
    lat           double precision,
    lon           double precision,
    govmap_id     text,
    matched_text  text,
    score         double precision,
    attempts      integer NOT NULL DEFAULT 1,
    merged        boolean NOT NULL DEFAULT false,
    fetched_at    timestamptz DEFAULT now()
)
"""
_INDEXES = [
    f"CREATE INDEX IF NOT EXISTS {_qi(GEOCODE_TABLE + '_status_idx')} "
    f"ON public.{_qi(GEOCODE_TABLE)} (status)",
    f"CREATE INDEX IF NOT EXISTS {_qi(GEOCODE_TABLE + '_unmerged_idx')} "
    f"ON public.{_qi(GEOCODE_TABLE)} (merged) WHERE status = 'hit'",
]


async def ensure_tables() -> None:
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        await conn.execute(_DDL)
        for stmt in _INDEXES:
            await conn.execute(stmt)
    from app.services.nadlan_index import _grant_readonly
    await _grant_readonly()


# ── the queue side (main DB) ──────────────────────────────────────────────────
async def ensure_dataset(db) -> TrackedDataset:
    """The tracked_dataset the geocoding tasks hang off.

    A ScrapeTask needs one (the FK is NOT NULL) and the worker dispatches on
    ``scraper_config["kind"]`` — so this row IS the switch that turns the
    feature on. The worker ships inert until it exists."""
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name == DATASET_SLUG)
    )).scalar_one_or_none()
    if ds is not None:
        return ds
    ds = TrackedDataset(
        ckan_id=f"over-{DATASET_SLUG}",
        ckan_name=DATASET_SLUG,
        title='נדל"ן לעם — גיאוקודינג כתובות מול GovMap',
        organization="govmap.gov.il",
        source_type="scraper",
        source_url="https://www.govmap.gov.il/",
        scraper_config={
            "kind": KIND,
            # Read by the worker. 2/s was chosen deliberately over the ~4.4/s
            # measured: this is an internal, unannounced endpoint and the point
            # is not to lean on it.
            "rate_per_second": 2.0,
            "max_results": 5,
            # Off on purpose — the real guard against a wrong match is the
            # locality check in merge_into_addresses(), which rejects a point
            # that lands outside the address's own town. A score threshold set
            # from 40 samples would be a guess.
            "min_score": 0,
        },
    )
    db.add(ds)
    await db.commit()
    await db.refresh(ds)
    logger.info("geocode: created tracked dataset %s", ds.id)
    return ds


async def pending_task(db) -> ScrapeTask | None:
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name == DATASET_SLUG)
    )).scalar_one_or_none()
    if ds is None:
        return None
    return (await db.execute(
        select(ScrapeTask)
        .where(ScrapeTask.tracked_dataset_id == ds.id,
               ScrapeTask.status.in_(("pending", "running")))
        .limit(1)
    )).scalar_one_or_none()


async def enqueue_next_batch(db, *, size: int = BATCH_SIZE) -> dict:
    """Queue ONE batch, and only if none is outstanding.

    One at a time on purpose: the queue is shared, and a thousand queued
    geocoding rows would bury every other job's visibility even though the
    priority band keeps them from being *claimed* first."""
    remaining = await remaining_count()
    if not remaining:
        return {"queued": False, "reason": "nothing left to geocode", "remaining": 0}
    existing = await pending_task(db)
    if existing is not None:
        return {"queued": False, "reason": f"batch {existing.id} still {existing.status}",
                "remaining": remaining}
    ds = await ensure_dataset(db)
    task = ScrapeTask(
        tracked_dataset_id=ds.id, status="pending", priority=PRIORITY_GEOCODE,
        phase="queued", params={"batch_size": size},
        message=f"גיאוקודינג GovMap — עד {size:,} כתובות ({remaining:,} נותרו)"[:500],
    )
    db.add(task)
    await db.commit()
    await db.refresh(task)
    logger.info("geocode: queued batch task %s (%d remaining)", task.id, remaining)
    return {"queued": True, "task_id": str(task.id), "size": size, "remaining": remaining}


# ── the data side (append DB) ─────────────────────────────────────────────────
def _selection_sql(limit: int) -> str:
    """Addresses still worth asking about.

    Re-evaluated on every call — no reservation, no stored batch (see the module
    docstring). Ordered by address_key so two concurrent readers would see the
    same head rather than interleaving, though only one batch is ever in flight.
    """
    return f"""
        SELECT a.address_key,
               a.settlement_name, a.street_name, a.house_num, a.house_suffix
        FROM public.{_qi(ADDRESSES_TABLE)} a
        LEFT JOIN public.{_qi(GEOCODE_TABLE)} g ON g.address_key = a.address_key
        WHERE a.point IS NULL
          AND a.street_name IS NOT NULL
          AND a.house_num IS NOT NULL
          AND a.settlement_name IS NOT NULL
          AND (g.address_key IS NULL
               OR (g.status NOT IN ('hit', 'wrong_locality')
                   AND g.attempts < {MAX_ATTEMPTS}))
        ORDER BY a.address_key
        LIMIT {int(limit)}
    """


def build_query(settlement: str | None, street: str | None,
                house: int | None, suffix: str | None) -> str:
    """The free-text the worker sends to GovMap: 'רחוב מספר יישוב'."""
    num = f"{house}{suffix or ''}" if house is not None else ""
    return " ".join(p for p in ((street or "").strip(), num, (settlement or "").strip()) if p)


async def batch_for_task(limit: int = BATCH_SIZE) -> list[dict]:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(_selection_sql(limit))
    return [{"address_key": r["address_key"],
             "query": build_query(r["settlement_name"], r["street_name"],
                                  r["house_num"], r["house_suffix"])}
            for r in rows]


async def remaining_count() -> int:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(f"""
            SELECT count(*) FROM public.{_qi(ADDRESSES_TABLE)} a
            LEFT JOIN public.{_qi(GEOCODE_TABLE)} g ON g.address_key = a.address_key
            WHERE a.point IS NULL AND a.street_name IS NOT NULL
              AND a.house_num IS NOT NULL AND a.settlement_name IS NOT NULL
              AND (g.address_key IS NULL
                   OR (g.status <> 'hit' AND g.attempts < {MAX_ATTEMPTS}))
        """) or 0


# A batch that asked this many addresses and got essentially nothing was not
# looking at empty countryside — it was being refused. GovMap answers a SOFT
# block with HTTP 200 and an empty result list, which is byte-identical to "no
# such address"; only the RUN distinguishes them. Ground truth from sampling:
# even the worst slice (moshavim/kibbutzim, the head of ORDER BY address_key)
# returns ~60% hits, and cities ~58%.
#
# This cost 25,548 addresses on the first night: they reached attempts=3 and left
# the selection permanently, while every run reported success. The worker now
# aborts on a run of 100 (GOVSCRAPER e386785); this is the second line of
# defence, because the damage lands HERE and must be impossible to write.
_IMPLAUSIBLE_MIN_ASKED = 200
_IMPLAUSIBLE_HIT_RATE = 0.01

#: Addresses GovMap is known to resolve. If these come back empty the endpoint
#: is refusing us; if they resolve, a batch of misses is simply a batch of
#: misses. This replaced a hit-rate threshold, which cannot tell the two apart:
#: measured per-locality rates run 6.9% (קריית שמונה) to 64.4% (מזכרת בתיה),
#: so any threshold high enough to catch a block also stalls the periphery.
CANARIES = ("אבימלך 8 פתח תקווה", "הרצל 1 תל אביב", "דיזנגוף 100 תל אביב יפו")


async def govmap_is_answering() -> bool | None:
    """Ask GovMap about addresses it certainly knows. None = could not tell.

    Three requests, only on a batch that already looks wrong, so this costs
    GovMap nothing in the normal case."""
    import uuid
    import httpx
    ok = 0
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            for q in CANARIES:
                r = await client.post(
                    "https://www.govmap.gov.il/api/search-service/autocomplete",
                    json={"searchText": q, "language": "he", "filterType": "address",
                          "isAccurate": True, "maxResults": 10},
                    headers={"Referer": "https://www.govmap.gov.il/",
                             "User-Agent": "Mozilla/5.0",
                             "x-fingerprint-id": str(uuid.uuid4()),
                             "x-user-id": str(uuid.uuid4()),
                             "x-trace-id": str(uuid.uuid4())})
                if r.status_code == 200 and (r.json().get("results") or []):
                    ok += 1
    except Exception as exc:                                  # network, shape, anything
        logger.warning("geocode: canary check failed to run: %s", exc)
        return None
    logger.info("geocode: canary %d/%d control addresses resolved", ok, len(CANARIES))
    return ok >= 2


def _looks_implausible(hits, not_found, payload) -> str | None:
    """Whether a payload is worth spending three canary requests on.

    Deliberately narrow. GovMap's address index is sparse at the house-number
    level outside the metro core — "חטיבת הנגב 10 שדרות" returns nothing while
    "חטיבת הנגב שדרות" returns 2,184 — so a low hit rate is normal and only a
    near-total zero over a large ask is odd enough to question."""
    asked = hits + not_found
    if asked < _IMPLAUSIBLE_MIN_ASKED:
        return None
    if hits == 0:
        return f"{asked} addresses asked, zero found"
    if hits / asked < _IMPLAUSIBLE_HIT_RATE:
        return f"{hits}/{asked} found ({100 * hits / asked:.1f}%)"
    return None


async def record_results(payload: dict) -> dict:
    """Persist one batch's outcome.

    ``failed`` is deliberately NOT written: the worker could not ask, so the
    address must reappear in the next selection. Writing it would make an
    available address look like a settled miss.

    Two further protections against burning an address's limited attempts on an
    answer GovMap never really gave:

    * a payload with almost no hits is checked against GovMap directly, using
      addresses it is known to resolve, and only quarantined if those control
      addresses fail too — a low hit rate on its own is normal;
    * an ``aborted`` payload never increments ``attempts``, because the run that
      triggered the abort is exactly the run whose answers are least trustworthy.
    """
    results = payload.get("results") or []
    # not_found and misses are the same list; the worker sends both for
    # backwards compatibility. Union them so either spelling works.
    not_found = list({*(payload.get("not_found") or []), *(payload.get("misses") or [])})
    failed = payload.get("failed") or []
    aborted = bool(payload.get("aborted"))

    quarantine = _looks_implausible(len(results), len(not_found), payload)
    if quarantine:
        # Suspicion is not evidence. Ask GovMap about addresses it is known to
        # answer: if it answers them, this batch really did miss.
        answering = await govmap_is_answering()
        if answering:
            logger.info("geocode: batch looked odd (%s) but GovMap answers its "
                        "control addresses — recording the misses as real", quarantine)
            quarantine = None
        elif answering is None:
            logger.warning("geocode: batch looked odd (%s) and the canary could not "
                           "run — requeuing rather than guessing", quarantine)
    if quarantine:
        logger.error(
            "geocode: QUARANTINED a batch — %s, and GovMap failed its own control "
            "addresses. Treating its %d misses as failed (they return to the "
            "queue) rather than recording them. samples=%s",
            quarantine, len(not_found), (payload.get("samples") or [])[:5])
        failed = list(failed) + [{"address_key": k, "reason": f"quarantined: {quarantine}"}
                                 for k in not_found]
        not_found = []

    await ensure_tables()
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            if results:
                await conn.executemany(
                    f"""INSERT INTO public.{_qi(GEOCODE_TABLE)}
                        (address_key, status, lat, lon, govmap_id, matched_text, score,
                         attempts, merged, fetched_at)
                        VALUES ($1,'hit',$2,$3,$4,$5,$6,1,false,now())
                        ON CONFLICT (address_key) DO UPDATE SET
                          status='hit', lat=EXCLUDED.lat, lon=EXCLUDED.lon,
                          govmap_id=EXCLUDED.govmap_id, matched_text=EXCLUDED.matched_text,
                          score=EXCLUDED.score, merged=false, fetched_at=now()""",
                    [(r.get("address_key"), r.get("lat"), r.get("lon"), r.get("govmap_id"),
                      r.get("matched_text"), r.get("score")) for r in results
                     if r.get("address_key") and r.get("lat") is not None])
            if not_found:
                # attempts+1 so a repeated refusal eventually becomes terminal
                # (MAX_ATTEMPTS) instead of cycling through every batch forever.
                await conn.executemany(
                    f"""INSERT INTO public.{_qi(GEOCODE_TABLE)}
                        (address_key, status, attempts, fetched_at)
                        VALUES ($1,'not_found',$2,now())
                        ON CONFLICT (address_key) DO UPDATE SET
                          status='not_found',
                          -- An aborted batch's answers are the least trustworthy
                          -- ones there are; do not spend an attempt on them.
                          attempts = public.over_re_geocode.attempts + $2,
                          fetched_at=now()""",
                    [(k, 0 if aborted else 1) for k in not_found if k])
    out = {"recorded_hits": len(results), "recorded_not_found": len(not_found),
           "requeued_failed": len(failed), "quarantined": quarantine,
           "attempted": payload.get("attempted"), "batch_size": payload.get("batch_size"),
           "aborted": bool(payload.get("aborted")),
           "abort_reason": payload.get("abort_reason")}
    logger.info("geocode: batch recorded %s", out)
    return out


async def reset_misses(*, only_burned: bool = False) -> dict:
    """Give back every address a soft block retired.

    On the first night 30,450 misses were recorded, 25,548 of them at
    ``attempts = 3`` — permanently out of the selection — while GovMap was
    answering HTTP 200 with an empty list. The hourly hit rate ran 100% → 6% →
    64% → 15% with the first two hours showing ZERO misses, so a real absence
    cannot be told from a refusal after the fact.

    The asymmetry decides it: re-asking an address that genuinely is not in
    GovMap costs one query, while leaving a false miss in place costs that
    address permanently. So the misses are deleted rather than sifted, and the
    addresses simply return to ``point IS NULL``.

    Hits are never touched — they are evidence, not guesses."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        where = "status = 'not_found'" + (" AND attempts >= 3" if only_burned else "")
        tag = await conn.execute(
            f"DELETE FROM public.{_qi(GEOCODE_TABLE)} WHERE {where}", timeout=600)
    freed = int(str(tag).rsplit(" ", 1)[-1]) if tag else 0
    logger.warning("geocode: reset %d recorded misses back into the work list", freed)
    return {"reset": freed, "only_burned": only_burned,
            "remaining": await remaining_count()}


async def recent_hit_rate(hours: int = 2) -> float | None:
    """Hit rate over the last N hours, or None if too little to judge.

    The circuit breaker: a fresh batch is pointless while GovMap is refusing
    us, and every one queued in that state is another chance to burn attempts."""
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            SELECT count(*) FILTER (WHERE status='hit')       AS hits,
                   count(*)                                    AS total
            FROM public.{_qi(GEOCODE_TABLE)}
            WHERE fetched_at > now() - make_interval(hours => {int(hours)})
        """)
    if not row or (row["total"] or 0) < 200:
        return None
    return (row["hits"] or 0) / row["total"]


async def merge_into_addresses() -> dict:
    """Fill missing points from the geocoder — never overwrite an existing one.

    A geocoded point is accepted only if it lands within 3 km of a parcel in the
    address's OWN locality. This is not a formality: asked for גדרה, GovMap
    answers חדרה — one letter apart, 69 km away — with full confidence and a
    high score. 196 of those were caught in one locality alone. No score
    threshold separates them; locality does.

    Two cases the first version got wrong, both fixed here:

    * a locality with **no parcels at all** (אחוזת ברק) could never satisfy the
      guard, so every point in it was rejected for lack of anything to check
      against. Absence of evidence is not evidence — those are accepted.
    * a rejected point stayed ``status='hit', merged=false`` forever: it could
      not be used, could not be retried, and still counted as a hit. It now
      goes to the terminal status ``wrong_locality``, which the selection
      excludes — re-asking is pointless, GovMap will answer חדרה again.
    """
    await ensure_tables()
    pool = await append_store.get_pool()
    from app.services.nadlan_index import GEOM_SRID, PARCELS_TABLE, PG_EXT_SCHEMA
    x = _qi(PG_EXT_SCHEMA)
    point = (f"{x}.ST_SetSRID({x}.ST_MakePoint(c.lon, c.lat), {GEOM_SRID})"
             f"::{x}.geography")
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"""
                CREATE TEMP TABLE _geo_judged ON COMMIT DROP AS
                WITH cand AS (
                  SELECT g.address_key, g.lat, g.lon, a.settlement_code
                  FROM public.{_qi(GEOCODE_TABLE)} g
                  JOIN public.{_qi(ADDRESSES_TABLE)} a USING (address_key)
                  WHERE g.status = 'hit' AND NOT g.merged AND a.point IS NULL
                )
                SELECT c.address_key, c.lat, c.lon,
                       EXISTS (SELECT 1 FROM public.{_qi(PARCELS_TABLE)} p
                                WHERE p.settlement_code = c.settlement_code)
                         AS checkable,
                       EXISTS (SELECT 1 FROM public.{_qi(PARCELS_TABLE)} p
                                WHERE p.settlement_code = c.settlement_code
                                  AND {x}.ST_DWithin(
                                        p.centroid::{x}.geography, {point}, 3000))
                         AS near
                FROM cand c
                WHERE c.settlement_code IS NOT NULL
                UNION ALL
                SELECT c.address_key, c.lat, c.lon, false, true
                FROM cand c WHERE c.settlement_code IS NULL
            """, timeout=1800)
            # Accepted: near a parcel of its own locality, or nothing to check.
            tag = await conn.execute(f"""
                UPDATE public.{_qi(ADDRESSES_TABLE)} a
                SET lat = j.lat, lon = j.lon,
                    point = {x}.ST_SetSRID({x}.ST_MakePoint(j.lon, j.lat), {GEOM_SRID})
                FROM _geo_judged j
                WHERE j.address_key = a.address_key AND a.point IS NULL
                  AND (j.near OR NOT j.checkable)
            """, timeout=1800)
            merged = int(str(tag).rsplit(" ", 1)[-1]) if tag else 0
            # Rejected: we could check, and it was somewhere else entirely.
            tag = await conn.execute(f"""
                UPDATE public.{_qi(GEOCODE_TABLE)} g SET status = 'wrong_locality'
                FROM _geo_judged j
                WHERE j.address_key = g.address_key
                  AND j.checkable AND NOT j.near
            """)
            rejected = int(str(tag).rsplit(" ", 1)[-1]) if tag else 0
            await conn.execute(
                f"""UPDATE public.{_qi(GEOCODE_TABLE)} g SET merged = true
                    FROM public.{_qi(ADDRESSES_TABLE)} a
                    WHERE a.address_key = g.address_key
                      AND g.status = 'hit' AND a.point IS NOT NULL""")
    from app.services import nadlan_query
    nadlan_query.invalidate_stats_cache()
    if rejected:
        logger.info("geocode: %d points rejected as wrong locality (terminal)", rejected)
    return {"merged": merged, "rejected_outside_locality": rejected}


async def stats() -> dict:
    pool = await append_store.get_readonly_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            SELECT
              (SELECT count(*) FROM public.{_qi(GEOCODE_TABLE)} WHERE status='hit')       AS hits,
              (SELECT count(*) FROM public.{_qi(GEOCODE_TABLE)} WHERE status='not_found') AS not_found,
              (SELECT count(*) FROM public.{_qi(GEOCODE_TABLE)}
                 WHERE status='hit' AND NOT merged)                                       AS unmerged,
              (SELECT count(*) FROM public.{_qi(GEOCODE_TABLE)}
                 WHERE status='wrong_locality')                                           AS wrong_locality
        """)
    return {**dict(row or {}), "remaining": await remaining_count()}
