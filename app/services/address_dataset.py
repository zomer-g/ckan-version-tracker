"""נדל"ן לעם — publishing the address spine as a tracked dataset.

``over_re_addresses`` is OVER's own product, not a copy of somebody's file: it
merges the official address register, Israel Post's street file and our GovMap
geocoding into one row per doorway, and since 2026-09-20 it carries the grid the
country actually uses (``itm_x``/``itm_y``, EPSG:6991) next to WGS84. Until now
it existed only as a console table — queryable, but with no page, no version
history, no download and nothing archiving it over time. This module gives it
those, as an ordinary tracked dataset.

Why a count gate and not a schedule
-----------------------------------
The table passes through states that look finished and are not, and both of them
would publish a version that is quietly, plausibly wrong:

* ``build_addresses`` TRUNCATEs the spine and rebuilds it from the two source
  files alone — **357,679 points, versus 451,667 once the geocoded ones are
  folded back in.** A version caught there is 20.8% short and looks healthy.
* Only ``pip`` writes ``parcel_key``, and it can only link the points that exist
  when it runs. ``merge_into_addresses`` runs on its own 15-minute tick,
  independent of the build — so even "after pip" is not enough: a build that
  finishes pip before the next merge tick leaves 357,679 points AND links for
  only those.

Neither state is visible from the stage names — both report ``ok``. What
separates them from a finished corpus is the row count, so that is what the
gate reads. A snapshot publishes only when ``count(point)`` is at or above the
high-water mark of the previous version; otherwise it declines, says why, and
leaves the next tick to try again. This is the same failure family as
``mevaker``'s frozen enumeration: a shrink that arrives as a successful run.
"""
from __future__ import annotations

import csv
import logging
import os
import tempfile
import uuid as _uuid
from datetime import datetime, timezone

from sqlalchemy import select

from app.models.tracked_dataset import TrackedDataset
from app.models.version_index import VersionIndex
from app.services import append_store
from app.services.nadlan_index import ADDRESSES_TABLE

logger = logging.getLogger(__name__)

#: The dataset's stable name. Unlike ``govmap-geocode`` — the geocoding ledger,
#: which is deliberately ``status='hidden'`` because it is a work log — this one
#: is meant to be found, so it is created active.
DATASET_SLUG = "over-re-addresses"
TITLE = 'נדל"ן לעם — כתובות ישראל עם נצ ברשת ישראל ו-WGS84'
ORGANIZATION = "גרסאות לעם"

#: The published columns, in order. ``point`` itself is left out: it is a
#: PostGIS blob that no spreadsheet can read, and everything it carries is
#: already here twice, in both grids.
COLUMNS = [
    "address_key", "settlement_code", "settlement_name",
    "street_key", "street_name", "house_num", "house_suffix", "entrance",
    "zip7", "zip5", "zip_level", "neighbourhood", "district",
    "lat", "lon", "itm_x", "itm_y",
    "point_source", "parcel_key", "parcel_match",
    "in_postal", "in_address_list",
]

#: Columns that need an expression rather than a bare name. Rounding is not
#: cosmetic: ``itm_x`` comes out of ST_Transform with 14 decimals — nanometres
#: — on a coordinate whose real accuracy is centimetres at best (the register's
#: own values carry 8) and ~1.5 m for the GovMap fifth. Publishing the full
#: float would state a precision the data does not have. 2 decimals is a
#: centimetre; 7 decimal degrees is about the same.
SELECT_EXPR = {
    "lat": 'round("lat"::numeric, 7) AS "lat"',
    "lon": 'round("lon"::numeric, 7) AS "lon"',
    "itm_x": 'round("itm_x"::numeric, 2) AS "itm_x"',
    "itm_y": 'round("itm_y"::numeric, 2) AS "itm_y"',
}

#: A version must not lose points. Expressed as a ratio rather than an equality
#: because a legitimate rebuild can move the count by a handful either way (the
#: register is re-read, a few doorways merge or split); 357,679/451,667 = 0.792,
#: so anything near the register-only floor is caught with room to spare.
MIN_POINT_RATIO = 0.98

#: After this many consecutive refusals the gate stops being a quiet guard and
#: starts being a reported problem.
#:
#: Without it this is a high-water mark, and a high-water mark has no way down:
#: a LEGITIMATE permanent shrink — a locality dropped from the address list, a
#: dedupe, a source correction — would freeze the dataset for good, and the
#: freeze would be indistinguishable from "nothing has changed since the last
#: version". That is the exact failure this gate was written to prevent,
#: pointed the other way, and it is how mevaker published healthy weekly
#: versions for years while frozen at 2019. A refusal is therefore never
#: silent: each one is written to ``last_error``, and a run of them raises
#: ``import_warning``, which the dataset page shows to readers. A human then
#: decides whether the corpus really shrank and passes ``force``.
REFUSALS_BEFORE_WARNING = 3


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def ensure_dataset(db) -> TrackedDataset:
    """Find or create the TrackedDataset. Idempotent; safe on every startup."""
    ds = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.ckan_name == DATASET_SLUG)
    )).scalar_one_or_none()
    if ds is not None:
        # Backfill for the row that already exists in production: it was
        # created before push_mode was declared below, and an early return
        # would leave it exposed to the worker loop forever.
        cfg = dict(ds.scraper_config or {})
        if cfg.get("push_mode") != "external":
            cfg["push_mode"] = "external"
            ds.scraper_config = cfg
            await db.flush()
            logger.info("address_dataset: declared %s externally pushed",
                        DATASET_SLUG)
        return ds
    ds = TrackedDataset(
        id=_uuid.uuid4(),
        ckan_id=DATASET_SLUG,
        ckan_name=DATASET_SLUG,
        title=TITLE,
        organization=ORGANIZATION,
        # 'scraper', not 'govmap'. The corpus is 79.2% the official register's
        # own ITM and 20.8% GovMap geocoding (measured, `point_source`), so
        # filing the whole thing under GovMap would misattribute four fifths of
        # it — and the GovMap fifth is the less accurate one.
        source_type="scraper",
        source_url="https://www.over.org.il/data?table=over_re_addresses",
        # Nothing polls this: it is produced here, from our own tables. The
        # snapshot job decides when a version exists, and the gate below is
        # what "when" means.
        is_active=True,
        status="active",
        poll_interval=0,
        storage_mode="full_snapshot",
        # push_mode=external: no worker in the fleet can produce this — it is
        # built here from our own tables — so the scrape-task loop must step
        # aside (TrackedDataset.is_externally_pushed). Without it every poll
        # queued a task that a worker claimed and failed with "no engine for
        # kind='over_internal'".
        scraper_config={"kind": "over_internal", "storage_backend": "r2",
                        "push_mode": "external"},
    )
    db.add(ds)
    await db.flush()
    logger.info("address_dataset: created tracked dataset %s (%s)",
                DATASET_SLUG, ds.id)
    return ds


async def current_counts() -> dict:
    """What the table holds right now — the numbers the gate reasons about."""
    pool = await append_store.get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(f"""
            SELECT count(*)                                        AS rows,
                   count(point)                                    AS with_point,
                   count(*) FILTER (WHERE point_source = 'govmap') AS govmap,
                   count(*) FILTER (WHERE point_source = 'address_register')
                                                                   AS register,
                   count(parcel_key)                               AS with_parcel,
                   -- The two reasons a row has no parcel, kept apart because
                   -- they mean different things to a reader: no point at all
                   -- (point-in-polygon was never possible) versus a point that
                   -- falls outside every parcel polygon.
                   count(*) FILTER (WHERE parcel_key IS NULL AND point IS NULL)
                                                                   AS no_parcel_no_point,
                   count(*) FILTER (WHERE parcel_key IS NULL AND point IS NOT NULL)
                                                                   AS no_parcel_has_point
            FROM public."{ADDRESSES_TABLE}"
        """)
    return dict(row)


def _previous_counts(latest: VersionIndex | None) -> dict:
    if latest is None or not latest.change_summary:
        return {}
    return (latest.change_summary or {}).get("counts") or {}


def gate(counts: dict, previous: dict) -> str | None:
    """Why this snapshot must NOT be published, or None if it may be.

    Reads the row counts, never the stage names — see the module docstring for
    why the stages cannot see the two states this exists to catch.
    """
    if not counts.get("rows"):
        return "the address table is empty"
    if not counts.get("with_point"):
        return "no address carries a coordinate"
    prev_points = previous.get("with_point") or 0
    if prev_points:
        floor = int(prev_points * MIN_POINT_RATIO)
        if counts["with_point"] < floor:
            return (
                f"only {counts['with_point']:,} of the previous version's "
                f"{prev_points:,} points are present (floor {floor:,}) — this "
                f"is what a rebuild looks like before the geocoded points are "
                f"merged back, so the version is being withheld rather than "
                f"published 20% short"
            )
    prev_parcels = previous.get("with_parcel") or 0
    if prev_parcels and counts.get("with_parcel", 0) < int(prev_parcels * MIN_POINT_RATIO):
        return (
            f"only {counts.get('with_parcel', 0):,} of the previous version's "
            f"{prev_parcels:,} parcel links are present — `pip` has not re-run "
            f"since the last rebuild"
        )
    return None


async def export_csv(path: str) -> int:
    """Stream the table to ``path`` as UTF-8-BOM CSV. Returns the row count.

    Streamed, not accumulated: the corpus is ~618k rows / ~70 MB and this runs
    on a 2 GB dyno that already peaks at ~670 MB loading the parcels register.
    The BOM is what makes Hebrew open correctly in Excel, which is where most
    of the people who will download this actually read it.
    """
    pool = await append_store.get_pool()
    cols = ", ".join(SELECT_EXPR.get(c, f'"{c}"') for c in COLUMNS)
    written = 0
    with open(path, "w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(COLUMNS)
        async with pool.acquire() as conn:
            async with conn.transaction():
                async for rec in conn.cursor(
                    f'SELECT {cols} FROM public."{ADDRESSES_TABLE}" '
                    f'ORDER BY address_key'
                ):
                    writer.writerow(["" if v is None else v for v in rec])
                    written += 1
    return written


def _he_date() -> str:
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Asia/Jerusalem")).strftime("%d.%m.%Y")
    except Exception:  # noqa: BLE001
        return datetime.now(timezone.utc).strftime("%d.%m.%Y")


async def _record_refusal(db, ds, why: str, counts: dict) -> dict:
    """Write the refusal down, and escalate a run of them into a visible warning.

    This is the half that keeps the gate from becoming the thing it guards
    against. A strict floor with a silent refusal freezes the dataset the first
    time the corpus legitimately shrinks, and the freeze reads exactly like
    "nothing has changed" — which is how a register can publish healthy-looking
    versions for years while standing still. So: every refusal lands in
    ``last_error``, and once there have been ``REFUSALS_BEFORE_WARNING`` in a
    row the dataset raises ``import_warning``, which the dataset page shows to
    readers. Nobody has to go looking.
    """
    sc = dict(ds.scraper_config or {})
    streak = int(sc.get("refusal_streak") or 0) + 1
    sc["refusal_streak"] = streak
    sc["last_refusal"] = why
    ds.scraper_config = sc
    ds.last_error = f"הגרסה נמנעה ({streak}): {why}"[:2000]
    if streak >= REFUSALS_BEFORE_WARNING:
        ds.import_warning = (
            f"לא פורסמה גרסה חדשה ב-{streak} ניסיונות רצופים. הסיבה: {why}. "
            f"אם המאגר באמת התכווץ — יש לאשר פרסום ידנית; אחרת המאגר קפוא "
            f"והגרסה האחרונה אינה משקפת את המצב הנוכחי."
        )[:2000]
        ds.import_warning_at = datetime.now(timezone.utc)
        logger.error("address_dataset: %d consecutive refusals — %s", streak, why)
    else:
        logger.warning("address_dataset: not publishing (%d) — %s", streak, why)
    await db.flush()
    return {"published": False, "reason": why, "counts": counts,
            "refusal_streak": streak}


async def snapshot(db, *, force: bool = False) -> dict:
    """Publish one version of the address spine, or explain why it did not.

    ``force`` bypasses the count gate. It exists for the case where the corpus
    legitimately shrank and a human has looked at it — never for automation.
    """
    # The app-side publish pattern (delta_archiver / conditional_archiver /
    # r2_backfill): bytes → storage_client → a VersionIndex written here.
    # Deliberately NOT snapshot_service.create_version_snapshot, whose
    # signature is coupled to the poll loop it is the upload half of, and not
    # /api/worker/push-version either — that exists for publishers OUTSIDE the
    # worker fleet, and this table is produced inside this very process.
    from app.services import storage_client as _storage
    from app.services.storage_client import storage_client as _r2

    ds = await ensure_dataset(db)
    counts = await current_counts()
    latest = (await db.execute(
        select(VersionIndex)
        .where(VersionIndex.tracked_dataset_id == ds.id)
        .order_by(VersionIndex.version_number.desc()).limit(1)
    )).scalar_one_or_none()
    previous = _previous_counts(latest)

    refusal = None if force else gate(counts, previous)
    if refusal:
        return await _record_refusal(db, ds, refusal, counts)

    if not _r2.is_configured():
        return {"published": False,
                "reason": "R2 is not configured — nowhere to put the snapshot",
                "counts": counts}

    next_version = (latest.version_number + 1) if latest else 1
    resource_id = f"{DATASET_SLUG}-csv"
    fd, tmp = tempfile.mkstemp(prefix="over_re_addresses_", suffix=".csv")
    os.close(fd)
    try:
        rows = await export_csv(tmp)
        size = os.path.getsize(tmp)
        key = _storage.build_key(str(ds.id), next_version, "addresses.csv")
        await _r2.upload_object(key, file_path=tmp,
                                content_type="text/csv; charset=utf-8")
        mappings = {resource_id: _storage.mark(key),
                    "_names": {resource_id: f"כתובות ישראל — {_he_date()}"}}
    except Exception as e:  # noqa: BLE001
        # No VersionIndex row is written, so nothing claims an archive that
        # does not exist — the next tick simply tries again.
        msg = f"הצילום נכשל: {type(e).__name__}: {e}"[:2000]
        logger.exception("address_dataset: snapshot failed")
        ds.last_error = msg
        await db.flush()
        return {"published": False, "reason": msg, "counts": counts}
    finally:
        try:
            os.unlink(tmp)
        except OSError:
            pass

    version = VersionIndex(
        id=_uuid.uuid4(),
        tracked_dataset_id=ds.id,
        version_number=next_version,
        metadata_modified=_utc_stamp(),
        resource_mappings=mappings,
        source="internal",
        change_summary={
            "type": "over_internal",
            "counts": counts,
            "rows_written": rows,
            "bytes": size,
            "previous_counts": previous or None,
            # Stated, not left to be discovered. `parcel_key` is NULL on 30.4%
            # of the rows (2026-09-20), and a consumer reading it as "the
            # parcel this address is in" will otherwise meet that absence one
            # row at a time. The column is honestly NULL; the rate is the part
            # that belongs in the version, next to the number it qualifies.
            "note": (
                f"{counts['with_point']:,} של-{counts['rows']:,} הכתובות נושאות "
                f"נצ — {counts['register']:,} ממרשם הכתובות ו-"
                f"{counts['govmap']:,} מגיאוקודינג מול GovMap. "
                f"ל-{counts['with_parcel']:,} יש שיוך לחלקה; "
                f"{counts.get('no_parcel_no_point', 0):,} כתובות ללא נצ כלל "
                f"(אי אפשר לשייך) ו-{counts.get('no_parcel_has_point', 0):,} "
                f"עם נצ שנופל מחוץ לכל פוליגון חלקה"
            ),
        },
    )
    db.add(version)
    ds.last_polled_at = datetime.now(timezone.utc)
    ds.last_error = None
    sc = dict(ds.scraper_config or {})
    sc.pop("refusal_streak", None)
    sc.pop("last_refusal", None)
    ds.scraper_config = sc
    # A version landed, so whatever was being withheld no longer is. Clearing
    # both is what stops a stale refusal warning outliving its cause.
    ds.import_warning = None
    ds.import_warning_at = None
    await db.flush()
    logger.info("address_dataset: published version %d (%d rows, %d points)",
                next_version, rows, counts["with_point"])
    return {"published": True, "version": next_version, "rows": rows,
            "counts": counts}
