"""Targeted re-sampling: turning "sample only X" into parameters for one run.

Most tracked sources have exactly one useful question — *what does it look like
now* — and one answer: poll the whole thing. A register that is large, slow to
read and changes in place has several, and they are not the same question:

    הכול         re-read every item (the honest full pass; hours)
    חדשים        only items the site has and OVER does not
    לפי סטטוס    only the items sitting at one status — "which of the plans at
                 ועדת המשנה moved?" — which is where the change actually is
    תיק בודד     one item, now, because someone asked about it

All four are the same scrape with a different target list, so none of them is a
new dataset or a new scraper: they are ``scrape_tasks.params``, merged over the
dataset's ``scraper_config`` for that run only (migration 047). This module is
the half that decides WHAT to target, and it decides it from the archive OVER
already holds — the only place that knows which items exist, what status each is
at, and how far the numbering had got last time anyone looked.

Enabled per dataset by a ``sampling`` block in ``scraper_config``, which a
source manifest ships in its ``default_config``::

    "sampling": {
        "modes": ["all", "new", "status", "item"],
        "item_key": "מספר תיק",      # the column that identifies an ITEM
        "status_column": "סטטוס",     # what "by status" filters on
        "sample_column": "תאריך דגימה",  # when THIS row was sampled
        "key_separator": "/",         # keys are <period>/<serial> → frontier walk
        "resource": "…",              # which table, for a multi-resource dataset
    }

A dataset without the block is not samplable this way and every endpoint here
answers 409 for it — the routine poll is unaffected, which is the point.

The archive is read as ITEMS, never as rows: an item's status is the status of
its most recent sample (append_store.latest_source). A file that was at
"נפתח תיק רישוי" in April and "נדונה בוועדת המשנה" in July is at the latter, and
asking for the former must not return it.
"""
from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.services import append_store, append_tables

logger = logging.getLogger(__name__)

# Every mode a dataset may declare. A dataset lists the subset it supports —
# "new" only means something for a source that can cheaply enumerate what it
# does not have yet, and that is a property of the site, not of OVER.
MODES = ("all", "new", "status", "item")

# What each mode is called where a human reads it (the admin panel, the
# activity log, a version's change_summary).
MODE_LABELS_HE = {
    "all": "כל הישויות במאגר",
    "new": "רק ישויות חדשות באתר",
    "status": "לפי סטטוס",
    "item": "תיק בודד",
}

# A "by status" run hands the worker a target list it pulls page by page. This
# caps how many items one run may target, so a status holding most of the
# register can't quietly turn a targeted sample into a full sweep under a label
# that says otherwise. Over the cap the run is REFUSED with the count, not
# silently truncated — a truncated target list reads as "all of them" (lessons:
# no silent caps).
MAX_TARGETS = 20000

# The cap above guards against a targeted run that is secretly a full sweep.
# It does not apply when the caller EXPLICITLY names a source dataset to take
# the keys from: there the full corpus is the point, and the size is stated in
# the summary rather than hidden. Still bounded, because a target list has to
# be pulled page by page by the worker.
MAX_TARGETS_FROM_DATASET = 250000

# How many key series a "what's new" walk looks at, when the source does not say.
#
# A frontier is one entry per series ever seen, and this register has 90 of them
# — one per year back to the 1930s. Walking all of them is not merely wasteful,
# it is walking the wrong thing: a series prefix is the year the item was OPENED,
# and the walk probes the ``.00`` suffix a brand-new item starts at, so a new
# item can only ever appear at the far end of the CURRENT series. The older 88
# spend ~25 misses each proving a year that closed decades ago is still closed.
#
# Two, not one, because the boundary is a date and runs land on both sides of
# it: through January a run must still see the tail of last year's numbering.
DEFAULT_NEW_SERIES_WINDOW = 2

# The modes a source may ask OVER to run on a cadence of its own, via
# ``sampling.schedule``. Only "new" so far, and the restriction is not
# arbitrary: "new" is fully specified by the archive (walk forward from the
# frontier), whereas "status" needs someone to say WHICH status — a selector
# this module has no basis to invent, and getting it wrong turns a cheap weekly
# run into a day-long one.
SCHEDULABLE_MODES = ("new",)


def sampling_spec(ds) -> dict | None:
    """The dataset's ``sampling`` block, or None if it declares none.

    The source's MANIFEST is the base and the dataset's stored config is layered
    over it, key by key. Both halves are load-bearing:

    * the manifest half is what makes this work on the datasets that already
      exist. A stored ``scraper_config`` is a snapshot taken when the dataset was
      created, so a manifest that later learns to declare something — a sampling
      block at all, a cadence, a narrower walk — would otherwise apply only to
      datasets created afterwards, and every existing one would need a config
      backfill. That is precisely the drift the declarative registry exists to
      avoid, and the datasets with history worth reading are the OLD ones.
    * the stored half still wins for every key it declares, so a per-dataset
      choice an admin made stays made.

    Resolution goes through the same matcher OVER classifies a pasted URL with,
    so a dataset gets the block its own corpus declares (the register's four
    modes, the documents index's two) rather than the source's default. The
    cache is warmed at startup and this is a pure lookup — no DB, so it is safe
    inside response serialization.
    """
    stored = (ds.scraper_config or {}).get("sampling")
    stored = stored if isinstance(stored, dict) else {}
    base = _manifest_spec(ds)
    spec = {**base, **stored} if isinstance(base, dict) else stored
    if not spec.get("item_key"):
        return None
    return spec


def _manifest_spec(ds) -> dict | None:
    """The ``sampling`` block this dataset's URL resolves to in the registry."""
    url = getattr(ds, "source_url", None)
    if not url or getattr(ds, "source_type", None) != "scraper":
        return None
    try:
        from app.services import source_registry

        match = source_registry.match_manifests(
            url, source_registry.cached_manifests())
    except Exception as e:  # noqa: BLE001 — never break a read path over this
        logger.debug("sampling spec lookup failed for %s: %s", url, e)
        return None
    if match is None:
        return None
    spec = (match.scraper_config or {}).get("sampling")
    return spec if isinstance(spec, dict) else None


def available_modes(ds) -> list[str]:
    spec = sampling_spec(ds) or {}
    declared = [m for m in (spec.get("modes") or MODES) if m in MODES]
    return declared or list(MODES)


async def resolve_table(ds, db: AsyncSession) -> str | None:
    """The physical table the sampling spec is about.

    A multi-resource dataset can declare ``resource``; otherwise the first
    table is used, which for these sources is the register itself (the
    documents / tabs tables are a different grain and are not what "sample by
    status" means)."""
    spec = sampling_spec(ds) or {}
    tables = await append_tables.resolve_tables(ds, db)
    if not tables:
        return None
    wanted = spec.get("resource")
    if wanted:
        for t in tables:
            if wanted in (t.get("table"), t.get("resource_id"), t.get("resource_name")):
                return t["table"]
    return tables[0]["table"]


async def options(ds, db: AsyncSession) -> dict:
    """What the admin panel needs to offer the four buttons: the modes this
    dataset supports, the statuses its items are actually at (with counts, so
    "לפי סטטוס" shows how much work each choice is), how far each key series has
    got, and which modes OVER is already running on a cadence of their own.

    That last part is not decoration. A mode that runs weekly by itself and a
    mode that only ever runs when someone clicks look identical in this panel
    otherwise, and the difference is the whole question "is this dataset keeping
    up?" — so the cadence and when it last fired are stated.
    """
    spec = sampling_spec(ds)
    if not spec:
        return {"enabled": False, "modes": []}
    table = await resolve_table(ds, db)
    out: dict = {
        "enabled": True,
        "modes": available_modes(ds),
        "mode_labels": {m: MODE_LABELS_HE[m] for m in available_modes(ds)},
        "item_key": spec.get("item_key"),
        "status_column": spec.get("status_column"),
        "sample_column": spec.get("sample_column"),
        "table": table,
        "statuses": [],
        "frontier": {},
        "max_targets": MAX_TARGETS,
        "schedule": {},
    }
    for mode in SCHEDULABLE_MODES:
        seconds = schedule_for(ds, mode)
        if not seconds:
            continue
        last = await last_run_at(db, ds.id, mode)
        out["schedule"][mode] = {
            "interval_seconds": seconds,
            "last_run_at": last.isoformat() if last else None,
        }
    if not table or not append_store.is_configured():
        return out
    try:
        if spec.get("status_column"):
            out["statuses"] = await append_store.latest_value_counts(
                table, key_col=spec["item_key"], value_col=spec["status_column"],
                order_col=spec.get("sample_column"),
            )
        if spec.get("key_separator"):
            out["frontier"] = await append_store.key_frontier(
                table, key_col=spec["item_key"], separator=spec["key_separator"])
    except Exception as e:  # noqa: BLE001 — the panel must render without the archive
        logger.warning("sampling options for %s: %s", ds.id, e)
        out["error"] = str(e)
    return out


def recent_series(frontier: dict, window=None) -> dict:
    """The last N series of a frontier — the only ones a new item can open in.

    "What's new" means "since the last time we looked", and the frontier already
    says that per series. What it does not say is that most of its series are
    dead: a key's prefix is the period the item was opened in, so the run that
    walks all 90 of a 90-year register spends 88 of them re-proving that 1974 is
    over. Trimming to the newest few is what keeps a weekly run at minutes
    rather than an hour, and nothing reachable is lost — a later request against
    an OLD item is a sub-key of that item (``1999/0448.12``), which the walk
    does not probe for in any case (it probes ``.00``, what a NEW item opens
    with) and which a re-sample of the item itself is the honest way to catch.

    ``window=0`` means "every series", for a source whose numbering genuinely
    does not retire.
    """
    try:
        n = DEFAULT_NEW_SERIES_WINDOW if window is None else int(window)
    except (TypeError, ValueError):
        n = DEFAULT_NEW_SERIES_WINDOW
    frontier = {str(k): v for k, v in (frontier or {}).items() if k}
    if n <= 0 or len(frontier) <= n:
        return frontier
    # Lexicographic on a zero-padded period prefix is chronological, which is
    # the same assumption key_frontier's max() already rests on.
    return {k: frontier[k] for k in sorted(frontier, reverse=True)[:n]}


def schedule_for(ds, mode: str) -> int | None:
    """Seconds between automatic runs of ``mode``, if the source asks for any.

    Declared by the source, not by OVER: ``sampling.schedule = {"new": 604800}``
    on the corpus that wants it. A source that says nothing is polled exactly as
    it was — this adds a cadence, it never changes the existing one.
    """
    if mode not in SCHEDULABLE_MODES:
        return None
    sched = (sampling_spec(ds) or {}).get("schedule")
    if not isinstance(sched, dict):
        return None
    try:
        seconds = int(sched.get(mode))
    except (TypeError, ValueError):
        return None
    return seconds if seconds > 0 else None


async def last_run_at(db: AsyncSession, dataset_id, mode: str):
    """When a run of ``mode`` was last QUEUED for this dataset, or None.

    Read off the task queue rather than a column of its own: the task is where a
    run's mode is already recorded (migration 047), it is never pruned, and it
    survives a restart — so the cadence cannot be reset by a deploy, which is
    the failure mode a weekly schedule would show up as monthly.

    Queued, not completed, is deliberate: a run that failed still consumed its
    slot, and re-firing a 40-minute walk every tick because it errored once is
    worse than waiting for the next one.
    """
    from app.models.scrape_task import ScrapeTask

    return (await db.execute(
        select(ScrapeTask.created_at)
        .where(ScrapeTask.tracked_dataset_id == dataset_id,
               ScrapeTask.params["run_mode"].astext == mode)
        .order_by(ScrapeTask.created_at.desc())
        .limit(1)
    )).scalar_one_or_none()


class SamplingError(ValueError):
    """A run that can't be built — unknown mode, missing argument, too big."""


class SamplingBusy(SamplingError):
    """This dataset already has a run in flight.

    Separate from the errors above because it is not a bad request: nothing is
    wrong with what was asked, it is only the wrong moment. A caller that can
    wait (the scheduler) skips a tick; one that cannot (an admin watching) is
    told to wait rather than having a scrape already in flight re-aimed under it
    — that would publish a version labelled as something it did not read.
    """


async def _targets_from_dataset(
    db: AsyncSession, source_id: str, status: str | None, status_column: str | None,
) -> tuple[str, int, str]:
    """``(dataset_id, count, title)`` for a run that reads ANOTHER dataset's items.

    Two corpora of one source can share their item list. Jerusalem's documents
    index is one document per row, but the items it must READ are building
    files — the register's grain — and the register already holds all ~100k of
    their numbers. Without this the documents run re-discovers them from
    scratch: ~15 hours of sweeping to rebuild a list that is already stored.

    The status filter, when given, applies to the SOURCE dataset — that is the
    only dataset where the status lives. Asking for "the documents of every file
    at ועדת המשנה" is therefore one run, not a manual list.
    """
    from app.models.tracked_dataset import TrackedDataset

    uid = _parse_uuid(source_id)
    src = (await db.execute(
        select(TrackedDataset).where(TrackedDataset.id == uid)
    )).scalar_one_or_none()
    if src is None:
        raise SamplingError(f"מאגר המקור לרשימת היעדים לא נמצא: {source_id}")
    src_spec = sampling_spec(src)
    if not src_spec:
        raise SamplingError(
            f"מאגר המקור ({src.title}) לא מגדיר מפתח ישות, ולכן אין ממנו רשימת יעדים")
    table = await resolve_table(src, db)
    if not table:
        raise SamplingError(f"למאגר המקור ({src.title}) אין עדיין טבלת ארכיון")
    _keys, total = await append_store.latest_item_keys(
        table, key_col=src_spec["item_key"], order_col=src_spec.get("sample_column"),
        value_col=(status_column or src_spec.get("status_column")) if status else None,
        value=status, limit=1,
    )
    if total == 0:
        raise SamplingError(f"מאגר המקור ({src.title}) לא מחזיק ישויות תואמות")
    if total > MAX_TARGETS_FROM_DATASET:
        raise SamplingError(
            f"{total:,} ישויות במאגר המקור — מעל התקרה של "
            f"{MAX_TARGETS_FROM_DATASET:,} לרשימת יעדים")
    return str(src.id), total, src.title


def _parse_uuid(value: str):
    import uuid as _uuid
    try:
        return _uuid.UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        raise SamplingError(f"מזהה מאגר לא תקין: {value}")


async def build_params(
    ds, db: AsyncSession, *, mode: str, status: str | None = None,
    item: str | None = None, targets_from: str | None = None,
) -> tuple[dict, str]:
    """``(params, summary_he)`` for one targeted run.

    The params land on the scrape task and are merged over the dataset's
    ``scraper_config`` when the worker claims it, so what reaches the scraper is
    its normal config plus ``run_*`` keys. Everything here is small on purpose:
    a target LIST is not embedded (a status can hold tens of thousands of items)
    — the worker pulls it from ``/api/worker/dataset/{id}/keys``, which reads
    the same latest-sample-per-item view this module does, so the two can't
    disagree about what "at status X" means.
    """
    spec = sampling_spec(ds)
    if not spec:
        raise SamplingError("המאגר הזה לא מוגדר לדגימה ממוקדת (אין sampling ב-scraper_config)")
    mode = (mode or "").strip().lower()
    if mode not in available_modes(ds):
        raise SamplingError(f"מצב דגימה לא נתמך במאגר הזה: {mode!r}")

    # Every mode but the full pass produces a version that holds only part of
    # the corpus. The worker echoes this back on push-version so the shrink
    # guard doesn't measure a one-file sample against a 90k-file register, and
    # so the version says what it is.
    params: dict = {"run_mode": mode, "run_partial": mode != "all"}

    # Take the item list from ANOTHER dataset — the two corpora of one source
    # share their items. This is checked before the per-mode branches because
    # it REPLACES where the list comes from, whatever the mode's usual source
    # would have been; the run itself is still a targeted re-read.
    if targets_from:
        source_id, total, title = await _targets_from_dataset(
            db, targets_from, status, spec.get("status_column"))
        params["run_mode"] = "status"
        params["run_partial"] = True
        params["run_targets_dataset"] = source_id
        params["run_target_count"] = total
        if status:
            params["run_status"] = status
        label = f"לפי רשימת הישויות של «{title}»"
        if status:
            label += f", סטטוס {status}"
        return params, f"{label} ({total:,} ישויות)"

    if mode == "all":
        return params, MODE_LABELS_HE["all"]

    if mode == "item":
        value = (item or "").strip()
        if not value:
            raise SamplingError("לא נמסר מזהה ישות (למשל מספר תיק)")
        params["run_item"] = value
        return params, f"{MODE_LABELS_HE['item']}: {value}"

    table = await resolve_table(ds, db)
    if not table:
        raise SamplingError("למאגר אין עדיין טבלת ארכיון — הרץ דגימה מלאה אחת קודם")

    if mode == "new":
        # The frontier is the whole instruction: "start past the highest key you
        # already hold". Tiny, so it rides in the task rather than being fetched.
        # Narrowed to the live series — see recent_series.
        if spec.get("key_separator"):
            full = await append_store.key_frontier(
                table, key_col=spec["item_key"], separator=spec["key_separator"])
            walked = recent_series(full, spec.get("new_series_window"))
            params["run_frontier"] = walked
            if walked:
                return params, (f"{MODE_LABELS_HE['new']} "
                                f"(סדרות {', '.join(sorted(walked, reverse=True))}, "
                                f"מעבר ל-{', '.join(sorted(walked.values(), reverse=True))})")
        return params, MODE_LABELS_HE["new"]

    # mode == "status"
    value = (status or "").strip()
    if not value:
        raise SamplingError("לא נבחר סטטוס")
    if not spec.get("status_column"):
        raise SamplingError("המאגר לא מגדיר עמודת סטטוס")
    _keys, total = await append_store.latest_item_keys(
        table, key_col=spec["item_key"], order_col=spec.get("sample_column"),
        value_col=spec["status_column"], value=value, limit=1,
    )
    if total == 0:
        raise SamplingError(f"אין ישויות בסטטוס {value!r}")
    if total > MAX_TARGETS:
        raise SamplingError(
            f"{total:,} ישויות בסטטוס {value!r} — מעל התקרה של {MAX_TARGETS:,} "
            "לריצה ממוקדת. הרץ דגימה מלאה במקום."
        )
    params["run_status"] = value
    params["run_target_count"] = total
    return params, f"{MODE_LABELS_HE['status']}: {value} ({total:,} ישויות)"


async def queue_run(
    ds, db: AsyncSession, *, mode: str = "all", status: str | None = None,
    item: str | None = None, targets_from: str | None = None,
    priority: int | None = None, actor: str = "admin", reaim: bool = True,
    note: str = "דגימה ממוקדת",
) -> tuple:
    """Put one targeted run on the queue. ``(task, summary, params)``.

    The single path to a sampling task, shared by the admin's button and by the
    scheduled cadence, so the two cannot drift into meaning different things.
    The run's parameters belong on the TASK rather than going through poll_job:
    they ARE the run (migration 047), and the worker reads them merged over the
    dataset's config when it claims it.

    ``reaim`` is the one place the two callers legitimately differ. At most one
    active task exists per dataset (uq_scrape_tasks_active_per_dataset), so a
    PENDING task has to be either re-aimed or yielded to:

    * an admin who clicked supersedes whatever a routine poll queued — that is
      what clicking meant (``reaim=True``);
    * a scheduled run must NOT (``reaim=False``). The pending task it would
      overwrite could be the monthly full pass, and silently converting a
      48-hour sweep into a 40-minute walk — every week, forever — would quietly
      cancel the very run that catches what the walk cannot see.

    A RUNNING task is never re-aimed by anyone: a scrape in flight would publish
    its version under the new label while holding the old one's rows.
    """
    from app.models.scrape_task import PRIORITY_MANUAL, ScrapeTask
    from app.services.activity_log import log_event

    if not getattr(ds, "is_active", True):
        raise SamplingError(
            "המאגר מושהה (is_active=false) — הפעל אותו מחדש לפני דגימה")
    params, summary = await build_params(
        ds, db, mode=mode, status=status, item=item, targets_from=targets_from)

    existing = (await db.execute(
        select(ScrapeTask).where(
            ScrapeTask.tracked_dataset_id == ds.id,
            ScrapeTask.status.in_(["pending", "running"]),
        )
    )).scalar_one_or_none()
    if existing is not None and (existing.status == "running" or not reaim):
        raise SamplingBusy(
            "כבר רצה עכשיו משימה על המאגר הזה — המתן לסיומה"
            if existing.status == "running"
            else "כבר ממתינה בתור משימה על המאגר הזה — הדגימה המתוזמנת תחכה לתור הבא")

    prio = PRIORITY_MANUAL if priority is None else priority
    label = f"{note}: {summary}"[:500]
    if existing is not None:
        existing.params = params
        existing.priority = max(existing.priority or 0, prio)
        existing.message = label
        task = existing
    else:
        task = ScrapeTask(
            tracked_dataset_id=ds.id, status="pending", priority=prio,
            phase="queued", params=params, message=label,
        )
        db.add(task)
    await db.commit()
    await log_event(
        event="queued", dataset=ds, status="info", actor=actor,
        message=f"נוספה לתור {note} — {summary}", detail=str(params),
    )
    return task, summary, params
