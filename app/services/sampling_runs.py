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


def sampling_spec(ds) -> dict | None:
    """The dataset's ``sampling`` block, or None if it declares none."""
    spec = (ds.scraper_config or {}).get("sampling")
    if not isinstance(spec, dict) or not spec.get("item_key"):
        return None
    return spec


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
    "לפי סטטוס" shows how much work each choice is), and how far each key series
    has got."""
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


class SamplingError(ValueError):
    """A run that can't be built — unknown mode, missing argument, too big."""


async def build_params(
    ds, db: AsyncSession, *, mode: str, status: str | None = None,
    item: str | None = None,
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
        if spec.get("key_separator"):
            params["run_frontier"] = await append_store.key_frontier(
                table, key_col=spec["item_key"], separator=spec["key_separator"])
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
