"""What data.gov.il refuses to serve a server, remembered between polls.

data.gov.il puts some resource FILES behind a bot wall: a headless download
gets an HTML challenge page under the file's name instead of the file, which
``ckan_client`` raises as "Got HTML" and ``version_detector`` turns into a
blocked id. Tabular resources are unaffected — their rows still come through
the datastore API — so a blocked file is a hole in a dataset that otherwise
looks healthy.

Why this is stored rather than recomputed
-----------------------------------------
Being blocked is a property of the RESOURCE, not of one poll. The wall does not
come down because the package's ``metadata_modified`` stood still. But the
warning used to be built only after ``detect_resource_changes``, which two
shortcuts earlier in ``poll_dataset`` return before ever reaching:

  * nothing-changed (the metadata revision we already hold), and
  * a version already exists for this revision.

So a dataset whose metadata has not moved since the detection shipped kept
taking a shortcut past the only code that could notice its files were missing,
and lost them in total silence. Measured 2026-08-07 across the 13 CKAN datasets
holding blocked files: only 6 carried any warning at all. רמזורים was quietly
missing 11 resources, תושבים בישראל לפי ישובים another 11, with nothing in the
UI to say so.

Remembering the finding on the dataset fixes both halves: the shortcuts can
render the warning without repeating the work, and the entries are already the
structured work-list — resource id, name, format, URL — that a worker capable
of getting past the wall needs, instead of it having to parse a Hebrew
sentence out of an error field.

``assessed()`` is the third piece. A dataset that has never once run detection
has nothing stored, and would keep taking the shortcut forever — so the caller
treats "never assessed" as work to do, exactly as it already treats a NEON
archive that is behind its plan. That costs one full poll per dataset, once.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

CONFIG_KEY = "blocked_resources"


def describe(resources: list[dict], blocked_ids) -> list[dict]:
    """Structured entries for the resources this poll found blocked.

    Ordered by the dataset's own resource order rather than by the id set, so
    the stored list is stable between polls and does not churn the config
    (and the warning does not reshuffle) just because a set iterated
    differently.
    """
    ids = set(blocked_ids or ())
    return [
        {
            "id": r["id"],
            "name": r.get("name") or r["id"][:8],
            "format": (r.get("format") or "?").upper(),
            "url": r.get("url") or "",
        }
        for r in resources
        if r.get("id") in ids
    ]


def pending(entries: list[dict] | None) -> list[dict]:
    """The blocked resources that still have no archive.

    A file the worker fetched stays BLOCKED — data.gov.il goes on refusing this
    server, and the next poll will detect it again, correctly. What changes is
    that it is no longer MISSING. Reporting "waiting to be fetched" over a
    dataset that is fully archived would be false in the direction that matters,
    so the notice is driven by this rather than by the raw list.
    """
    return [e for e in (entries or []) if not e.get("fetched_at")]


def note_for(entries: list[dict] | None) -> str | None:
    """The standing user-facing indication, or None when nothing is missing."""
    missing = pending(entries)
    if not missing:
        return None
    listed = ", ".join(f"{e.get('name')} ({e.get('format')})" for e in missing)
    return (
        "ℹ ממתין לגירוד בדפדפן — "
        f"{len(missing)} קבצים חסומים להורדה שרתית ב-data.gov.il: {listed}"
    )


def _utc(value):
    import datetime as _dt

    if not value:
        return None
    try:
        t = _dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    # data.gov.il's metadata_modified is naive UTC.
    return t if t.tzinfo else t.replace(tzinfo=_dt.timezone.utc)


def _fetched_after(fetched_at, modified) -> bool:
    """Was the file fetched after the package last changed?

    The stamp's ``fetched_modified`` is whatever the worker pushed as the
    version's ``metadata_modified`` — the push time, never the package's
    revision — so an equality test alone never held, and every later poll asked
    for the same file again and stored it again (accid_taz, road_strat_2030:
    two identical versions minutes apart on 2026-10-01). A fetch that happened
    after the source's last change is still the source's current file.
    """
    a, m = _utc(fetched_at), _utc(modified)
    return a is not None and m is not None and a >= m


def carry_fetch_state(entries: list[dict], previous: list[dict] | None,
                      *, modified: str | None) -> list[dict]:
    """Re-attach what a previous poll knew about which files have been fetched.

    Detection rebuilds the entries from the source every time and has no memory,
    so without this each poll would forget that a file was already archived and
    the notice would come straight back on a dataset that is complete.

    The stamp only survives while the SOURCE has not moved: it records the
    package's ``metadata_modified`` at fetch time, and a package that has been
    revised may well be offering a different file under the same resource id.
    Then the entry is pending again and the worker is asked for it again —
    which is the correct answer, and the reason this keys on the revision rather
    than on a bare "done" flag.
    """
    was = {e.get("id"): e for e in (previous or []) if e.get("fetched_at")}
    out = []
    for entry in entries:
        old = was.get(entry.get("id"))
        if old and (old.get("fetched_modified") == modified
                    or _fetched_after(old.get("fetched_at"), modified)):
            entry = {**entry,
                     "fetched_at": old["fetched_at"],
                     "fetched_modified": old.get("fetched_modified"),
                     "fetched_version": old.get("fetched_version")}
        out.append(entry)
    return out


def delivered(record: dict) -> bool:
    """Did a worker's per-resource record actually deliver the file?

    A raw file counts only with a size: the worker omits ``bytes`` for a
    0-byte file, and data.gov.il answers a burst with exactly that — 200 and no
    body. Stamped, such a file was never asked for again (161 of 202 חצב files
    on 2026-09-27).
    """
    status = record.get("status")
    return status == "features" or (status == "raw_only" and bool(record.get("bytes")))


def empty_deliveries(entries: list[dict] | None, versions: dict) -> set[str]:
    """Stamped entries whose delivering version shows they arrived empty.

    ``versions`` maps a version number to that version's ``change_summary``.
    Stamps made before :func:`delivered` existed are judged by the same rule.
    """
    out = set()
    for e in entries or []:
        if not e.get("fetched_at"):
            continue
        summary = versions.get(e.get("fetched_version")) or {}
        records = ((summary.get("scrape_metadata") or {}).get("blocked_files") or {}).get("resources") or []
        rec = next((r for r in records
                    if (r.get("resource_id") or r.get("id")) == e.get("id")), None)
        if rec is not None and not delivered(rec):
            out.add(e["id"])
    return out


def file_resources(resources: list[dict] | None) -> list[dict]:
    """The resources that are FILES — a URL and no datastore table behind it.

    These are what a datastore-only archive never touches. A dataset whose rows
    stream to the SQL archive (``archive_neon``) used to be recorded as "checked,
    nothing blocked" on the strength of its tables alone, so its SHP/KMZ/PDF
    were never fetched by anyone (rail_stat, 2026-10-01: two tables archived,
    the map itself not at all).
    """
    return [r for r in (resources or []) if r.get("url") and not r.get("datastore_active")]


async def unstamp_empty_deliveries(db, rows) -> int:
    """Clear the fetched stamp on every file that arrived empty, judged from
    the delivering version's own per-resource record. Returns how many.

    Runs for any dataset, not only the ones the catalog watch reads: the
    2026-09-27 burst stamped 0-byte files on ministry_of_transport layers too
    (lrt_stat, metronit, tma_42…), and nothing outside the watch ever looked.
    """
    from sqlalchemy import select, tuple_

    from app.models.version_index import VersionIndex

    wanted = {(r.id, e.get("fetched_version")) for r in rows
              for e in stored(r) if e.get("fetched_at")}
    wanted = {(ds_id, n) for ds_id, n in wanted if isinstance(n, int)}
    if not wanted:
        return 0
    summaries: dict = {}
    for v in (await db.execute(
        select(VersionIndex.tracked_dataset_id, VersionIndex.version_number,
               VersionIndex.change_summary)
        .where(tuple_(VersionIndex.tracked_dataset_id, VersionIndex.version_number)
               .in_(list(wanted)))
    )).all():
        summaries.setdefault(v.tracked_dataset_id, {})[v.version_number] = v.change_summary or {}
    total = 0
    for r in rows:
        empty = empty_deliveries(stored(r), summaries.get(r.id, {}))
        if empty and unstamp(r, empty):
            logger.info("%s — %d empty file(s) made pending again",
                        getattr(r, "ckan_name", r.id), len(empty))
            total += len(empty)
    return total


def unstamp(ds, resource_ids) -> bool:
    """Make these resources pending again. Returns True if anything changed."""
    ids = set(resource_ids or ())
    entries = stored(ds)
    out, changed = [], False
    for entry in entries:
        if entry.get("id") in ids and entry.get("fetched_at"):
            entry = {k: v for k, v in entry.items()
                     if k not in ("fetched_at", "fetched_modified", "fetched_version")}
            changed = True
        out.append(entry)
    if changed:
        ds.scraper_config = {**(ds.scraper_config or {}), CONFIG_KEY: out}
    return changed


def mark_fetched(ds, resource_ids, *, modified: str | None,
                 version: int | None = None, now: str | None = None) -> bool:
    """Record that a worker delivered these resources. Returns True if changed.

    Called when a ``ckan_blocked_files`` run pushes its version. Only the
    resources it actually delivered are stamped — a run that failed on one file
    leaves that one pending, so a partial rescue reads as partial.
    """
    import datetime as _dt

    ids = set(resource_ids or ())
    if not ids:
        return False
    entries = stored(ds)
    if not entries:
        return False
    stamp = now or _dt.datetime.now(_dt.timezone.utc).isoformat()
    changed = False
    out = []
    for entry in entries:
        if entry.get("id") in ids and entry.get("fetched_modified") != modified:
            entry = {**entry, "fetched_at": stamp, "fetched_modified": modified,
                     "fetched_version": version}
            changed = True
        out.append(entry)
    if changed:
        ds.scraper_config = {**(ds.scraper_config or {}), CONFIG_KEY: out}
    return changed


def stored(ds) -> list[dict]:
    """What the last assessment found. [] both when nothing is blocked and
    when nothing has been assessed — use `assessed()` to tell those apart."""
    entries = (ds.scraper_config or {}).get(CONFIG_KEY)
    return list(entries) if isinstance(entries, list) else []


def assessed(ds) -> bool:
    """Has detection ever run against this dataset?

    An empty list means "checked, nothing blocked" and is a real answer; a
    missing key means we have never looked.
    """
    return isinstance((ds.scraper_config or {}).get(CONFIG_KEY), list)


def remember(ds, entries: list[dict]) -> bool:
    """Persist this poll's finding. Returns True when it actually changed.

    The column is plain JSONB, not a MutableDict, so an in-place edit would not
    be flagged dirty and would never reach the database — the config is
    REPLACED, matching how every other writer here does it.

    A no-op write is skipped so an unchanged dataset does not dirty its row on
    every poll.
    """
    current = (ds.scraper_config or {}).get(CONFIG_KEY)
    if isinstance(current, list) and current == entries:
        return False
    ds.scraper_config = {**(ds.scraper_config or {}), CONFIG_KEY: entries}
    return True
